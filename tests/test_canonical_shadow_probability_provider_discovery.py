from __future__ import annotations

from mlb_app.simulation.shadow import (
    CANONICAL_SHADOW_PROBABILITY_PROVIDER_DISCOVERY_VERSION,
    discover_canonical_shadow_probability_provider,
)


def probabilities():
    return {
        "k": 0.225,
        "bb": 0.085,
        "hbp": 0.011,
        "single": 0.145,
        "double": 0.045,
        "triple": 0.004,
        "hr": 0.030,
        "reached_on_error": 0.007,
        "out": 0.448,
    }


def model(version="pa_outcome_v1"):
    return {
        "model_version": version,
        "probabilities": probabilities(),
    }


def complete_workspace():
    return {
        "awayPAOutcomeModel": model(),
        "homePAOutcomeModel": model(),
        "awayVsHomeBullpenPAOutcomeModel": model(),
        "homeVsAwayBullpenPAOutcomeModel": model(),
    }


def test_complete_versioned_models_discover_provider():
    result = (
        discover_canonical_shadow_probability_provider(
            workspace=complete_workspace(),
        )
    )

    assert result.status == "ready"
    assert result.ready is True
    assert result.provider is not None
    assert result.provider.provider_name == (
        "model_projections_pa_outcome"
    )
    assert result.provider.provider_version == (
        "pa_outcome_v1"
    )
    assert result.provider.artifact_id is None


def test_readiness_field_contains_serializable_identity():
    result = (
        discover_canonical_shadow_probability_provider(
            workspace=complete_workspace(),
        )
    )

    fields = result.readiness_workspace_fields()
    provider = fields[
        "canonicalProbabilityProvider"
    ]

    assert provider["provider_name"] == (
        "model_projections_pa_outcome"
    )
    assert provider["provider_version"] == (
        "pa_outcome_v1"
    )
    assert provider["identity"] == (
        "model_projections_pa_outcome:pa_outcome_v1"
    )


def test_diagnostics_do_not_claim_artifact_discovery():
    diagnostics = (
        discover_canonical_shadow_probability_provider(
            workspace=complete_workspace(),
        ).to_diagnostics()
    )

    assert diagnostics["schema_version"] == (
        CANONICAL_SHADOW_PROBABILITY_PROVIDER_DISCOVERY_VERSION
    )
    assert diagnostics["ready"] is True
    assert diagnostics["artifact_discovered"] is False
    assert diagnostics[
        "probability_records_exposed"
    ] is False
    assert diagnostics["activation_permitted"] is False
    assert diagnostics["authoritative_source"] == (
        "legacy"
    )


def test_missing_model_blocks_provider_identity():
    workspace = complete_workspace()
    workspace.pop("homePAOutcomeModel")

    result = (
        discover_canonical_shadow_probability_provider(
            workspace=workspace,
        )
    )

    assert result.ready is False
    assert result.status == "partial"
    assert result.missing_models == (
        "homePAOutcomeModel",
    )
    assert result.readiness_workspace_fields() == {}


def test_mismatched_versions_block_provider_identity():
    workspace = complete_workspace()
    workspace[
        "homeVsAwayBullpenPAOutcomeModel"
    ] = model("pa_outcome_v2")

    result = (
        discover_canonical_shadow_probability_provider(
            workspace=workspace,
        )
    )

    assert result.ready is False
    assert result.status == "partial"
    assert result.model_versions == (
        "pa_outcome_v1",
        "pa_outcome_v2",
    )


def test_missing_outcome_blocks_provider_identity():
    workspace = complete_workspace()
    workspace[
        "awayPAOutcomeModel"
    ]["probabilities"].pop("hbp")

    result = (
        discover_canonical_shadow_probability_provider(
            workspace=workspace,
        )
    )

    assert result.ready is False
    assert result.invalid_models == (
        "awayPAOutcomeModel",
    )


def test_invalid_probability_total_blocks_provider_identity():
    workspace = complete_workspace()
    workspace[
        "homePAOutcomeModel"
    ]["probabilities"]["out"] = 0.9

    result = (
        discover_canonical_shadow_probability_provider(
            workspace=workspace,
        )
    )

    assert result.ready is False
    assert result.invalid_models == (
        "homePAOutcomeModel",
    )


def test_source_workspace_is_not_mutated():
    workspace = complete_workspace()
    snapshot = repr(workspace)

    discover_canonical_shadow_probability_provider(
        workspace=workspace,
    )

    assert repr(workspace) == snapshot
