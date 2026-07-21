from __future__ import annotations

from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalPlateAppearanceOutcome,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
)
from mlb_app.simulation.shadow import (
    CANONICAL_SHADOW_FALLBACK_CATALOG_DISCOVERY_VERSION,
    discover_canonical_shadow_fallback_catalog,
)


PROVIDER = CanonicalProbabilityProviderIdentity(
    provider_name="model_projections_pa_outcome",
    provider_version="pa_outcome_v1",
)


def probabilities(out=0.448):
    return {
        "k": 0.225,
        "bb": 0.085,
        "hbp": 0.011,
        "single": 0.145,
        "double": 0.045,
        "triple": 0.004,
        "hr": 0.030,
        "reached_on_error": 0.007,
        "out": out,
    }


def complete_workspace():
    return {
        "awayPAOutcomeModel": {
            "probabilities": probabilities(),
        },
        "homePAOutcomeModel": {
            "probabilities": probabilities(),
        },
        "awayVsHomeBullpenPAOutcomeModel": {
            "probabilities": probabilities(),
        },
        "homeVsAwayBullpenPAOutcomeModel": {
            "probabilities": probabilities(),
        },
    }


def test_complete_models_build_global_catalog():
    result = discover_canonical_shadow_fallback_catalog(
        workspace=complete_workspace(),
        provider=PROVIDER,
    )

    assert result.status == "ready"
    assert result.ready is True
    assert result.catalog is not None
    assert result.catalog.provider == PROVIDER
    assert len(result.catalog.records) == 1

    record = result.catalog.records[0]

    assert record.tier is (
        CanonicalProbabilityFallbackTier.GLOBAL
    )
    assert record.identity is None


def test_catalog_uses_canonical_outcome_order():
    result = discover_canonical_shadow_fallback_catalog(
        workspace=complete_workspace(),
        provider=PROVIDER,
    )

    record = result.catalog.records[0]

    assert tuple(
        point.outcome
        for point in record.probabilities
    ) == CANONICAL_PA_OUTCOME_ORDER


def test_reached_on_error_is_folded_into_out():
    result = discover_canonical_shadow_fallback_catalog(
        workspace=complete_workspace(),
        provider=PROVIDER,
    )

    record = result.catalog.records[0]
    values = {
        point.outcome: point.probability
        for point in record.probabilities
    }

    assert values[
        CanonicalPlateAppearanceOutcome.OUT
    ] == 0.455


def test_readiness_fields_are_serializable_summary():
    result = discover_canonical_shadow_fallback_catalog(
        workspace=complete_workspace(),
        provider=PROVIDER,
    )

    fields = result.readiness_workspace_fields()
    summary = fields[
        "canonicalProbabilityFallbackCatalog"
    ]

    assert summary["provider_identity"] == (
        PROVIDER.identity
    )
    assert summary["record_count"] == 1
    assert summary["tiers"] == ["global"]
    assert len(summary["digest"]) == 64


def test_diagnostics_do_not_expose_probability_rows():
    diagnostics = (
        discover_canonical_shadow_fallback_catalog(
            workspace=complete_workspace(),
            provider=PROVIDER,
        ).to_diagnostics()
    )

    assert diagnostics["schema_version"] == (
        CANONICAL_SHADOW_FALLBACK_CATALOG_DISCOVERY_VERSION
    )
    assert diagnostics[
        "probability_records_exposed"
    ] is False
    assert diagnostics[
        "exact_artifact_discovered"
    ] is False
    assert diagnostics["global_record_only"] is True
    assert diagnostics[
        "reached_on_error_mapping"
    ] == "folded_into_canonical_out"


def test_missing_provider_blocks_catalog():
    result = discover_canonical_shadow_fallback_catalog(
        workspace=complete_workspace(),
        provider=None,
    )

    assert result.status == "blocked"
    assert result.ready is False
    assert result.catalog is None


def test_missing_model_keeps_catalog_blocked():
    workspace = complete_workspace()
    workspace.pop("homePAOutcomeModel")

    result = discover_canonical_shadow_fallback_catalog(
        workspace=workspace,
        provider=PROVIDER,
    )

    assert result.status == "partial"
    assert result.ready is False
    assert result.missing_models == (
        "homePAOutcomeModel",
    )


def test_invalid_distribution_keeps_catalog_blocked():
    workspace = complete_workspace()
    workspace[
        "awayPAOutcomeModel"
    ]["probabilities"]["out"] = 0.9

    result = discover_canonical_shadow_fallback_catalog(
        workspace=workspace,
        provider=PROVIDER,
    )

    assert result.ready is False
    assert result.invalid_models == (
        "awayPAOutcomeModel",
    )


def test_source_workspace_is_not_mutated():
    workspace = complete_workspace()
    snapshot = repr(workspace)

    discover_canonical_shadow_fallback_catalog(
        workspace=workspace,
        provider=PROVIDER,
    )

    assert repr(workspace) == snapshot
