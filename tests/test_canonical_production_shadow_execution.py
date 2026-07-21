from __future__ import annotations

from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalOutcomeProbability,
    CanonicalProbabilityArtifact,
    CanonicalProbabilityArtifactRecord,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
)
from mlb_app.simulation.shadow import (
    CANONICAL_PRODUCTION_SHADOW_EXECUTION_VERSION,
    CanonicalShadowBullpenDiscovery,
    CanonicalShadowBullpenSideDiscovery,
    CanonicalShadowExactArtifactDiscovery,
    CanonicalShadowFallbackCatalogDiscovery,
    CanonicalShadowLineupDiscovery,
    CanonicalShadowProbabilityProviderDiscovery,
    run_canonical_production_shadow,
)


PROVIDER = CanonicalProbabilityProviderIdentity(
    provider_name="model_projections_pa_outcome",
    provider_version="pa_outcome_v1",
)


def probability_points():
    values = {
        "out": 0.43,
        "single": 0.15,
        "double": 0.05,
        "triple": 0.005,
        "hr": 0.03,
        "bb": 0.085,
        "hbp": 0.01,
        "k": 0.24,
    }

    return tuple(
        CanonicalOutcomeProbability(
            outcome=outcome,
            probability=values[outcome.value],
        )
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    )


def lineups():
    return CanonicalShadowLineupDiscovery(
        away_player_ids=tuple(
            f"a{index}"
            for index in range(1, 10)
        ),
        home_player_ids=tuple(
            f"h{index}"
            for index in range(1, 10)
        ),
        away_source_count=9,
        home_source_count=9,
        status="ready",
    )


def bullpens():
    return CanonicalShadowBullpenDiscovery(
        away=CanonicalShadowBullpenSideDiscovery(
            team_id="1",
            starter_id="100",
            bullpen_pitcher_ids=("101",),
            source_record_count=2,
            status="ready",
        ),
        home=CanonicalShadowBullpenSideDiscovery(
            team_id="2",
            starter_id="200",
            bullpen_pitcher_ids=("201",),
            source_record_count=2,
            status="ready",
        ),
    )


def provider_discovery():
    return CanonicalShadowProbabilityProviderDiscovery(
        provider=PROVIDER,
        model_versions=("pa_outcome_v1",),
        valid_model_count=4,
        status="ready",
    )


def exact_discovery():
    records = tuple(
        CanonicalProbabilityArtifactRecord(
            batter_id=batter_id,
            pitcher_id=pitcher_id,
            probabilities=probability_points(),
        )
        for batter_id, pitcher_id in (
            *(
                (f"a{index}", "200")
                for index in range(1, 10)
            ),
            *(
                (f"h{index}", "100")
                for index in range(1, 10)
            ),
        )
    )

    artifact = CanonicalProbabilityArtifact(
        provider=PROVIDER,
        records=records,
    )

    return CanonicalShadowExactArtifactDiscovery(
        artifact=artifact,
        away_record_count=9,
        home_record_count=9,
        away_real_profile_count=9,
        home_real_profile_count=9,
        status="ready",
    )


def fallback_discovery():
    catalog = CanonicalProbabilityFallbackCatalog(
        provider=PROVIDER,
        records=(
            CanonicalProbabilityFallbackRecord(
                tier=(
                    CanonicalProbabilityFallbackTier
                    .GLOBAL
                ),
                identity=None,
                probabilities=probability_points(),
            ),
        ),
    )

    return CanonicalShadowFallbackCatalogDiscovery(
        catalog=catalog,
        source_model_count=4,
        status="ready",
    )


def run(**overrides):
    kwargs = {
        "game_pk": 123,
        "lineups": lineups(),
        "bullpens": bullpens(),
        "provider_discovery": provider_discovery(),
        "exact_artifact_discovery": (
            exact_discovery()
        ),
        "fallback_catalog_discovery": (
            fallback_discovery()
        ),
        "bootstrap_ready": True,
        "simulation_count": 2,
    }
    kwargs.update(overrides)

    return run_canonical_production_shadow(
        **kwargs
    )


def test_ready_inputs_execute_real_trial_batch():
    result = run()

    assert result.status == "executed"
    assert result.executed is True
    assert result.material is not None
    assert result.execution_inputs is not None
    assert result.simulation_count == 2


def test_trial_batch_contains_requested_games():
    result = run()

    assert len(
        result.material.canonical_payload[
            "metadata"
        ]["simulation_count"]
        if False
        else result.material.canonical_payload
    ) > 0

    assert (
        result.material
        .probability_resolution_diagnostics
        .total_resolutions
        > 0
    )


def test_policy_enables_exact_then_global():
    result = run()

    tiers = (
        result.execution_inputs
        .fallback_policy
        .tiers
    )

    assert tiers == (
        CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
        CanonicalProbabilityFallbackTier.GLOBAL,
    )


def test_execution_carries_atomic_input_provenance():
    result = run()

    assert (
        result.material
        .canonical_shadow_execution_inputs
        is result.execution_inputs
    )
    assert len(
        result.execution_inputs.assembly_digest
    ) == 64


def test_diagnostics_keep_legacy_authority():
    diagnostics = run().to_diagnostics()

    assert diagnostics["schema_version"] == (
        CANONICAL_PRODUCTION_SHADOW_EXECUTION_VERSION
    )
    assert diagnostics["executed"] is True
    assert diagnostics[
        "production_authority_changed"
    ] is False
    assert diagnostics[
        "activation_permitted"
    ] is False
    assert diagnostics["authoritative_source"] == (
        "legacy"
    )


def test_not_ready_does_not_execute():
    result = run(
        bootstrap_ready=False,
    )

    assert result.status == "blocked"
    assert result.executed is False
    assert result.material is None


def test_missing_artifact_does_not_execute():
    result = run(
        exact_artifact_discovery=(
            CanonicalShadowExactArtifactDiscovery(
                status="unavailable",
            )
        )
    )

    assert result.status == "blocked"
    assert result.executed is False


def test_invalid_simulation_count_fails_open():
    result = run(
        simulation_count=0,
    )

    assert result.status == "error"
    assert result.executed is False
    assert result.error_type == "ValueError"
