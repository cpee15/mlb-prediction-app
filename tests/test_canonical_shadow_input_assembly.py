from __future__ import annotations

import pytest

from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalBaserunningEvidenceCatalog,
    CanonicalCatcherBaserunningProfile,
    CanonicalGameConfig,
    CanonicalLineup,
    CanonicalMatchupInput,
    CanonicalOutcomeProbability,
    CanonicalPitcherBaserunningProfile,
    CanonicalPitchingPlan,
    CanonicalPlateAppearanceOutcome,
    CanonicalProbabilityArtifact,
    CanonicalProbabilityArtifactRecord,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackPolicy,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
    CanonicalRunnerBaserunningProfile,
    build_canonical_trial_factory_input,
)
from mlb_app.simulation.shadow import (
    CANONICAL_SHADOW_INPUT_ASSEMBLY_VERSION,
    CanonicalShadowExecutionBundle,
    CanonicalShadowExecutionInputs,
    assemble_canonical_shadow_execution_inputs,
)


def provider_identity(
    artifact_id="input-assembly-artifact",
):
    return CanonicalProbabilityProviderIdentity(
        provider_name="input-assembly-test",
        provider_version="v1",
        artifact_id=artifact_id,
    )


def lineup(side):
    return CanonicalLineup(
        team_side=side,
        player_ids=tuple(
            f"{side}_batter_{index}"
            for index in range(9)
        ),
    )


def pitching_plan(side):
    return CanonicalPitchingPlan(
        team_side=side,
        starter_id=f"{side}_starter",
        bullpen_pitcher_ids=(
            f"{side}_reliever",
        ),
    )


def matchup(provider=None):
    return CanonicalMatchupInput(
        game_pk=123,
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        away_pitching_plan=(
            pitching_plan("away")
        ),
        home_pitching_plan=(
            pitching_plan("home")
        ),
        probability_provider=(
            provider or provider_identity()
        ),
    )


def probabilities(
    selected=CanonicalPlateAppearanceOutcome.STRIKEOUT,
):
    return tuple(
        CanonicalOutcomeProbability(
            outcome=outcome,
            probability=(
                1.0
                if outcome is selected
                else 0.0
            ),
        )
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    )


def exact_record(
    batter_id,
    pitcher_id,
):
    return CanonicalProbabilityArtifactRecord(
        batter_id=batter_id,
        pitcher_id=pitcher_id,
        probabilities=probabilities(),
    )


def exact_artifact(
    *,
    records=(),
    provider=None,
):
    return CanonicalProbabilityArtifact(
        provider=provider or provider_identity(),
        records=tuple(records),
    )


def fallback_record(
    tier,
    identity,
):
    return CanonicalProbabilityFallbackRecord(
        tier=tier,
        identity=identity,
        probabilities=probabilities(),
    )


def fallback_catalog(
    *,
    records=None,
    provider=None,
):
    return CanonicalProbabilityFallbackCatalog(
        provider=provider or provider_identity(),
        records=tuple(
            records
            if records is not None
            else (
                fallback_record(
                    CanonicalProbabilityFallbackTier.GLOBAL,
                    None,
                ),
            )
        ),
    )


def fallback_policy():
    return CanonicalProbabilityFallbackPolicy(
        tiers=(
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
            CanonicalProbabilityFallbackTier.GLOBAL,
        )
    )


def baserunning_catalog(
    *,
    fatigue_index=0.10,
):
    return CanonicalBaserunningEvidenceCatalog(
        runners=(
            CanonicalRunnerBaserunningProfile(
                runner_id="away_batter_0",
                speed_score=0.85,
                attempt_rate=0.30,
                success_rate=0.80,
                lead_quality=0.75,
                fatigue_index=fatigue_index,
            ),
        ),
        pitchers=(
            CanonicalPitcherBaserunningProfile(
                pitcher_id="home_starter",
                hold_score=0.40,
                delivery_time_score=0.45,
                pickoff_attempt_rate=0.08,
                pickoff_success_rate=0.02,
            ),
        ),
        away_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id="away_catcher",
            team_side="away",
            throwing_score=0.45,
            pop_time_score=0.40,
        ),
        home_catcher=CanonicalCatcherBaserunningProfile(
            catcher_id="home_catcher",
            team_side="home",
            throwing_score=0.45,
            pop_time_score=0.40,
        ),
    )


def assembled(
    *,
    exact=None,
    catalog=None,
    policy=None,
    config=None,
    baserunning=None,
):
    return assemble_canonical_shadow_execution_inputs(
        matchup_input=matchup(),
        exact_artifact=(
            exact
            if exact is not None
            else exact_artifact()
        ),
        fallback_catalog=(
            catalog
            if catalog is not None
            else fallback_catalog()
        ),
        fallback_policy=(
            policy
            if policy is not None
            else fallback_policy()
        ),
        baserunning_evidence_catalog=baserunning,
        game_config=(
            config
            if config is not None
            else CanonicalGameConfig(
                regulation_innings=1,
                max_extra_innings=0,
            )
        ),
    )


def test_assembly_version_and_provenance_are_explicit():
    value = assembled()

    assert value.assembly_version == (
        CANONICAL_SHADOW_INPUT_ASSEMBLY_VERSION
    )
    assert value.provider_identity == (
        provider_identity().identity
    )
    assert len(value.exact_artifact_digest) == 64
    assert len(value.fallback_catalog_digest) == 64
    assert len(value.assembly_digest) == 64


def test_assembly_rejects_exact_provider_mismatch():
    with pytest.raises(
        ValueError,
        match="exact artifact provider",
    ):
        CanonicalShadowExecutionInputs(
            matchup_input=matchup(),
            exact_artifact=exact_artifact(
                provider=provider_identity(
                    "different-exact"
                )
            ),
            fallback_catalog=fallback_catalog(),
        )


def test_assembly_rejects_fallback_provider_mismatch():
    with pytest.raises(
        ValueError,
        match="fallback catalog provider",
    ):
        CanonicalShadowExecutionInputs(
            matchup_input=matchup(),
            exact_artifact=exact_artifact(),
            fallback_catalog=fallback_catalog(
                provider=provider_identity(
                    "different-fallback"
                )
            ),
        )


def test_assembly_rejects_invalid_component_type():
    with pytest.raises(
        TypeError,
        match="matchup_input",
    ):
        CanonicalShadowExecutionInputs(
            matchup_input="invalid",
            exact_artifact=exact_artifact(),
            fallback_catalog=fallback_catalog(),
        )


def test_assembly_digest_is_record_order_independent():
    exact_records = (
        exact_record(
            "away_batter_0",
            "home_starter",
        ),
        exact_record(
            "away_batter_1",
            "home_starter",
        ),
    )
    fallback_records = (
        fallback_record(
            CanonicalProbabilityFallbackTier.BATTER,
            "away_batter_0",
        ),
        fallback_record(
            CanonicalProbabilityFallbackTier.GLOBAL,
            None,
        ),
    )

    first = assembled(
        exact=exact_artifact(
            records=exact_records
        ),
        catalog=fallback_catalog(
            records=fallback_records
        ),
    )
    second = assembled(
        exact=exact_artifact(
            records=tuple(
                reversed(exact_records)
            )
        ),
        catalog=fallback_catalog(
            records=tuple(
                reversed(fallback_records)
            )
        ),
    )

    assert first.assembly_digest == (
        second.assembly_digest
    )


def test_policy_change_changes_assembly_digest():
    exact_only = CanonicalProbabilityFallbackPolicy()
    exact_and_global = fallback_policy()

    first = assembled(
        policy=exact_only
    )
    second = assembled(
        policy=exact_and_global
    )

    assert first.assembly_digest != (
        second.assembly_digest
    )


def test_build_factory_preserves_assembled_inputs():
    value = assembled()
    factory = value.build_factory()

    assert factory.matchup_input is (
        value.matchup_input
    )
    assert factory.exact_artifact is (
        value.exact_artifact
    )
    assert factory.fallback_catalog is (
        value.fallback_catalog
    )
    assert factory.fallback_policy is (
        value.fallback_policy
    )
    assert factory.baserunning_evidence_catalog is (
        value.baserunning_evidence_catalog
    )
    assert factory.game_config is (
        value.game_config
    )


def test_assembled_factory_executes_atomic_bundle():
    value = assembled()
    factory = value.build_factory()

    bundle = factory(
        factory_input=(
            build_canonical_trial_factory_input(
                game_pk=123,
                config={
                    "simulation_count": 2,
                    "seed": 98765,
                    "canonical_model_version": (
                        "input-assembly-test-v1"
                    ),
                },
            )
        )
    )

    assert isinstance(
        bundle,
        CanonicalShadowExecutionBundle,
    )
    assert len(bundle.trial_batch.games) == 2
    assert (
        bundle.probability_resolution_diagnostics
        .total_resolutions
        == 12
    )


def test_optional_baserunning_catalog_provenance_is_explicit():
    source = baserunning_catalog()
    value = assembled(
        baserunning=source,
    )

    assert value.baserunning_evidence_catalog is source
    assert (
        value.baserunning_evidence_catalog_digest
        == source.digest
    )
    assert len(
        value.baserunning_evidence_catalog_digest
    ) == 64
    assert (
        value.build_factory()
        .baserunning_evidence_catalog
        is source
    )


def test_missing_baserunning_catalog_has_no_catalog_digest():
    value = assembled()

    assert value.baserunning_evidence_catalog is None
    assert value.baserunning_evidence_catalog_digest is None


def test_baserunning_catalog_change_changes_assembly_digest():
    first = assembled(
        baserunning=baserunning_catalog(
            fatigue_index=0.10,
        ),
    )
    second = assembled(
        baserunning=baserunning_catalog(
            fatigue_index=0.20,
        ),
    )

    assert (
        first.baserunning_evidence_catalog_digest
        != second.baserunning_evidence_catalog_digest
    )
    assert first.assembly_digest != second.assembly_digest


def test_assembly_rejects_invalid_baserunning_catalog_type():
    with pytest.raises(
        TypeError,
        match="baserunning_evidence_catalog",
    ):
        CanonicalShadowExecutionInputs(
            matchup_input=matchup(),
            exact_artifact=exact_artifact(),
            fallback_catalog=fallback_catalog(),
            baserunning_evidence_catalog="invalid",
        )
