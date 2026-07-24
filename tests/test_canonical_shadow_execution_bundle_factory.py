from __future__ import annotations

import pytest

import mlb_app.simulation.game_simulation_builder as builder
import mlb_app.simulation.shadow.execution_factory as execution_module
from mlb_app.simulation.events import GameState
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
    CanonicalPlateAppearanceQuery,
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
    CANONICAL_SHADOW_EXECUTION_BUNDLE_FACTORY_VERSION,
    CanonicalShadowExecutionBundle,
    CanonicalShadowExecutionBundleFactory,
    build_canonical_shadow_execution_bundle_factory,
)


def provider_identity(
    artifact_id="execution-factory-artifact",
):
    return CanonicalProbabilityProviderIdentity(
        provider_name="execution-factory-test",
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


def exact_artifact(
    records=(),
    provider=None,
):
    return CanonicalProbabilityArtifact(
        provider=provider or provider_identity(),
        records=tuple(records),
    )


def fallback_catalog(provider=None):
    return CanonicalProbabilityFallbackCatalog(
        provider=provider or provider_identity(),
        records=(
            CanonicalProbabilityFallbackRecord(
                tier=(
                    CanonicalProbabilityFallbackTier.GLOBAL
                ),
                identity=None,
                probabilities=probabilities(),
            ),
        ),
    )


def fallback_policy():
    return CanonicalProbabilityFallbackPolicy(
        tiers=(
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
            CanonicalProbabilityFallbackTier.GLOBAL,
        )
    )


def factory_input(
    *,
    game_pk=123,
):
    return build_canonical_trial_factory_input(
        game_pk=game_pk,
        config={
            "simulation_count": 2,
            "seed": 98765,
            "canonical_model_version": (
                "execution-bundle-factory-test-v1"
            ),
        },
    )


def baserunning_catalog():
    runner_ids = tuple(
        (
            f"away_batter_{index}",
            f"home_batter_{index}",
        )
        for index in range(9)
    )

    return CanonicalBaserunningEvidenceCatalog(
        runners=tuple(
            CanonicalRunnerBaserunningProfile(
                runner_id=runner_id,
                speed_score=0.85,
                attempt_rate=0.30,
                success_rate=0.80,
                lead_quality=0.75,
                fatigue_index=0.10,
            )
            for pair in runner_ids
            for runner_id in pair
        ),
        pitchers=(
            CanonicalPitcherBaserunningProfile(
                pitcher_id="away_starter",
                hold_score=0.40,
                delivery_time_score=0.45,
                pickoff_attempt_rate=0.08,
                pickoff_success_rate=0.02,
            ),
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


def execution_factory(
    *,
    records=(),
    baserunning=None,
):
    return (
        build_canonical_shadow_execution_bundle_factory(
            matchup_input=matchup(),
            exact_artifact=exact_artifact(records),
            fallback_catalog=fallback_catalog(),
            baserunning_evidence_catalog=baserunning,
            fallback_policy=fallback_policy(),
            game_config=CanonicalGameConfig(
                regulation_innings=1,
                max_extra_innings=0,
            ),
        )
    )


def legacy_result():
    return {
        "model_version": "legacy-test-v1",
        "simulations": 2,
        "away_expected_runs": 0.0,
        "home_expected_runs": 0.0,
        "total_expected_runs": 0.0,
        "home_win_probability": 0.0,
        "away_run_distribution": {
            "0": 1.0,
        },
        "home_run_distribution": {
            "0": 1.0,
        },
        "total_run_distribution": {
            "0": 1.0,
        },
        "metadata": {
            "simulation_count": 2,
        },
    }


def test_factory_version_is_explicit():
    value = execution_factory()

    assert value.factory_version == (
        CANONICAL_SHADOW_EXECUTION_BUNDLE_FACTORY_VERSION
    )


def test_factory_rejects_provider_mismatch():
    different = provider_identity(
        "different-artifact"
    )

    with pytest.raises(
        ValueError,
        match="exact artifact provider",
    ):
        CanonicalShadowExecutionBundleFactory(
            matchup_input=matchup(),
            exact_artifact=exact_artifact(
                provider=different
            ),
            fallback_catalog=fallback_catalog(),
        )


def test_factory_rejects_game_identity_mismatch():
    value = execution_factory()

    with pytest.raises(
        ValueError,
        match="game_pk",
    ):
        value(
            factory_input=factory_input(
                game_pk=456
            )
        )


def test_factory_returns_atomic_bundle():
    bundle = execution_factory()(
        factory_input=factory_input()
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


def test_global_fallback_usage_is_collected():
    bundle = execution_factory()(
        factory_input=factory_input()
    )
    diagnostics = (
        bundle.probability_resolution_diagnostics
    )

    assert diagnostics.exact_resolutions == 0
    assert diagnostics.fallback_resolutions == 12
    assert diagnostics.count_for(
        CanonicalProbabilityFallbackTier.GLOBAL
    ) == 12


def test_exact_rows_take_priority_when_available():
    records = tuple(
        CanonicalProbabilityArtifactRecord(
            batter_id=batter_id,
            pitcher_id=pitcher_id,
            probabilities=probabilities(),
        )
        for batter_id, pitcher_id in (
            *(
                (
                    f"away_batter_{index}",
                    "home_starter",
                )
                for index in range(3)
            ),
            *(
                (
                    f"home_batter_{index}",
                    "away_starter",
                )
                for index in range(3)
            ),
        )
    )

    bundle = execution_factory(
        records=records
    )(
        factory_input=factory_input()
    )
    diagnostics = (
        bundle.probability_resolution_diagnostics
    )

    assert diagnostics.exact_resolutions == 12
    assert diagnostics.fallback_resolutions == 0


def test_each_factory_call_uses_fresh_diagnostics():
    value = execution_factory()
    input_value = factory_input()

    first = value(
        factory_input=input_value
    )
    second = value(
        factory_input=input_value
    )

    assert first == second
    assert (
        first.probability_resolution_diagnostics
        .total_resolutions
        == 12
    )
    assert (
        second.probability_resolution_diagnostics
        .total_resolutions
        == 12
    )


def test_builder_accepts_first_class_factory():
    value = execution_factory()

    output = (
        builder._attach_canonical_shadow_diagnostics(
            legacy_result(),
            config={
                "canonical_shadow_enabled": True,
                "simulation_count": 2,
                "seed": 98765,
                "canonical_model_version": (
                    "execution-bundle-factory-test-v1"
                ),
            },
            game_pk=123,
            trial_batch_factory=value,
        )
    )

    shadow = output[
        "diagnostics"
    ]["canonical_shadow"]

    assert shadow["authoritative_source"] == "legacy"
    assert shadow["canonical_available"] is True
    assert (
        shadow["probability_resolution"]
        ["summary"]["total_resolutions"]
        == 12
    )
    assert output["away_expected_runs"] == 0.0


def test_injected_catalog_attaches_coupled_resolver_per_trial(
    monkeypatch,
):
    source = baserunning_catalog()
    build_calls = []
    coupled_calls = []

    def build_factory(*, catalog):
        build_calls.append(catalog)

        def coupled_factory(
            context,
            plate_appearance_resolver,
        ):
            coupled_calls.append(
                (
                    context.trial_index,
                    plate_appearance_resolver,
                )
            )

            return lambda state, batter_id, sequence: None

        return coupled_factory

    monkeypatch.setattr(
        execution_module,
        "build_canonical_catalog_baserunning_resolver_factory",
        build_factory,
    )

    bundle = execution_factory(
        baserunning=source,
    )(
        factory_input=factory_input()
    )

    assert len(bundle.trial_batch.games) == 2
    assert build_calls == [source]
    assert [
        trial_index
        for trial_index, _ in coupled_calls
    ] == [0, 1]
    assert all(
        callable(plate_appearance_resolver)
        for _, plate_appearance_resolver
        in coupled_calls
    )
    assert (
        bundle.canonical_shadow_execution_inputs
        .baserunning_evidence_catalog
        is source
    )


def test_missing_catalog_does_not_build_baserunning_resolver(
    monkeypatch,
):
    def unexpected_build(*, catalog):
        raise AssertionError(
            "missing catalog must not activate baserunning"
        )

    monkeypatch.setattr(
        execution_module,
        "build_canonical_catalog_baserunning_resolver_factory",
        unexpected_build,
    )

    bundle = execution_factory()(
        factory_input=factory_input()
    )

    assert len(bundle.trial_batch.games) == 2
    assert (
        bundle.canonical_shadow_execution_inputs
        .baserunning_evidence_catalog
        is None
    )
