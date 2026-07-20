from __future__ import annotations

import pytest

import mlb_app.simulation.game_simulation_builder as builder
from mlb_app.simulation.box_score import (
    ReducedBoxScore,
    TeamBoxScore,
)
from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalGameConfig,
    CanonicalGameOutcomeProjection,
    CanonicalLineup,
    CanonicalMatchupInput,
    CanonicalOutcomeProbability,
    CanonicalPitchingPlan,
    CanonicalPlateAppearanceOutcome,
    CanonicalProbabilityArtifact,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackPolicy,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
    CanonicalTrialBatch,
    CanonicalTrialDiagnostics,
    DistributionPoint,
    ProbabilityMetric,
    build_canonical_trial_factory_input,
)
from mlb_app.simulation.projections import (
    aggregate_projection_payload,
)
from mlb_app.simulation.shadow import (
    CanonicalShadowExecutionBundle,
    assemble_canonical_shadow_execution_inputs,
    canonical_shadow_execution_bundle_to_material,
)


def provider_identity():
    return CanonicalProbabilityProviderIdentity(
        provider_name="atomic-provenance-test",
        provider_version="v1",
        artifact_id="artifact-123",
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


def probabilities():
    return tuple(
        CanonicalOutcomeProbability(
            outcome=outcome,
            probability=(
                1.0
                if outcome
                is CanonicalPlateAppearanceOutcome.STRIKEOUT
                else 0.0
            ),
        )
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    )


def execution_inputs():
    provider = provider_identity()

    return assemble_canonical_shadow_execution_inputs(
        matchup_input=CanonicalMatchupInput(
            game_pk=123,
            away_lineup=lineup("away"),
            home_lineup=lineup("home"),
            away_pitching_plan=(
                pitching_plan("away")
            ),
            home_pitching_plan=(
                pitching_plan("home")
            ),
            probability_provider=provider,
        ),
        exact_artifact=CanonicalProbabilityArtifact(
            provider=provider,
            records=(),
        ),
        fallback_catalog=(
            CanonicalProbabilityFallbackCatalog(
                provider=provider,
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
        ),
        fallback_policy=(
            CanonicalProbabilityFallbackPolicy(
                tiers=(
                    CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
                    CanonicalProbabilityFallbackTier.GLOBAL,
                )
            )
        ),
        game_config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
        ),
    )


def trial_batch():
    box_score = ReducedBoxScore(
        away=TeamBoxScore(
            team_side="away",
            runs=0,
            hits=0,
        ),
        home=TeamBoxScore(
            team_side="home",
            runs=0,
            hits=0,
        ),
        pitcher_attribution_complete=True,
    )

    projections = aggregate_projection_payload(
        box_scores=(box_score,),
        model_version="atomic-provenance-test-v1",
        replay_validation_passes=(True,),
    )

    outcomes = CanonicalGameOutcomeProjection(
        simulation_count=1,
        away_win_probability=0.0,
        home_win_probability=0.0,
        tie_probability=1.0,
        extra_innings_probability=0.0,
        walk_off_probability=0.0,
        away_run_distribution=(
            DistributionPoint(
                value=0,
                probability=1.0,
            ),
        ),
        home_run_distribution=(
            DistributionPoint(
                value=0,
                probability=1.0,
            ),
        ),
        total_run_distribution=(
            DistributionPoint(
                value=0,
                probability=1.0,
            ),
        ),
        team_total_probabilities=(
            ProbabilityMetric(
                name="away_3_plus",
                probability=0.0,
            ),
            ProbabilityMetric(
                name="home_3_plus",
                probability=0.0,
            ),
        ),
        total_probabilities=(
            ProbabilityMetric(
                name="over_6.5",
                probability=0.0,
            ),
            ProbabilityMetric(
                name="under_6.5",
                probability=1.0,
            ),
        ),
    )

    return CanonicalTrialBatch(
        games=(object(),),
        box_scores=(box_score,),
        reconciliations=(object(),),
        outcomes=outcomes,
        projections=projections,
        diagnostics=CanonicalTrialDiagnostics(
            game_validation_pass_rate=1.0,
            box_score_reconciliation_pass_rate=1.0,
        ),
    )


def diagnostics_snapshot():
    from mlb_app.simulation.game import (
        CanonicalProbabilityResolutionDiagnosticsCollector,
    )

    return (
        CanonicalProbabilityResolutionDiagnosticsCollector()
        .snapshot()
    )


def legacy_result():
    return {
        "model_version": "legacy-test-v1",
        "simulations": 1,
        "away_expected_runs": 0.0,
        "home_expected_runs": 0.0,
        "total_expected_runs": 0.0,
        "home_win_probability": 0.0,
        "away_run_distribution": {"0": 1.0},
        "home_run_distribution": {"0": 1.0},
        "total_run_distribution": {"0": 1.0},
        "metadata": {
            "simulation_count": 1,
        },
    }


def test_existing_bundle_without_inputs_remains_valid():
    bundle = CanonicalShadowExecutionBundle(
        trial_batch=trial_batch(),
        probability_resolution_diagnostics=(
            diagnostics_snapshot()
        ),
    )

    assert (
        bundle.canonical_shadow_execution_inputs
        is None
    )


def test_bundle_rejects_invalid_optional_inputs():
    with pytest.raises(
        TypeError,
        match="canonical_shadow_execution_inputs",
    ):
        CanonicalShadowExecutionBundle(
            trial_batch=trial_batch(),
            probability_resolution_diagnostics=(
                diagnostics_snapshot()
            ),
            canonical_shadow_execution_inputs="invalid",
        )


def test_bundle_material_carries_same_inputs():
    inputs = execution_inputs()
    bundle = CanonicalShadowExecutionBundle(
        trial_batch=trial_batch(),
        probability_resolution_diagnostics=(
            diagnostics_snapshot()
        ),
        canonical_shadow_execution_inputs=inputs,
    )

    material = (
        canonical_shadow_execution_bundle_to_material(
            bundle
        )
    )

    assert (
        material.canonical_shadow_execution_inputs
        is inputs
    )


def test_first_class_factory_includes_inputs():
    inputs = execution_inputs()
    factory = inputs.build_factory()

    bundle = factory(
        factory_input=(
            build_canonical_trial_factory_input(
                game_pk=123,
                config={
                    "simulation_count": 1,
                    "seed": 12345,
                    "canonical_model_version": (
                        "atomic-provenance-test-v1"
                    ),
                },
            )
        )
    )

    assert (
        bundle.canonical_shadow_execution_inputs
        is not None
    )
    assert (
        bundle.canonical_shadow_execution_inputs
        .assembly_digest
        == inputs.assembly_digest
    )


def test_builder_material_preserves_two_value_unpacking():
    inputs = execution_inputs()
    factory = inputs.build_factory()

    resolved = builder._canonical_shadow_material(
        {
            "simulation_count": 1,
            "seed": 12345,
            "canonical_model_version": (
                "atomic-provenance-test-v1"
            ),
        },
        game_pk=123,
        trial_batch_factory=factory,
    )

    payload, diagnostics = resolved

    assert payload["simulation_count"] == 1
    assert diagnostics.total_resolutions == 6
    assert (
        resolved.canonical_shadow_execution_inputs
        is not None
    )


def test_builder_attaches_all_atomic_outputs():
    inputs = execution_inputs()
    factory = inputs.build_factory()

    output = (
        builder._attach_canonical_shadow_diagnostics(
            legacy_result(),
            config={
                "canonical_shadow_enabled": True,
                "simulation_count": 1,
                "seed": 12345,
                "canonical_model_version": (
                    "atomic-provenance-test-v1"
                ),
            },
            game_pk=123,
            trial_batch_factory=factory,
        )
    )

    shadow = output[
        "diagnostics"
    ]["canonical_shadow"]

    assert shadow["canonical_available"] is True
    assert (
        shadow["probability_resolution"]
        ["summary"]["total_resolutions"]
        == 6
    )
    assert (
        shadow["input_provenance"]
        ["assembly_digest"]
        == inputs.assembly_digest
    )
    assert shadow["authoritative_source"] == "legacy"


def test_explicit_bundle_attaches_input_provenance():
    inputs = execution_inputs()
    bundle = CanonicalShadowExecutionBundle(
        trial_batch=trial_batch(),
        probability_resolution_diagnostics=(
            diagnostics_snapshot()
        ),
        canonical_shadow_execution_inputs=inputs,
    )

    output = (
        builder._attach_canonical_shadow_diagnostics(
            legacy_result(),
            config={
                "canonical_shadow_enabled": True,
                "canonical_shadow_execution_bundle": (
                    bundle
                ),
            },
            game_pk=123,
        )
    )

    assert (
        output["diagnostics"]["canonical_shadow"]
        ["input_provenance"]["assembly_digest"]
        == inputs.assembly_digest
    )


def test_plain_trial_batch_path_omits_provenance():
    output = (
        builder._attach_canonical_shadow_diagnostics(
            legacy_result(),
            config={
                "canonical_shadow_enabled": True,
                "canonical_shadow_trial_batch": (
                    trial_batch()
                ),
            },
            game_pk=123,
        )
    )

    shadow = output[
        "diagnostics"
    ]["canonical_shadow"]

    assert "input_provenance" not in shadow
    assert shadow["authoritative_source"] == "legacy"
