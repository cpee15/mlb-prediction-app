from mlb_app.simulation.events import GameState
from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalGameConfig,
    CanonicalLineup,
    CanonicalMatchupInput,
    CanonicalOutcomeProbability,
    CanonicalPitchingPlan,
    CanonicalPlateAppearanceOutcome,
    CanonicalPlateAppearanceQuery,
    CanonicalProbabilityArtifact,
    CanonicalProbabilityArtifactRecord,
    CanonicalProbabilityFallbackAdapter,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackPolicy,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
    CanonicalProbabilityResolutionDiagnosticsCollector,
    CanonicalTrialExecutionPlan,
    build_canonical_pa_resolver_factory,
    build_canonical_probability_diagnostics_provider,
    build_canonical_trial_factory_input,
    run_canonical_trial_execution_plan,
)


def provider_identity():
    return CanonicalProbabilityProviderIdentity(
        provider_name="diagnostics-test",
        provider_version="v1",
        artifact_id="diagnostics-artifact",
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


def matchup():
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
        probability_provider=provider_identity(),
    )


def probabilities(selected):
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
    batter_id="away_batter_0",
    pitcher_id="home_starter",
):
    return CanonicalProbabilityArtifactRecord(
        batter_id=batter_id,
        pitcher_id=pitcher_id,
        probabilities=probabilities(
            CanonicalPlateAppearanceOutcome.STRIKEOUT
        ),
    )


def exact_artifact(records=()):
    return CanonicalProbabilityArtifact(
        provider=provider_identity(),
        records=tuple(records),
    )


def global_catalog():
    return CanonicalProbabilityFallbackCatalog(
        provider=provider_identity(),
        records=(
            CanonicalProbabilityFallbackRecord(
                tier=(
                    CanonicalProbabilityFallbackTier.GLOBAL
                ),
                identity=None,
                probabilities=probabilities(
                    CanonicalPlateAppearanceOutcome.STRIKEOUT
                ),
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


def adapter(records=()):
    return CanonicalProbabilityFallbackAdapter(
        exact_artifact=exact_artifact(records),
        fallback_catalog=global_catalog(),
        policy=fallback_policy(),
    )


def query(
    *,
    batter_id="away_batter_0",
    sequence=0,
):
    return CanonicalPlateAppearanceQuery(
        matchup_input=matchup(),
        state=GameState(
            inning=1,
            half="top",
        ),
        batter_id=batter_id,
        pitcher_id="home_starter",
        sequence=sequence,
        trial_index=0,
        trial_seed=12345,
    )


def test_empty_collector_snapshot():
    diagnostics = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
        .snapshot()
    )

    assert diagnostics.total_resolutions == 0
    assert diagnostics.exact_resolutions == 0
    assert diagnostics.fallback_resolutions == 0
    assert diagnostics.fallback_rate == 0.0


def test_exact_resolution_is_observed():
    collector = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
    )
    provider = (
        build_canonical_probability_diagnostics_provider(
            fallback_adapter=adapter(
                (exact_record(),)
            ),
            collector=collector,
        )
    )

    provider(query())

    diagnostics = collector.snapshot()

    assert diagnostics.total_resolutions == 1
    assert diagnostics.exact_resolutions == 1
    assert diagnostics.fallback_resolutions == 0
    assert diagnostics.observations[0].tier is (
        CanonicalProbabilityFallbackTier.EXACT_MATCHUP
    )


def test_global_fallback_is_observed():
    collector = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
    )
    provider = (
        build_canonical_probability_diagnostics_provider(
            fallback_adapter=adapter(),
            collector=collector,
        )
    )

    provider(query())

    diagnostics = collector.snapshot()

    assert diagnostics.fallback_resolutions == 1
    assert diagnostics.fallback_rate == 1.0
    assert diagnostics.observations[0].tier is (
        CanonicalProbabilityFallbackTier.GLOBAL
    )


def test_mixed_tier_counts_reconcile():
    collector = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
    )
    provider = (
        build_canonical_probability_diagnostics_provider(
            fallback_adapter=adapter(
                (exact_record(),)
            ),
            collector=collector,
        )
    )

    provider(query())
    provider(
        query(
            batter_id="away_batter_1",
            sequence=1,
        )
    )

    diagnostics = collector.snapshot()

    assert diagnostics.total_resolutions == 2
    assert diagnostics.exact_resolutions == 1
    assert diagnostics.fallback_resolutions == 1
    assert diagnostics.fallback_rate == 0.5


def test_wrapper_returns_underlying_probabilities_unchanged():
    fallback_adapter = adapter()
    collector = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
    )
    pa_query = query()

    expected = fallback_adapter.resolve(
        pa_query
    ).probabilities
    actual = (
        build_canonical_probability_diagnostics_provider(
            fallback_adapter=fallback_adapter,
            collector=collector,
        )(pa_query)
    )

    assert actual == expected


def test_observation_preserves_resolution_provenance():
    fallback_adapter = adapter()
    collector = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
    )
    provider = (
        build_canonical_probability_diagnostics_provider(
            fallback_adapter=fallback_adapter,
            collector=collector,
        )
    )

    provider(query())

    observation = collector.snapshot().observations[0]

    assert observation.exact_artifact_digest == (
        fallback_adapter.exact_artifact.digest
    )
    assert observation.fallback_catalog_digest == (
        fallback_adapter.fallback_catalog.digest
    )
    assert observation.policy_version == (
        fallback_adapter.policy.policy_version
    )


def build_execution_plan(collector):
    matchup_input = matchup()
    factory_input = build_canonical_trial_factory_input(
        game_pk=123,
        config={
            "simulation_count": 2,
            "seed": 98765,
            "canonical_model_version": (
                "probability-diagnostics-test-v1"
            ),
        },
    )
    provider = (
        build_canonical_probability_diagnostics_provider(
            fallback_adapter=adapter(),
            collector=collector,
        )
    )

    return CanonicalTrialExecutionPlan(
        factory_input=factory_input,
        away_lineup=matchup_input.away_lineup,
        home_lineup=matchup_input.home_lineup,
        resolver_factory=(
            build_canonical_pa_resolver_factory(
                provider
            )
        ),
        game_config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
        ),
        matchup_input=matchup_input,
    )


def test_full_trial_batch_aggregates_resolution_usage():
    collector = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
    )

    batch = run_canonical_trial_execution_plan(
        build_execution_plan(collector)
    )
    diagnostics = collector.snapshot()

    assert len(batch.games) == 2
    assert diagnostics.total_resolutions == 12
    assert diagnostics.exact_resolutions == 0
    assert diagnostics.fallback_resolutions == 12
    assert diagnostics.count_for(
        CanonicalProbabilityFallbackTier.GLOBAL
    ) == 12


def test_full_trial_batch_diagnostics_replay_identically():
    first_collector = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
    )
    second_collector = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
    )

    first_batch = run_canonical_trial_execution_plan(
        build_execution_plan(first_collector)
    )
    second_batch = run_canonical_trial_execution_plan(
        build_execution_plan(second_collector)
    )

    assert first_batch == second_batch
    assert (
        first_collector.snapshot()
        == second_collector.snapshot()
    )
