import pytest

from mlb_app.simulation.events import GameState
from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
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
    build_canonical_probability_fallback_provider,
)


def provider_identity(
    artifact_id="fallback-artifact",
):
    return CanonicalProbabilityProviderIdentity(
        provider_name="fallback-test",
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


def exact_artifact(records=()):
    return CanonicalProbabilityArtifact(
        provider=provider_identity(),
        records=tuple(records),
    )


def fallback_record(
    tier,
    identity,
    selected,
):
    return CanonicalProbabilityFallbackRecord(
        tier=tier,
        identity=identity,
        probabilities=probabilities(selected),
    )


def catalog(records=(), provider=None):
    return CanonicalProbabilityFallbackCatalog(
        provider=provider or provider_identity(),
        records=tuple(records),
    )


def query(
    batter_id="away_batter_0",
    pitcher_id="home_starter",
    matchup_input=None,
):
    return CanonicalPlateAppearanceQuery(
        matchup_input=(
            matchup_input or matchup()
        ),
        state=GameState(
            inning=1,
            half="top",
        ),
        batter_id=batter_id,
        pitcher_id=pitcher_id,
        sequence=0,
        trial_index=0,
        trial_seed=12345,
    )


def policy(*tiers):
    return CanonicalProbabilityFallbackPolicy(
        tiers=tuple(tiers),
    )


def test_default_policy_preserves_fail_closed_exact_lookup():
    adapter = CanonicalProbabilityFallbackAdapter(
        exact_artifact=exact_artifact(),
        fallback_catalog=catalog(
            (
                fallback_record(
                    CanonicalProbabilityFallbackTier.GLOBAL,
                    None,
                    CanonicalPlateAppearanceOutcome.WALK,
                ),
            )
        ),
    )

    with pytest.raises(
        KeyError,
        match="enabled_tiers",
    ):
        adapter(query())


def test_exact_matchup_has_priority():
    exact = CanonicalProbabilityArtifactRecord(
        batter_id="away_batter_0",
        pitcher_id="home_starter",
        probabilities=probabilities(
            CanonicalPlateAppearanceOutcome.STRIKEOUT
        ),
    )
    adapter = CanonicalProbabilityFallbackAdapter(
        exact_artifact=exact_artifact((exact,)),
        fallback_catalog=catalog(
            (
                fallback_record(
                    CanonicalProbabilityFallbackTier.GLOBAL,
                    None,
                    CanonicalPlateAppearanceOutcome.WALK,
                ),
            )
        ),
        policy=policy(
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
            CanonicalProbabilityFallbackTier.GLOBAL,
        ),
    )

    resolution = adapter.resolve(query())

    assert resolution.tier is (
        CanonicalProbabilityFallbackTier.EXACT_MATCHUP
    )
    assert resolution.probabilities.probability_for(
        CanonicalPlateAppearanceOutcome.STRIKEOUT
    ) == 1.0


def test_batter_fallback_is_observable():
    adapter = CanonicalProbabilityFallbackAdapter(
        exact_artifact=exact_artifact(),
        fallback_catalog=catalog(
            (
                fallback_record(
                    CanonicalProbabilityFallbackTier.BATTER,
                    "away_batter_0",
                    CanonicalPlateAppearanceOutcome.SINGLE,
                ),
            )
        ),
        policy=policy(
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
            CanonicalProbabilityFallbackTier.BATTER,
        ),
    )

    resolution = adapter.resolve(query())

    assert resolution.tier is (
        CanonicalProbabilityFallbackTier.BATTER
    )
    assert resolution.source_identity == "away_batter_0"
    assert resolution.probabilities.probability_for(
        CanonicalPlateAppearanceOutcome.SINGLE
    ) == 1.0


def test_pitcher_fallback_is_observable():
    adapter = CanonicalProbabilityFallbackAdapter(
        exact_artifact=exact_artifact(),
        fallback_catalog=catalog(
            (
                fallback_record(
                    CanonicalProbabilityFallbackTier.PITCHER,
                    "home_starter",
                    CanonicalPlateAppearanceOutcome.OUT,
                ),
            )
        ),
        policy=policy(
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
            CanonicalProbabilityFallbackTier.PITCHER,
        ),
    )

    resolution = adapter.resolve(query())

    assert resolution.tier is (
        CanonicalProbabilityFallbackTier.PITCHER
    )
    assert resolution.source_identity == "home_starter"


def test_global_fallback_requires_explicit_policy_tier():
    adapter = CanonicalProbabilityFallbackAdapter(
        exact_artifact=exact_artifact(),
        fallback_catalog=catalog(
            (
                fallback_record(
                    CanonicalProbabilityFallbackTier.GLOBAL,
                    None,
                    CanonicalPlateAppearanceOutcome.WALK,
                ),
            )
        ),
        policy=policy(
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
            CanonicalProbabilityFallbackTier.GLOBAL,
        ),
    )

    resolution = adapter.resolve(query())

    assert resolution.tier is (
        CanonicalProbabilityFallbackTier.GLOBAL
    )
    assert resolution.source_identity is None


def test_policy_order_controls_selected_fallback():
    records = (
        fallback_record(
            CanonicalProbabilityFallbackTier.BATTER,
            "away_batter_0",
            CanonicalPlateAppearanceOutcome.SINGLE,
        ),
        fallback_record(
            CanonicalProbabilityFallbackTier.PITCHER,
            "home_starter",
            CanonicalPlateAppearanceOutcome.DOUBLE,
        ),
    )
    adapter = CanonicalProbabilityFallbackAdapter(
        exact_artifact=exact_artifact(),
        fallback_catalog=catalog(records),
        policy=policy(
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
            CanonicalProbabilityFallbackTier.PITCHER,
            CanonicalProbabilityFallbackTier.BATTER,
        ),
    )

    resolution = adapter.resolve(query())

    assert resolution.tier is (
        CanonicalProbabilityFallbackTier.PITCHER
    )
    assert resolution.probabilities.probability_for(
        CanonicalPlateAppearanceOutcome.DOUBLE
    ) == 1.0


def test_provider_callable_returns_canonical_probabilities():
    provider = build_canonical_probability_fallback_provider(
        exact_artifact=exact_artifact(),
        fallback_catalog=catalog(
            (
                fallback_record(
                    CanonicalProbabilityFallbackTier.GLOBAL,
                    None,
                    CanonicalPlateAppearanceOutcome.HOME_RUN,
                ),
            )
        ),
        policy=policy(
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
            CanonicalProbabilityFallbackTier.GLOBAL,
        ),
    )

    result = provider(query())

    assert result.probability_for(
        CanonicalPlateAppearanceOutcome.HOME_RUN
    ) == 1.0


def test_catalog_rejects_duplicate_tier_identity_rows():
    value = fallback_record(
        CanonicalProbabilityFallbackTier.GLOBAL,
        None,
        CanonicalPlateAppearanceOutcome.WALK,
    )

    with pytest.raises(
        ValueError,
        match="duplicate",
    ):
        catalog((value, value))


def test_policy_requires_exact_tier_first():
    with pytest.raises(
        ValueError,
        match="exact matchup",
    ):
        policy(
            CanonicalProbabilityFallbackTier.GLOBAL,
        )


def test_adapter_rejects_provider_identity_mismatch():
    with pytest.raises(
        ValueError,
        match="same provider identity",
    ):
        CanonicalProbabilityFallbackAdapter(
            exact_artifact=exact_artifact(),
            fallback_catalog=catalog(
                provider=provider_identity(
                    "different-artifact"
                )
            ),
        )
