from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalGameConfig,
    CanonicalLineup,
    CanonicalMatchupInput,
    CanonicalOutcomeProbability,
    CanonicalPitchingPlan,
    CanonicalPlateAppearanceOutcome,
    CanonicalProbabilityArtifact,
    CanonicalProbabilityArtifactRecord,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
)
from mlb_app.simulation.shadow.hitter_profile_simulation_shadow_overlay import (
    build_hitter_profile_simulation_shadow_overlay,
)


def provider():
    return CanonicalProbabilityProviderIdentity(
        provider_name="base-provider",
        provider_version="v1",
        artifact_id="base-artifact",
    )


def probabilities(k_rate=0.20):
    remaining = 1.0 - k_rate
    values = {
        CanonicalPlateAppearanceOutcome.OUT:
            remaining * 0.60,
        CanonicalPlateAppearanceOutcome.SINGLE:
            remaining * 0.15,
        CanonicalPlateAppearanceOutcome.DOUBLE:
            remaining * 0.06,
        CanonicalPlateAppearanceOutcome.TRIPLE:
            remaining * 0.01,
        CanonicalPlateAppearanceOutcome.HOME_RUN:
            remaining * 0.05,
        CanonicalPlateAppearanceOutcome.WALK:
            remaining * 0.08,
        CanonicalPlateAppearanceOutcome.HIT_BY_PITCH:
            remaining * 0.05,
        CanonicalPlateAppearanceOutcome.STRIKEOUT:
            k_rate,
    }
    return tuple(
        CanonicalOutcomeProbability(
            outcome=outcome,
            probability=values[outcome],
        )
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    )


def inputs():
    identity = provider()
    matchup = CanonicalMatchupInput(
        game_pk=1,
        away_lineup=CanonicalLineup(
            team_side="away",
            player_ids=tuple(
                str(value)
                for value in range(1, 10)
            ),
        ),
        home_lineup=CanonicalLineup(
            team_side="home",
            player_ids=tuple(
                str(value)
                for value in range(11, 20)
            ),
        ),
        away_pitching_plan=CanonicalPitchingPlan(
            team_side="away",
            starter_id="100",
            bullpen_pitcher_ids=("101",),
        ),
        home_pitching_plan=CanonicalPitchingPlan(
            team_side="home",
            starter_id="200",
            bullpen_pitcher_ids=("201",),
        ),
        probability_provider=identity,
    )
    artifact = CanonicalProbabilityArtifact(
        provider=identity,
        records=(
            CanonicalProbabilityArtifactRecord(
                batter_id="1",
                pitcher_id="200",
                probabilities=probabilities(),
            ),
            CanonicalProbabilityArtifactRecord(
                batter_id="2",
                pitcher_id="200",
                probabilities=probabilities(0.25),
            ),
        ),
    )
    catalog = CanonicalProbabilityFallbackCatalog(
        provider=identity,
        records=(
            CanonicalProbabilityFallbackRecord(
                tier=CanonicalProbabilityFallbackTier.GLOBAL,
                identity=None,
                probabilities=probabilities(),
            ),
        ),
    )
    return matchup, artifact, catalog


def gate(passed=True):
    return {
        "status": (
            "accepted_for_feature_flag_integration"
            if passed
            else "blocked"
        ),
        "gate_passed": passed,
        "decision": {
            "feature_flag_integration_allowed":
                passed,
            "production_activation_allowed":
                False,
        },
        "production_authority_changed": False,
    }


def candidate():
    return {
        "status": "ready",
        "executed": True,
        "production_inputs_unchanged": True,
        "production_authority_changed": False,
        "fallback_telemetry": {
            "fallback_count": 0,
        },
        "probability_deltas": {
            "out": 0.02,
            "reached_on_error": 0.0,
            "single": 0.0,
            "double": 0.0,
            "triple": 0.0,
            "hr": 0.0,
            "bb": 0.0,
            "hbp": 0.0,
            "k": -0.02,
        },
    }


def test_disabled_returns_original_inputs():
    matchup, artifact, catalog = inputs()

    result = (
        build_hitter_profile_simulation_shadow_overlay(
            matchup_input=matchup,
            exact_artifact=artifact,
            fallback_catalog=catalog,
            candidate_results={
                "1": candidate(),
            },
        )
    )

    assert result["status"] == "disabled"
    assert result["overlay_applied"] is False
    assert result["matchup_input"] is matchup
    assert result["exact_artifact"] is artifact
    assert result["fallback_catalog"] is catalog


def test_blocked_gate_fails_closed():
    matchup, artifact, catalog = inputs()

    result = (
        build_hitter_profile_simulation_shadow_overlay(
            enabled=True,
            acceptance_gate=gate(False),
            matchup_input=matchup,
            exact_artifact=artifact,
            fallback_catalog=catalog,
            candidate_results={
                "1": candidate(),
            },
        )
    )

    assert result["status"] == "blocked"
    assert result["overlay_applied"] is False
    assert (
        "canary_acceptance_gate_not_passed"
        in result["blockers"]
    )


def test_overlays_only_eligible_exact_rows():
    matchup, artifact, catalog = inputs()

    result = (
        build_hitter_profile_simulation_shadow_overlay(
            enabled=True,
            acceptance_gate=gate(),
            matchup_input=matchup,
            exact_artifact=artifact,
            fallback_catalog=catalog,
            candidate_results={
                "1": candidate(),
            },
        )
    )

    assert result["status"] == "ready"
    assert result["overlay_applied"] is True
    assert result["overlaid_matchup_count"] == 1
    assert result["preserved_matchup_count"] == 1

    shadow_artifact = result["exact_artifact"]
    eligible = shadow_artifact.record_for(
        batter_id="1",
        pitcher_id="200",
    )
    preserved = shadow_artifact.record_for(
        batter_id="2",
        pitcher_id="200",
    )

    original_eligible = artifact.record_for(
        batter_id="1",
        pitcher_id="200",
    )
    original_preserved = artifact.record_for(
        batter_id="2",
        pitcher_id="200",
    )

    assert (
        eligible.probabilities
        != original_eligible.probabilities
    )
    assert (
        preserved.probabilities
        == original_preserved.probabilities
    )
    assert (
        artifact.record_for(
            batter_id="1",
            pitcher_id="200",
        )
        == original_eligible
    )


def test_rebinds_all_canonical_provider_identities():
    matchup, artifact, catalog = inputs()

    result = (
        build_hitter_profile_simulation_shadow_overlay(
            enabled=True,
            acceptance_gate=gate(),
            matchup_input=matchup,
            exact_artifact=artifact,
            fallback_catalog=catalog,
            candidate_results={
                "1": candidate(),
            },
        )
    )

    shadow_provider = result[
        "matchup_input"
    ].probability_provider

    assert shadow_provider != provider()
    assert (
        result["exact_artifact"].provider
        == shadow_provider
    )
    assert (
        result["fallback_catalog"].provider
        == shadow_provider
    )
    assert (
        result["production_authority_changed"]
        is False
    )


def test_preserves_fallback_probability_values():
    matchup, artifact, catalog = inputs()

    result = (
        build_hitter_profile_simulation_shadow_overlay(
            enabled=True,
            acceptance_gate=gate(),
            matchup_input=matchup,
            exact_artifact=artifact,
            fallback_catalog=catalog,
            candidate_results={
                "1": candidate(),
            },
        )
    )

    assert (
        result["fallback_catalog"]
        .records[0]
        .probabilities
        == catalog.records[0].probabilities
    )


def test_ineligible_candidate_uses_original_inputs():
    matchup, artifact, catalog = inputs()
    blocked = candidate()
    blocked["fallback_telemetry"] = {
        "fallback_count": 1,
    }

    result = (
        build_hitter_profile_simulation_shadow_overlay(
            enabled=True,
            acceptance_gate=gate(),
            matchup_input=matchup,
            exact_artifact=artifact,
            fallback_catalog=catalog,
            candidate_results={
                "1": blocked,
            },
        )
    )

    assert result["status"] == "fallback"
    assert result["overlay_applied"] is False
    assert result["exact_artifact"] is artifact


def test_output_is_factory_compatible():
    matchup, artifact, catalog = inputs()

    result = (
        build_hitter_profile_simulation_shadow_overlay(
            enabled=True,
            acceptance_gate=gate(),
            matchup_input=matchup,
            exact_artifact=artifact,
            fallback_catalog=catalog,
            candidate_results={
                "1": candidate(),
            },
        )
    )

    assert (
        result["matchup_input"]
        .probability_provider
        == result["exact_artifact"].provider
        == result["fallback_catalog"].provider
    )
