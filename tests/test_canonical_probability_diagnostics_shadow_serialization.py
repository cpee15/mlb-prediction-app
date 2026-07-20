from copy import deepcopy

from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalLineup,
    CanonicalMatchupInput,
    CanonicalOutcomeProbability,
    CanonicalPitchingPlan,
    CanonicalPlateAppearanceOutcome,
    CanonicalPlateAppearanceQuery,
    CanonicalProbabilityArtifact,
    CanonicalProbabilityFallbackAdapter,
    CanonicalProbabilityFallbackCatalog,
    CanonicalProbabilityFallbackPolicy,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
    CanonicalProbabilityResolutionDiagnosticsCollector,
)
from mlb_app.simulation.events import GameState
from mlb_app.simulation.shadow import (
    CANONICAL_PROBABILITY_DIAGNOSTICS_SHADOW_VERSION,
    attach_canonical_shadow,
    probability_resolution_diagnostics_to_dict,
)


def provider_identity():
    return CanonicalProbabilityProviderIdentity(
        provider_name="shadow-serialization-test",
        provider_version="v1",
        artifact_id="shadow-diagnostics-artifact",
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


def fallback_adapter():
    return CanonicalProbabilityFallbackAdapter(
        exact_artifact=CanonicalProbabilityArtifact(
            provider=provider_identity(),
            records=(),
        ),
        fallback_catalog=(
            CanonicalProbabilityFallbackCatalog(
                provider=provider_identity(),
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
        policy=CanonicalProbabilityFallbackPolicy(
            tiers=(
                CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
                CanonicalProbabilityFallbackTier.GLOBAL,
            )
        ),
    )


def query(
    *,
    trial_index=0,
    sequence=0,
):
    return CanonicalPlateAppearanceQuery(
        matchup_input=matchup(),
        state=GameState(
            inning=1,
            half="top",
        ),
        batter_id="away_batter_0",
        pitcher_id="home_starter",
        sequence=sequence,
        trial_index=trial_index,
        trial_seed=12345,
    )


def diagnostics_snapshot():
    collector = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
    )
    collector.record(
        fallback_adapter().resolve(
            query()
        )
    )
    return collector.snapshot()


def canonical_payload():
    return {
        "simulation_count": 10,
        "mean": 4.5,
        "floor": 2.0,
        "ceiling": 8.0,
    }


def legacy_result():
    return {
        "simulation_count": 10,
        "mean": 4.0,
        "floor": 1.0,
        "ceiling": 7.0,
        "diagnostics": {
            "legacy_marker": True,
        },
    }


def test_serializes_summary_and_schema_versions():
    payload = probability_resolution_diagnostics_to_dict(
        diagnostics_snapshot()
    )

    assert payload["schema_version"] == (
        CANONICAL_PROBABILITY_DIAGNOSTICS_SHADOW_VERSION
    )
    assert payload["summary"] == {
        "total_resolutions": 1,
        "exact_resolutions": 0,
        "fallback_resolutions": 1,
        "fallback_rate": 1.0,
    }


def test_serializes_enum_values_as_json_strings():
    payload = probability_resolution_diagnostics_to_dict(
        diagnostics_snapshot()
    )

    assert payload["tier_usage"][-1] == {
        "tier": "global",
        "count": 1,
    }
    assert payload["observations"][0]["tier"] == "global"
    assert payload["observations"][0]["is_fallback"] is True


def test_empty_snapshot_serializes_deterministically():
    snapshot = (
        CanonicalProbabilityResolutionDiagnosticsCollector()
        .snapshot()
    )

    first = probability_resolution_diagnostics_to_dict(
        snapshot
    )
    second = probability_resolution_diagnostics_to_dict(
        snapshot
    )

    assert first == second
    assert first["observations"] == []
    assert first["summary"]["fallback_rate"] == 0.0


def test_attachment_is_optional_and_default_output_is_unchanged():
    legacy = legacy_result()

    before = attach_canonical_shadow(
        legacy_result=legacy,
        enabled=True,
        canonical_payload=canonical_payload(),
    )
    after = attach_canonical_shadow(
        legacy_result=legacy,
        enabled=True,
        canonical_payload=canonical_payload(),
        probability_resolution_diagnostics=None,
    )

    assert before == after
    assert (
        "probability_resolution"
        not in after["diagnostics"]["canonical_shadow"]
    )


def test_snapshot_attaches_under_canonical_shadow_namespace():
    output = attach_canonical_shadow(
        legacy_result=legacy_result(),
        enabled=True,
        canonical_payload=canonical_payload(),
        probability_resolution_diagnostics=(
            diagnostics_snapshot()
        ),
    )

    attached = output[
        "diagnostics"
    ]["canonical_shadow"]["probability_resolution"]

    assert attached["summary"]["total_resolutions"] == 1
    assert attached["observations"][0]["tier"] == "global"


def test_attachment_does_not_mutate_legacy_input():
    legacy = legacy_result()
    original = deepcopy(legacy)

    attach_canonical_shadow(
        legacy_result=legacy,
        enabled=True,
        canonical_payload=canonical_payload(),
        probability_resolution_diagnostics=(
            diagnostics_snapshot()
        ),
    )

    assert legacy == original


def test_legacy_remains_authoritative_with_attachment():
    output = attach_canonical_shadow(
        legacy_result=legacy_result(),
        enabled=True,
        canonical_payload=canonical_payload(),
        probability_resolution_diagnostics=(
            diagnostics_snapshot()
        ),
    )

    shadow = output["diagnostics"]["canonical_shadow"]

    assert shadow["authoritative_source"] == "legacy"
    assert output["mean"] == 4.0


def test_invalid_optional_diagnostics_fail_open():
    output = attach_canonical_shadow(
        legacy_result=legacy_result(),
        enabled=True,
        canonical_payload=canonical_payload(),
        probability_resolution_diagnostics="invalid",
    )

    attached = output[
        "diagnostics"
    ]["canonical_shadow"]["probability_resolution"]

    assert attached["status"] == "error"
    assert attached["error_type"] == "TypeError"
    assert output["mean"] == 4.0
