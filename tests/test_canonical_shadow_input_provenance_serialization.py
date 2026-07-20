from __future__ import annotations

from copy import deepcopy
import json

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
    CanonicalProbabilityFallbackPolicy,
    CanonicalProbabilityFallbackRecord,
    CanonicalProbabilityFallbackTier,
    CanonicalProbabilityProviderIdentity,
)
from mlb_app.simulation.shadow import (
    CANONICAL_SHADOW_INPUT_PROVENANCE_VERSION,
    assemble_canonical_shadow_execution_inputs,
    attach_canonical_shadow,
    canonical_shadow_input_provenance_to_dict,
)


def provider_identity():
    return CanonicalProbabilityProviderIdentity(
        provider_name="input-provenance-test",
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
            f"{side}_reliever_1",
            f"{side}_reliever_2",
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

    matchup = CanonicalMatchupInput(
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
    )

    exact = CanonicalProbabilityArtifact(
        provider=provider,
        records=(
            CanonicalProbabilityArtifactRecord(
                batter_id="away_batter_0",
                pitcher_id="home_starter",
                probabilities=probabilities(),
            ),
        ),
    )

    fallback = CanonicalProbabilityFallbackCatalog(
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

    policy = CanonicalProbabilityFallbackPolicy(
        tiers=(
            CanonicalProbabilityFallbackTier.EXACT_MATCHUP,
            CanonicalProbabilityFallbackTier.GLOBAL,
        )
    )

    return assemble_canonical_shadow_execution_inputs(
        matchup_input=matchup,
        exact_artifact=exact,
        fallback_catalog=fallback,
        fallback_policy=policy,
        game_config=CanonicalGameConfig(
            regulation_innings=9,
            max_extra_innings=3,
            automatic_runner_enabled=True,
            max_plate_appearances_per_half=75,
        ),
    )


def legacy_result():
    return {
        "mean": 4.0,
        "diagnostics": {
            "source": "legacy",
        },
    }


def test_serialization_is_versioned_and_json_safe():
    payload = (
        canonical_shadow_input_provenance_to_dict(
            execution_inputs()
        )
    )

    assert payload["schema_version"] == (
        CANONICAL_SHADOW_INPUT_PROVENANCE_VERSION
    )
    assert len(payload["assembly_digest"]) == 64

    encoded = json.dumps(
        payload,
        sort_keys=True,
    )

    assert "input-provenance-test" in encoded
    assert "exact_matchup" in encoded


def test_serialization_exposes_provenance_not_records():
    payload = (
        canonical_shadow_input_provenance_to_dict(
            execution_inputs()
        )
    )

    assert payload["artifacts"]["exact"][
        "record_count"
    ] == 1
    assert payload["artifacts"]["fallback_catalog"][
        "record_count"
    ] == 1
    assert (
        payload["probability_records_exposed"]
        is False
    )
    assert "records" not in payload["artifacts"]["exact"]
    assert (
        "records"
        not in payload["artifacts"]["fallback_catalog"]
    )


def test_serialization_uses_string_policy_tiers():
    payload = (
        canonical_shadow_input_provenance_to_dict(
            execution_inputs()
        )
    )

    assert payload["fallback_policy"]["tiers"] == [
        "exact_matchup",
        "global",
    ]


def test_serialization_includes_game_configuration():
    payload = (
        canonical_shadow_input_provenance_to_dict(
            execution_inputs()
        )
    )

    assert payload["game_config"] == {
        "regulation_innings": 9,
        "max_extra_innings": 3,
        "automatic_runner_enabled": True,
        "max_plate_appearances_per_half": 75,
    }


def test_omitted_inputs_preserve_existing_output():
    original = legacy_result()

    output = attach_canonical_shadow(
        legacy_result=original,
        enabled=False,
    )

    assert "input_provenance" not in (
        output["diagnostics"]["canonical_shadow"]
    )


def test_inputs_attach_under_canonical_shadow():
    value = execution_inputs()

    output = attach_canonical_shadow(
        legacy_result=legacy_result(),
        enabled=False,
        canonical_shadow_execution_inputs=value,
    )

    attached = output[
        "diagnostics"
    ]["canonical_shadow"]["input_provenance"]

    assert attached["assembly_digest"] == (
        value.assembly_digest
    )
    assert attached["authoritative_source"] == "legacy"


def test_attachment_does_not_mutate_legacy_input():
    original = legacy_result()
    snapshot = deepcopy(original)

    attach_canonical_shadow(
        legacy_result=original,
        enabled=False,
        canonical_shadow_execution_inputs=(
            execution_inputs()
        ),
    )

    assert original == snapshot


def test_invalid_optional_inputs_fail_open():
    output = attach_canonical_shadow(
        legacy_result=legacy_result(),
        enabled=False,
        canonical_shadow_execution_inputs="invalid",
    )

    attached = output[
        "diagnostics"
    ]["canonical_shadow"]["input_provenance"]

    assert attached["status"] == "error"
    assert attached["error_type"] == "TypeError"
    assert attached["authoritative_source"] == "legacy"
    assert output["mean"] == 4.0
