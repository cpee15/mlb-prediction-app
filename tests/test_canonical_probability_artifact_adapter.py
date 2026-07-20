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
    CanonicalProbabilityArtifactAdapter,
    CanonicalProbabilityArtifactRecord,
    CanonicalProbabilityProviderIdentity,
    build_canonical_probability_artifact_provider,
)


def provider_identity(
    *,
    artifact_id="artifact-1",
):
    return CanonicalProbabilityProviderIdentity(
        provider_name="artifact-test",
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


def matchup(
    *,
    provider=None,
):
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


def record(
    *,
    batter_id="away_batter_0",
    pitcher_id="home_starter",
):
    return CanonicalProbabilityArtifactRecord(
        batter_id=batter_id,
        pitcher_id=pitcher_id,
        probabilities=probabilities(),
    )


def artifact(
    *,
    provider=None,
    records=None,
):
    return CanonicalProbabilityArtifact(
        provider=provider or provider_identity(),
        records=tuple(
            records
            if records is not None
            else (record(),)
        ),
    )


def query(
    *,
    matchup_input=None,
    batter_id="away_batter_0",
    pitcher_id="home_starter",
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


def test_adapter_maps_exact_artifact_row():
    value = artifact()
    adapter = (
        build_canonical_probability_artifact_provider(
            value
        )
    )
    pa_query = query()

    result = adapter(pa_query)

    assert result.query == pa_query
    assert result.provider == value.provider
    assert result.probabilities == (
        value.records[0].probabilities
    )
    assert result.probability_for(
        CanonicalPlateAppearanceOutcome.STRIKEOUT
    ) == 1.0


def test_artifact_digest_is_order_independent():
    first_record = record()
    second_record = record(
        batter_id="away_batter_1",
    )

    first = artifact(
        records=(
            first_record,
            second_record,
        )
    )
    second = artifact(
        records=(
            second_record,
            first_record,
        )
    )

    assert first.digest == second.digest
    assert len(first.digest) == 64


def test_artifact_rejects_duplicate_matchup_rows():
    value = record()

    with pytest.raises(
        ValueError,
        match="duplicate",
    ):
        artifact(
            records=(
                value,
                value,
            )
        )


def test_artifact_record_requires_complete_distribution():
    with pytest.raises(
        ValueError,
        match="canonical order",
    ):
        CanonicalProbabilityArtifactRecord(
            batter_id="away_batter_0",
            pitcher_id="home_starter",
            probabilities=probabilities()[:-1],
        )


def test_adapter_rejects_provider_identity_mismatch():
    artifact_value = artifact()
    mismatched_matchup = matchup(
        provider=provider_identity(
            artifact_id="artifact-2",
        )
    )
    adapter = CanonicalProbabilityArtifactAdapter(
        artifact=artifact_value,
    )

    with pytest.raises(
        ValueError,
        match="provider must match",
    ):
        adapter(
            query(
                matchup_input=mismatched_matchup,
            )
        )


def test_adapter_fails_closed_for_missing_row():
    value = artifact()
    adapter = CanonicalProbabilityArtifactAdapter(
        artifact=value,
    )

    with pytest.raises(
        KeyError,
        match="has no row",
    ):
        adapter(
            query(
                batter_id="away_batter_1",
            )
        )


def test_digest_changes_with_probability_mass():
    first = artifact()

    changed_record = (
        CanonicalProbabilityArtifactRecord(
            batter_id="away_batter_0",
            pitcher_id="home_starter",
            probabilities=probabilities(
                CanonicalPlateAppearanceOutcome.WALK
            ),
        )
    )
    second = artifact(
        records=(
            changed_record,
        )
    )

    assert first.digest != second.digest
