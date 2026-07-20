from dataclasses import replace

import pytest

from mlb_app.simulation.events import GameState
from mlb_app.simulation.game import (
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalLineup,
    CanonicalMatchupInput,
    CanonicalOutcomeProbability,
    CanonicalPitchingPlan,
    CanonicalPlateAppearanceOutcome,
    CanonicalPlateAppearanceProbabilities,
    CanonicalPlateAppearanceQuery,
    CanonicalProbabilityProviderIdentity,
    derive_canonical_pa_sampling_seed,
    sample_canonical_plate_appearance,
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


def provider():
    return CanonicalProbabilityProviderIdentity(
        provider_name="probability-test",
        provider_version="v1",
        artifact_id="artifact-1",
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
        probability_provider=provider(),
    )


def query(
    *,
    sequence=0,
    trial_seed=98765,
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
        trial_index=0,
        trial_seed=trial_seed,
    )


def distribution(
    pa_query=None,
    masses=None,
):
    pa_query = pa_query or query()
    masses = masses or {
        CanonicalPlateAppearanceOutcome.OUT: 0.50,
        CanonicalPlateAppearanceOutcome.SINGLE: 0.15,
        CanonicalPlateAppearanceOutcome.DOUBLE: 0.07,
        CanonicalPlateAppearanceOutcome.TRIPLE: 0.01,
        CanonicalPlateAppearanceOutcome.HOME_RUN: 0.05,
        CanonicalPlateAppearanceOutcome.WALK: 0.08,
        CanonicalPlateAppearanceOutcome.HIT_BY_PITCH: 0.02,
        CanonicalPlateAppearanceOutcome.STRIKEOUT: 0.12,
    }

    return CanonicalPlateAppearanceProbabilities(
        query=pa_query,
        probabilities=tuple(
            CanonicalOutcomeProbability(
                outcome=outcome,
                probability=masses[outcome],
            )
            for outcome in CANONICAL_PA_OUTCOME_ORDER
        ),
        provider=pa_query.matchup_input.probability_provider,
    )


def test_complete_distribution_validates():
    probabilities = distribution()

    assert sum(
        point.probability
        for point in probabilities.probabilities
    ) == pytest.approx(1.0)

    assert probabilities.probability_for(
        CanonicalPlateAppearanceOutcome.HOME_RUN
    ) == 0.05


def test_distribution_requires_canonical_order():
    probabilities = list(
        distribution().probabilities
    )
    probabilities[0], probabilities[1] = (
        probabilities[1],
        probabilities[0],
    )

    with pytest.raises(
        ValueError,
        match="canonical order",
    ):
        CanonicalPlateAppearanceProbabilities(
            query=query(),
            probabilities=tuple(probabilities),
            provider=provider(),
        )


def test_distribution_requires_unit_probability_mass():
    masses = {
        outcome: 0.10
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    }

    with pytest.raises(
        ValueError,
        match="must sum to 1",
    ):
        distribution(masses=masses)


def test_query_rejects_invalid_pitcher_identity():
    with pytest.raises(
        ValueError,
        match="fielding team pitching plan",
    ):
        CanonicalPlateAppearanceQuery(
            matchup_input=matchup(),
            state=GameState(
                inning=1,
                half="top",
            ),
            batter_id="away_batter_0",
            pitcher_id="unknown_pitcher",
            sequence=0,
            trial_index=0,
            trial_seed=123,
        )


def test_same_identity_reproduces_sample():
    probabilities = distribution()

    first = sample_canonical_plate_appearance(
        probabilities
    )
    second = sample_canonical_plate_appearance(
        probabilities
    )

    assert first == second


def test_sequence_changes_sampling_identity():
    first_query = query(sequence=0)
    second_query = replace(
        first_query,
        sequence=1,
    )

    first_seed = derive_canonical_pa_sampling_seed(
        query=first_query,
        provider=first_query.matchup_input.probability_provider,
    )
    second_seed = derive_canonical_pa_sampling_seed(
        query=second_query,
        provider=second_query.matchup_input.probability_provider,
    )

    assert first_seed != second_seed


def test_zero_mass_outcomes_are_never_selected():
    masses = {
        outcome: 0.0
        for outcome in CANONICAL_PA_OUTCOME_ORDER
    }
    masses[CanonicalPlateAppearanceOutcome.WALK] = 1.0

    sampled = sample_canonical_plate_appearance(
        distribution(masses=masses)
    )

    assert (
        sampled.outcome
        is CanonicalPlateAppearanceOutcome.WALK
    )
