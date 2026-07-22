from dataclasses import replace

import pytest

from mlb_app.simulation.events import GameState
from mlb_app.simulation.game import (
    CanonicalBullpenPitcher,
    CanonicalBullpenRole,
    CanonicalStarterHookPolicy,
    build_canonical_bullpen_selector,
    CANONICAL_PA_OUTCOME_ORDER,
    CanonicalGameConfig,
    CanonicalLineup,
    CanonicalMatchupInput,
    CanonicalOutcomeProbability,
    CanonicalPitchingPlan,
    CanonicalPlateAppearanceOutcome,
    CanonicalPlateAppearanceProbabilities,
    CanonicalPlateAppearanceResolverFactory,
    CanonicalProbabilityProviderIdentity,
    CanonicalTrialExecutionPlan,
    build_canonical_pa_resolver_factory,
    build_canonical_trial_factory_input,
    build_canonical_trial_resolver_context,
    run_canonical_trial_execution_plan,
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


def provider_identity():
    return CanonicalProbabilityProviderIdentity(
        provider_name="resolver-factory-test",
        provider_version="v1",
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


def factory_input(simulations=1):
    return build_canonical_trial_factory_input(
        game_pk=123,
        config={
            "simulation_count": simulations,
            "seed": 98765,
            "canonical_model_version": (
                "pa-resolver-factory-test-v1"
            ),
        },
    )


def all_out_provider(observed):
    def provide(query):
        observed.append(query)

        return CanonicalPlateAppearanceProbabilities(
            query=query,
            probabilities=tuple(
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
            ),
            provider=query.matchup_input.probability_provider,
        )

    return provide


def test_factory_requires_matchup_input():
    resolver_factory = (
        build_canonical_pa_resolver_factory(
            all_out_provider([])
        )
    )
    context = (
        build_canonical_trial_resolver_context(
            factory_input=factory_input(),
            trial_index=0,
        )
    )

    with pytest.raises(
        ValueError,
        match="requires matchup_input",
    ):
        resolver_factory(context)


def test_top_half_uses_home_starter():
    observed = []
    matchup_input = matchup()
    context = (
        build_canonical_trial_resolver_context(
            factory_input=factory_input(),
            trial_index=0,
            matchup_input=matchup_input,
        )
    )
    resolver = (
        CanonicalPlateAppearanceResolverFactory(
            probability_provider=(
                all_out_provider(observed)
            )
        )(context)
    )

    event = resolver(
        GameState(
            inning=1,
            half="top",
        ),
        "away_batter_0",
        0,
    )

    assert observed[0].pitcher_id == "home_starter"
    assert event.pitcher_id == "home_starter"
    assert event.event_type == "k"


def test_bottom_half_uses_away_starter():
    observed = []
    matchup_input = matchup()
    context = (
        build_canonical_trial_resolver_context(
            factory_input=factory_input(),
            trial_index=0,
            matchup_input=matchup_input,
        )
    )
    resolver = (
        build_canonical_pa_resolver_factory(
            all_out_provider(observed)
        )(context)
    )

    event = resolver(
        GameState(
            inning=1,
            half="bottom",
        ),
        "home_batter_0",
        0,
    )

    assert observed[0].pitcher_id == "away_starter"
    assert event.pitcher_id == "away_starter"


def test_resolver_propagates_trial_identity():
    observed = []
    matchup_input = matchup()
    input_value = factory_input(
        simulations=3
    )
    context = (
        build_canonical_trial_resolver_context(
            factory_input=input_value,
            trial_index=2,
            matchup_input=matchup_input,
        )
    )
    resolver = (
        build_canonical_pa_resolver_factory(
            all_out_provider(observed)
        )(context)
    )

    resolver(
        GameState(
            inning=1,
            half="top",
        ),
        "away_batter_0",
        9,
    )

    assert observed[0].trial_index == 2
    assert observed[0].trial_seed == (
        input_value.seed_for_trial(2)
    )
    assert observed[0].sequence == 9


def test_provider_must_return_matching_query():
    matchup_input = matchup()
    input_value = factory_input()
    context = (
        build_canonical_trial_resolver_context(
            factory_input=input_value,
            trial_index=0,
            matchup_input=matchup_input,
        )
    )

    different_query = None

    def mismatched_provider(query):
        nonlocal different_query
        different_query = replace(
            query,
            sequence=query.sequence + 1,
        )

        return CanonicalPlateAppearanceProbabilities(
            query=different_query,
            probabilities=tuple(
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
            ),
            provider=query.matchup_input.probability_provider,
        )

    resolver = (
        build_canonical_pa_resolver_factory(
            mismatched_provider
        )(context)
    )

    with pytest.raises(
        ValueError,
        match="different query",
    ):
        resolver(
            GameState(
                inning=1,
                half="top",
            ),
            "away_batter_0",
            0,
        )


def test_execution_plan_runs_full_game_pipeline():
    observed = []
    matchup_input = matchup()
    input_value = factory_input(
        simulations=2
    )

    plan = CanonicalTrialExecutionPlan(
        factory_input=input_value,
        away_lineup=matchup_input.away_lineup,
        home_lineup=matchup_input.home_lineup,
        resolver_factory=(
            build_canonical_pa_resolver_factory(
                all_out_provider(observed)
            )
        ),
        game_config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
        ),
        matchup_input=matchup_input,
    )

    batch = run_canonical_trial_execution_plan(
        plan
    )

    assert len(batch.games) == 2
    assert all(
        game.final_state.away_score == 0
        and game.final_state.home_score == 0
        for game in batch.games
    )

    assert len(observed) == 12

    assert {
        query.trial_index
        for query in observed
    } == {0, 1}

    assert {
        query.pitcher_id
        for query in observed
    } == {
        "away_starter",
        "home_starter",
    }


def test_execution_plan_replays_identically():
    matchup_input = matchup()
    input_value = factory_input(
        simulations=2
    )

    def build_plan():
        return CanonicalTrialExecutionPlan(
            factory_input=input_value,
            away_lineup=matchup_input.away_lineup,
            home_lineup=matchup_input.home_lineup,
            resolver_factory=(
                build_canonical_pa_resolver_factory(
                    all_out_provider([])
                )
            ),
            game_config=CanonicalGameConfig(
                regulation_innings=1,
                max_extra_innings=0,
            ),
            matchup_input=matchup_input,
        )

    first = run_canonical_trial_execution_plan(
        build_plan()
    )
    second = run_canonical_trial_execution_plan(
        build_plan()
    )

    assert first == second


def test_resolver_factory_can_change_pitchers_with_manager():
    queries = []

    provider = all_out_provider(queries)

    factory = CanonicalPlateAppearanceResolverFactory(
        probability_provider=provider,
        starter_hook_policy=CanonicalStarterHookPolicy(
            minimum_batters_faced=3,
            target_batters_faced=3,
            maximum_batters_faced=3,
        ),
        bullpen_selector=(
            build_canonical_bullpen_selector()
        ),
        away_bullpen=(
            CanonicalBullpenPitcher(
                pitcher_id="away_reliever",
                role=(
                    CanonicalBullpenRole.LONG_RELIEF
                ),
            ),
        ),
        home_bullpen=(
            CanonicalBullpenPitcher(
                pitcher_id="home_reliever",
                role=(
                    CanonicalBullpenRole.LONG_RELIEF
                ),
            ),
        ),
    )

    matchup_input = matchup()
    context = (
        build_canonical_trial_resolver_context(
            factory_input=factory_input(),
            trial_index=0,
            matchup_input=matchup_input,
        )
    )

    resolver = factory(context)

    state = GameState(
        inning=4,
        half="top",
    )

    for index in range(4):
        state = replace(
            state,
            outs=0,
            batting_order_index=index,
            plate_appearance_number=index,
        )

        event = resolver(
            state,
            f"away_batter_{index}",
            index,
        )

        state = event.state_after

    assert tuple(
        query.pitcher_id
        for query in queries
    ) == (
        "home_starter",
        "home_starter",
        "home_starter",
        "home_reliever",
    )
