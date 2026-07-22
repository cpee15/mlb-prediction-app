from dataclasses import replace

import pytest

from mlb_app.simulation.events import (
    OutRecord,
    PlayEvent,
)
from mlb_app.simulation.game import (
    CanonicalGameConfig,
    CanonicalLineup,
    CanonicalTrialExecutionPlan,
    CanonicalTrialResolverContext,
    build_canonical_trial_factory_input,
    build_canonical_trial_resolver_context,
    run_canonical_trial_execution_plan,
)


def lineup(side):
    return CanonicalLineup(
        team_side=side,
        player_ids=tuple(
            f"{side}_{index}"
            for index in range(9)
        ),
    )


def out_event(state, batter_id, sequence):
    next_out = state.outs + 1

    return PlayEvent(
        sequence=sequence,
        event_type="out",
        batter_id=batter_id,
        state_before=state,
        state_after=replace(
            state,
            outs=next_out,
            batting_order_index=(
                state.batting_order_index + 1
            ) % 9,
            plate_appearance_number=(
                state.plate_appearance_number + 1
            ),
        ),
        outs_recorded=(
            OutRecord(
                runner_id=batter_id,
                out_number=next_out,
                reason="protocol_test_out",
            ),
        ),
    )


def factory_input(simulations=3):
    return build_canonical_trial_factory_input(
        game_pk=123,
        config={
            "simulation_count": simulations,
            "seed": 98765,
            "canonical_model_version": (
                "canonical-protocol-test-v1"
            ),
        },
    )


def test_resolver_context_uses_indexed_seed():
    inputs = factory_input()

    context = (
        build_canonical_trial_resolver_context(
            factory_input=inputs,
            trial_index=1,
        )
    )

    assert isinstance(
        context,
        CanonicalTrialResolverContext,
    )
    assert context.trial_index == 1
    assert context.trial_seed == (
        inputs.seed_for_trial(1)
    )


def test_mismatched_trial_seed_is_rejected():
    inputs = factory_input()

    with pytest.raises(
        ValueError,
        match="trial_seed does not match",
    ):
        CanonicalTrialResolverContext(
            factory_input=inputs,
            trial_index=0,
            trial_seed=inputs.seed_for_trial(0) + 1,
        )


def test_execution_plan_runs_exact_indexed_trials():
    inputs = factory_input(simulations=3)
    observed = []

    def resolver_factory(context):
        observed.append(
            (
                context.trial_index,
                context.trial_seed,
            )
        )

        def resolver(state, batter_id, sequence):
            return out_event(
                state,
                batter_id,
                sequence,
            )

        return resolver

    plan = CanonicalTrialExecutionPlan(
        factory_input=inputs,
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolver_factory=resolver_factory,
        game_config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
            automatic_runner_enabled=False,
        ),
    )

    batch = run_canonical_trial_execution_plan(
        plan
    )

    assert len(batch.games) == 3
    assert batch.outcomes.simulation_count == 3
    assert batch.projections.simulation_count == 3
    assert observed == [
        (
            index,
            inputs.seed_for_trial(index),
        )
        for index in range(3)
    ]
    assert batch.outcomes.tie_probability == 1.0
    assert (
        "tied_games_present"
        in batch.diagnostics.warnings
    )


def test_resolver_factory_runs_once_per_trial():
    calls = []

    def resolver_factory(context):
        calls.append(context.trial_index)

        return lambda state, batter_id, sequence: (
            out_event(
                state,
                batter_id,
                sequence,
            )
        )

    plan = CanonicalTrialExecutionPlan(
        factory_input=factory_input(
            simulations=2
        ),
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolver_factory=resolver_factory,
        game_config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
        ),
    )

    run_canonical_trial_execution_plan(plan)

    assert calls == [0, 1]


def test_non_callable_resolver_is_rejected():
    plan = CanonicalTrialExecutionPlan(
        factory_input=factory_input(
            simulations=1
        ),
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolver_factory=lambda context: None,
        game_config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
        ),
    )

    with pytest.raises(
        TypeError,
        match="must return a plate-appearance resolver",
    ):
        run_canonical_trial_execution_plan(plan)


def test_plan_rejects_reversed_lineups():
    with pytest.raises(
        ValueError,
        match="away_lineup must use away team side",
    ):
        CanonicalTrialExecutionPlan(
            factory_input=factory_input(),
            away_lineup=lineup("home"),
            home_lineup=lineup("away"),
            resolver_factory=lambda context: out_event,
        )


def test_resolver_context_defaults_to_nine_regulation_innings():
    value = build_canonical_trial_resolver_context(
        factory_input=factory_input(),
        trial_index=0,
    )

    assert value.regulation_innings == 9


def test_resolver_context_accepts_custom_regulation_innings():
    value = build_canonical_trial_resolver_context(
        factory_input=factory_input(),
        trial_index=0,
        regulation_innings=7,
    )

    assert value.regulation_innings == 7


def test_resolver_context_rejects_invalid_regulation_innings():
    with pytest.raises(
        ValueError,
        match="must be positive",
    ):
        build_canonical_trial_resolver_context(
            factory_input=factory_input(),
            trial_index=0,
            regulation_innings=0,
        )
