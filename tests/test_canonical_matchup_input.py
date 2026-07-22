from dataclasses import replace

import pytest

from mlb_app.simulation.events import (
    OutRecord,
    PlayEvent,
)
from mlb_app.simulation.game import (
    CANONICAL_MATCHUP_INPUT_VERSION,
    CanonicalGameConfig,
    CanonicalLineup,
    CanonicalMatchupInput,
    CanonicalPitchingPlan,
    CanonicalProbabilityProviderIdentity,
    CanonicalTrialExecutionPlan,
    build_canonical_trial_factory_input,
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
            f"{side}_reliever_1",
            f"{side}_reliever_2",
        ),
    )


def matchup(game_pk=123):
    return CanonicalMatchupInput(
        game_pk=game_pk,
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        away_pitching_plan=(
            pitching_plan("away")
        ),
        home_pitching_plan=(
            pitching_plan("home")
        ),
        probability_provider=(
            CanonicalProbabilityProviderIdentity(
                provider_name="test-provider",
                provider_version="v1",
                artifact_id="artifact-123",
            )
        ),
    )


def out_event(state, batter_id, sequence):
    next_out = state.outs + 1

    return PlayEvent(
        sequence=sequence,
        event_type="out",
        batter_id=batter_id,
        pitcher_id=(
            "home_starter"
            if state.half == "top"
            else "away_starter"
        ),
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
                reason="matchup_contract_out",
            ),
        ),
    )


def test_matchup_contract_records_fixed_identity():
    value = matchup()

    assert value.schema_version == (
        CANONICAL_MATCHUP_INPUT_VERSION
    )
    assert (
        value.away_pitching_plan.starter_id
        == "away_starter"
    )
    assert value.home_pitching_plan.available_pitcher_ids == (
        "home_starter",
        "home_reliever_1",
        "home_reliever_2",
    )
    assert (
        value.probability_provider.identity
        == "test-provider:v1:artifact-123"
    )


def test_starter_cannot_appear_in_bullpen():
    with pytest.raises(
        ValueError,
        match="starter cannot also appear",
    ):
        CanonicalPitchingPlan(
            team_side="away",
            starter_id="pitcher_1",
            bullpen_pitcher_ids=(
                "pitcher_1",
            ),
        )


def test_matchup_rejects_shared_lineup_players():
    away = lineup("away")

    with pytest.raises(
        ValueError,
        match="cannot share players",
    ):
        CanonicalMatchupInput(
            game_pk=123,
            away_lineup=away,
            home_lineup=CanonicalLineup(
                team_side="home",
                player_ids=away.player_ids,
            ),
            away_pitching_plan=(
                pitching_plan("away")
            ),
            home_pitching_plan=(
                pitching_plan("home")
            ),
            probability_provider=(
                CanonicalProbabilityProviderIdentity(
                    provider_name="provider",
                    provider_version="v1",
                )
            ),
        )


def test_matchup_is_available_to_each_resolver():
    matchup_input = matchup()
    factory_input = (
        build_canonical_trial_factory_input(
            game_pk=123,
            config={
                "simulation_count": 2,
                "seed": 999,
                "canonical_model_version": (
                    "matchup-contract-test-v1"
                ),
            },
        )
    )
    observed = []

    def resolver_factory(context):
        observed.append(
            (
                context.trial_index,
                context.matchup_input,
            )
        )

        return out_event

    plan = CanonicalTrialExecutionPlan(
        factory_input=factory_input,
        away_lineup=matchup_input.away_lineup,
        home_lineup=matchup_input.home_lineup,
        resolver_factory=resolver_factory,
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
    assert observed == [
        (0, matchup_input),
        (1, matchup_input),
    ]


def test_execution_plan_rejects_mismatched_game():
    matchup_input = matchup(game_pk=124)
    factory_input = (
        build_canonical_trial_factory_input(
            game_pk=123,
            config={
                "simulation_count": 1,
            },
        )
    )

    with pytest.raises(
        ValueError,
        match="matchup game_pk must match",
    ):
        CanonicalTrialExecutionPlan(
            factory_input=factory_input,
            away_lineup=matchup_input.away_lineup,
            home_lineup=matchup_input.home_lineup,
            resolver_factory=lambda context: out_event,
            matchup_input=matchup_input,
        )


def test_execution_plan_rejects_mismatched_lineup():
    matchup_input = matchup()
    factory_input = (
        build_canonical_trial_factory_input(
            game_pk=123,
            config={
                "simulation_count": 1,
            },
        )
    )

    different_away = CanonicalLineup(
        team_side="away",
        player_ids=tuple(
            f"different_away_{index}"
            for index in range(9)
        ),
    )

    with pytest.raises(
        ValueError,
        match="matchup away lineup must match",
    ):
        CanonicalTrialExecutionPlan(
            factory_input=factory_input,
            away_lineup=different_away,
            home_lineup=matchup_input.home_lineup,
            resolver_factory=lambda context: out_event,
            matchup_input=matchup_input,
        )

def test_pitching_plan_accepts_opener_bulk_sequence():
    value = CanonicalPitchingPlan(
        team_side="home",
        starter_id="opener",
        bullpen_pitcher_ids=(
            "bulk",
            "middle",
        ),
        plan_type="opener_bulk",
        preferred_replacement_pitcher_ids=(
            "bulk",
        ),
    )

    assert value.plan_type == "opener_bulk"
    assert (
        value.preferred_replacement_pitcher_ids
        == ("bulk",)
    )


def test_pitching_plan_rejects_preferred_pitcher_outside_bullpen():
    with pytest.raises(
        ValueError,
        match="must belong to bullpen",
    ):
        CanonicalPitchingPlan(
            team_side="home",
            starter_id="opener",
            bullpen_pitcher_ids=("middle",),
            plan_type="opener_bulk",
            preferred_replacement_pitcher_ids=(
                "bulk",
            ),
        )
