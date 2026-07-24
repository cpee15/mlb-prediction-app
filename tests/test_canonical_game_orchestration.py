from dataclasses import replace

import pytest

from mlb_app.simulation.events import (
    Base,
    DeterministicPlayResolver,
    GameState,
    OutRecord,
    PlayEvent,
    RunnerMovement,
    build_baserunning_event,
)
from mlb_app.simulation.game import (
    CanonicalGameConfig,
    CanonicalLineup,
    GameCompletionReason,
    simulate_canonical_game,
    validate_canonical_game,
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
                reason="test_out",
            ),
        ),
    )


def home_run_event(state, batter_id, sequence):
    scorers = [
        runner
        for runner in state.bases
        if runner is not None
    ] + [batter_id]

    movements = []

    for base, runner_id in zip(
        (Base.FIRST, Base.SECOND, Base.THIRD),
        state.bases,
    ):
        if runner_id is not None:
            movements.append(
                RunnerMovement(
                    runner_id=runner_id,
                    start_base=base,
                    end_base=Base.HOME,
                    scored=True,
                )
            )

    movements.append(
        RunnerMovement(
            runner_id=batter_id,
            start_base=Base.HOME,
            end_base=Base.HOME,
            scored=True,
        )
    )

    away_score = state.away_score
    home_score = state.home_score

    if state.half == "top":
        away_score += len(scorers)
    else:
        home_score += len(scorers)

    return PlayEvent(
        sequence=sequence,
        event_type="hr",
        batter_id=batter_id,
        state_before=state,
        state_after=replace(
            state,
            bases=(None, None, None),
            away_score=away_score,
            home_score=home_score,
            batting_order_index=(
                state.batting_order_index + 1
            ) % 9,
            plate_appearance_number=(
                state.plate_appearance_number + 1
            ),
        ),
        runner_movements=tuple(movements),
        runs_scored=tuple(scorers),
    )


def scripted_resolver(scoring_calls):
    calls = {"count": 0}

    def resolver(state, batter_id, sequence):
        call = calls["count"]
        calls["count"] += 1

        if call in scoring_calls:
            return home_run_event(
                state,
                batter_id,
                sequence,
            )

        return out_event(
            state,
            batter_id,
            sequence,
        )

    return resolver


def test_scoreless_regulation_reaches_extra_cap():
    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=out_event,
        config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=1,
            automatic_runner_enabled=False,
        ),
    )

    assert (
        result.completion_reason
        is GameCompletionReason.EXTRA_INNINGS_CAP_TIE
    )
    assert len(result.halves) == 4
    assert result.away_score == 0
    assert result.home_score == 0
    assert result.went_to_extras is True
    assert validate_canonical_game(result).passed is True


def test_bottom_half_is_skipped_when_home_already_leads():
    # Home scores in bottom first, then the away side is retired
    # in the top of the next inning.
    resolver = scripted_resolver({3})

    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=resolver,
        config=CanonicalGameConfig(
            regulation_innings=2,
            max_extra_innings=0,
            automatic_runner_enabled=False,
        ),
    )

    assert (
        result.completion_reason
        is GameCompletionReason.HOME_LEAD_AFTER_TOP
    )
    assert result.halves[-1].inning == 2
    assert result.halves[-1].half == "top"
    assert result.home_score == 1
    assert result.away_score == 0
    assert validate_canonical_game(result).passed is True


def test_bottom_regulation_walkoff_stops_immediately():
    # Three away outs, then home HR on first bottom PA.
    resolver = scripted_resolver({3})

    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=resolver,
        config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
            automatic_runner_enabled=False,
        ),
    )

    assert (
        result.completion_reason
        is GameCompletionReason.WALK_OFF
    )
    assert result.home_score == 1
    assert result.halves[-1].ended_by_walk_off is True
    assert result.halves[-1].final_state.outs == 0
    assert validate_canonical_game(result).passed is True


def test_away_regulation_win_completes_bottom_half():
    # Away HR on first call. All remaining PAs are outs.
    resolver = scripted_resolver({0})

    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=resolver,
        config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
            automatic_runner_enabled=False,
        ),
    )

    assert (
        result.completion_reason
        is GameCompletionReason.REGULATION
    )
    assert result.away_score == 1
    assert result.home_score == 0
    assert result.halves[-1].final_state.outs == 3
    assert validate_canonical_game(result).passed is True


def test_batting_orders_persist_between_innings():
    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=out_event,
        config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=1,
            automatic_runner_enabled=False,
        ),
    )

    away_halves = [
        half
        for half in result.halves
        if half.half == "top"
    ]
    home_halves = [
        half
        for half in result.halves
        if half.half == "bottom"
    ]

    assert away_halves[0].batting_order_start == 0
    assert away_halves[0].batting_order_end == 3
    assert away_halves[1].batting_order_start == 3
    assert home_halves[0].batting_order_end == 3
    assert home_halves[1].batting_order_start == 3


def test_event_sequences_are_global_across_halves():
    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=out_event,
        config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
            automatic_runner_enabled=False,
        ),
    )

    assert tuple(
        event.sequence
        for event in result.events
    ) == tuple(range(len(result.events)))


def test_automatic_runner_is_previous_batter():
    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=out_event,
        config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=1,
            automatic_runner_enabled=True,
        ),
    )

    extra_top = next(
        half
        for half in result.halves
        if half.inning == 2
        and half.half == "top"
    )

    assert (
        extra_top.automatic_runner_id
        == "away_2"
    )
    assert (
        extra_top.initial_state.second
        == "away_2"
    )


def test_resolver_wrong_sequence_is_rejected():
    def invalid(state, batter_id, sequence):
        return out_event(
            state,
            batter_id,
            sequence + 1,
        )

    with pytest.raises(
        ValueError,
        match="unexpected event sequence",
    ):
        simulate_canonical_game(
            away_lineup=lineup("away"),
            home_lineup=lineup("home"),
            resolve_plate_appearance=invalid,
            config=CanonicalGameConfig(
                regulation_innings=1,
                max_extra_innings=0,
            ),
        )


def test_runaway_half_is_safely_bounded():
    def never_out(state, batter_id, sequence):
        return home_run_event(
            state,
            batter_id,
            sequence,
        )

    with pytest.raises(
        RuntimeError,
        match="maximum plate appearances exceeded",
    ):
        simulate_canonical_game(
            away_lineup=lineup("away"),
            home_lineup=lineup("home"),
            resolve_plate_appearance=never_out,
            config=CanonicalGameConfig(
                regulation_innings=1,
                max_extra_innings=0,
                max_plate_appearances_per_half=5,
            ),
        )


def test_optional_baserunning_event_precedes_same_batter_pa():
    def plate_appearance(state, batter_id, sequence):
        if (
            state.outs == 0
            and state.bases == (None, None, None)
        ):
            return DeterministicPlayResolver().resolve(
                state=state,
                event_type="bb",
                batter_id=batter_id,
                sequence=sequence,
            )

        return out_event(
            state,
            batter_id,
            sequence,
        )

    def baserunning(state, batter_id, sequence):
        if (
            state.first is None
            or state.second is not None
        ):
            return None

        return build_baserunning_event(
            sequence=sequence,
            event_type="stolen_base",
            batter_id=batter_id,
            runner_id=state.first,
            state_before=state,
            origin_base=Base.FIRST,
            target_base=Base.SECOND,
        )

    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=plate_appearance,
        resolve_baserunning=baserunning,
        config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
            automatic_runner_enabled=False,
        ),
    )

    top = result.halves[0]

    assert tuple(
        event.event_type
        for event in top.events
    ) == (
        "bb",
        "stolen_base",
        "out",
        "out",
        "out",
    )
    assert top.events[1].batter_id == "away_1"
    assert top.events[2].batter_id == "away_1"
    assert (
        top.events[1].state_after
        == top.events[2].state_before
    )
    assert top.events[1].is_plate_appearance is False
    assert top.events[2].is_plate_appearance is True
    assert (
        top.final_state.plate_appearance_number
        - top.initial_state.plate_appearance_number
        == 4
    )
    assert tuple(
        event.sequence
        for event in result.events
    ) == tuple(range(len(result.events)))
    assert validate_canonical_game(result).passed is True


def test_baserunning_resolver_rejects_plate_appearance_event():
    def invalid_baserunning(
        state,
        batter_id,
        sequence,
    ):
        return out_event(
            state,
            batter_id,
            sequence,
        )

    with pytest.raises(
        ValueError,
        match=(
            "baserunning resolver must return "
            "non-plate-appearance event"
        ),
    ):
        simulate_canonical_game(
            away_lineup=lineup("away"),
            home_lineup=lineup("home"),
            resolve_plate_appearance=out_event,
            resolve_baserunning=invalid_baserunning,
            config=CanonicalGameConfig(
                regulation_innings=1,
                max_extra_innings=0,
                automatic_runner_enabled=False,
            ),
        )
