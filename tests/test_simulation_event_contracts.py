from dataclasses import FrozenInstanceError

import pytest

from mlb_app.simulation.events import (
    Base,
    DeterministicPlayResolver,
    GameState,
    PlayLedger,
    build_baserunning_event,
    replay_events,
)


def test_game_state_is_immutable():
    state = GameState()

    with pytest.raises(FrozenInstanceError):
        state.outs = 1


def test_game_state_rejects_duplicate_runner_occupancy():
    with pytest.raises(
        ValueError,
        match="runner cannot occupy multiple bases",
    ):
        GameState(bases=("runner-1", "runner-1", None))


def test_empty_base_walk_only_places_batter_on_first():
    resolver = DeterministicPlayResolver()
    state = GameState()

    event = resolver.resolve(
        state=state,
        event_type="bb",
        batter_id="batter",
        sequence=0,
    )

    assert event.state_before == state
    assert event.state_after.bases == ("batter", None, None)
    assert event.state_after.away_score == 0
    assert event.state_after.batting_order_index == 1
    assert event.state_after.plate_appearance_number == 1
    assert event.runs_scored == ()


def test_walk_does_not_move_unforced_second_or_third_base_runners():
    resolver = DeterministicPlayResolver()
    state = GameState(
        bases=(None, "runner-2", "runner-3"),
    )

    event = resolver.resolve(
        state=state,
        event_type="bb",
        batter_id="batter",
        sequence=0,
    )

    assert event.state_after.bases == (
        "batter",
        "runner-2",
        "runner-3",
    )
    assert event.runs_scored == ()


def test_bases_loaded_hbp_forces_exactly_one_run():
    resolver = DeterministicPlayResolver()
    state = GameState(
        bases=("runner-1", "runner-2", "runner-3"),
    )

    event = resolver.resolve(
        state=state,
        event_type="hbp",
        batter_id="batter",
        sequence=0,
    )

    assert event.state_after.bases == (
        "batter",
        "runner-1",
        "runner-2",
    )
    assert event.state_after.away_score == 1
    assert event.runs_scored == ("runner-3",)

    movements = {
        movement.runner_id: movement
        for movement in event.runner_movements
    }
    assert movements["runner-3"].scored is True
    assert movements["runner-2"].end_base is Base.THIRD
    assert movements["runner-1"].end_base is Base.SECOND
    assert movements["batter"].end_base is Base.FIRST


def test_bottom_half_grand_slam_updates_home_score_and_clears_bases():
    resolver = DeterministicPlayResolver()
    state = GameState(
        inning=9,
        half="bottom",
        outs=2,
        bases=("runner-1", "runner-2", "runner-3"),
        away_score=4,
        home_score=2,
        batting_order_index=8,
        plate_appearance_number=35,
    )

    event = resolver.resolve(
        state=state,
        event_type="hr",
        batter_id="batter",
        sequence=0,
    )

    assert event.state_after.bases == (None, None, None)
    assert event.state_after.away_score == 4
    assert event.state_after.home_score == 6
    assert event.state_after.outs == 2
    assert event.state_after.batting_order_index == 0
    assert event.state_after.plate_appearance_number == 36
    assert event.runs_scored == (
        "runner-1",
        "runner-2",
        "runner-3",
        "batter",
    )


def test_ledger_is_append_only_and_validates_sequence():
    resolver = DeterministicPlayResolver()
    initial_state = GameState()
    ledger = PlayLedger(initial_state=initial_state)

    first_event = resolver.resolve(
        state=initial_state,
        event_type="bb",
        batter_id="batter-1",
        sequence=0,
    )
    next_ledger = ledger.append(first_event)

    assert ledger.events == ()
    assert len(next_ledger.events) == 1

    invalid_event = resolver.resolve(
        state=next_ledger.current_state,
        event_type="hr",
        batter_id="batter-2",
        sequence=3,
    )

    with pytest.raises(ValueError, match="expected event sequence 1"):
        next_ledger.append(invalid_event)


def test_ledger_rejects_state_discontinuity():
    resolver = DeterministicPlayResolver()
    initial_state = GameState()
    ledger = PlayLedger(initial_state=initial_state)

    unrelated_state = GameState(bases=("other-runner", None, None))
    event = resolver.resolve(
        state=unrelated_state,
        event_type="hr",
        batter_id="batter",
        sequence=0,
    )

    with pytest.raises(ValueError, match="state_before"):
        ledger.append(event)


def test_replay_matches_live_final_state():
    resolver = DeterministicPlayResolver()
    initial_state = GameState()

    event_1 = resolver.resolve(
        state=initial_state,
        event_type="bb",
        batter_id="batter-1",
        sequence=0,
    )
    event_2 = resolver.resolve(
        state=event_1.state_after,
        event_type="hbp",
        batter_id="batter-2",
        sequence=1,
    )
    event_3 = resolver.resolve(
        state=event_2.state_after,
        event_type="hr",
        batter_id="batter-3",
        sequence=2,
    )

    events = (event_1, event_2, event_3)

    assert replay_events(initial_state, events) == event_3.state_after
    assert event_3.state_after.away_score == 3
    assert event_3.state_after.bases == (None, None, None)


def test_resolver_rejects_unsupported_probabilistic_event():
    resolver = DeterministicPlayResolver()

    with pytest.raises(
        ValueError,
        match="unsupported deterministic event_type",
    ):
        resolver.resolve(
            state=GameState(),
            event_type="single",
            batter_id="batter",
            sequence=0,
        )


def test_resolver_rejects_plate_appearance_after_three_outs():
    resolver = DeterministicPlayResolver()

    with pytest.raises(
        ValueError,
        match="cannot resolve a plate appearance with 3 outs",
    ):
        resolver.resolve(
            state=GameState(outs=3),
            event_type="bb",
            batter_id="batter",
            sequence=0,
        )


def test_successful_steal_is_a_non_plate_appearance_event():
    state = GameState(
        outs=1,
        bases=("runner-1", None, "runner-3"),
        batting_order_index=4,
        plate_appearance_number=13,
    )

    event = build_baserunning_event(
        sequence=0,
        event_type="stolen_base",
        batter_id="batter",
        runner_id="runner-1",
        state_before=state,
        origin_base=Base.FIRST,
        target_base=Base.SECOND,
        pitcher_id="pitcher",
    )

    assert event.is_plate_appearance is False
    assert event.state_after.bases == (
        None,
        "runner-1",
        "runner-3",
    )
    assert event.state_after.outs == 1
    assert event.state_after.batting_order_index == 4
    assert event.state_after.plate_appearance_number == 13
    assert event.outs_recorded == ()
    assert event.runs_scored == ()


def test_caught_stealing_records_out_without_advancing_batter():
    state = GameState(
        outs=1,
        bases=("runner-1", "runner-2", None),
        batting_order_index=7,
        plate_appearance_number=26,
    )

    event = build_baserunning_event(
        sequence=0,
        event_type="caught_stealing",
        batter_id="batter",
        runner_id="runner-2",
        state_before=state,
        origin_base=Base.SECOND,
        target_base=Base.THIRD,
    )

    assert event.is_plate_appearance is False
    assert event.state_after.bases == (
        "runner-1",
        None,
        None,
    )
    assert event.state_after.outs == 2
    assert event.state_after.batting_order_index == 7
    assert event.state_after.plate_appearance_number == 26
    assert len(event.outs_recorded) == 1
    assert event.outs_recorded[0].runner_id == "runner-2"
    assert event.outs_recorded[0].reason == "caught_stealing"


def test_baserunning_event_composes_with_next_plate_appearance():
    resolver = DeterministicPlayResolver()
    initial_state = GameState(
        bases=("runner-1", None, None),
    )
    ledger = PlayLedger(initial_state=initial_state)

    steal = build_baserunning_event(
        sequence=0,
        event_type="stolen_base",
        batter_id="batter",
        runner_id="runner-1",
        state_before=initial_state,
        origin_base=Base.FIRST,
        target_base=Base.SECOND,
    )
    ledger = ledger.append(steal)

    plate_appearance = resolver.resolve(
        state=ledger.current_state,
        event_type="bb",
        batter_id="batter",
        sequence=1,
    )
    ledger = ledger.append(plate_appearance)

    assert len(ledger.events) == 2
    assert ledger.events[0].is_plate_appearance is False
    assert ledger.events[1].is_plate_appearance is True
    assert ledger.current_state.bases == (
        "batter",
        "runner-1",
        None,
    )
    assert ledger.current_state.batting_order_index == 1
    assert ledger.current_state.plate_appearance_number == 1


def test_baserunning_event_rejects_occupied_target_base():
    with pytest.raises(
        ValueError,
        match="target base must be unoccupied",
    ):
        build_baserunning_event(
            sequence=0,
            event_type="stolen_base",
            batter_id="batter",
            runner_id="runner-1",
            state_before=GameState(
                bases=("runner-1", "runner-2", None),
            ),
            origin_base=Base.FIRST,
            target_base=Base.SECOND,
        )
