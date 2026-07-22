from dataclasses import replace

import pytest

from mlb_app.simulation.events import (
    Base,
    GameState,
    OutRecord,
    RunnerMovement,
    build_play_event,
)
from mlb_app.simulation.game import (
    CANONICAL_PITCHER_RESPONSIBILITY_VERSION,
    CanonicalPitcherResponsibilityLedger,
)


def event(
    *,
    sequence,
    pitcher_id,
    event_type,
    movements,
):
    return replace(
        build_play_event(
            sequence=sequence,
            event_type=event_type,
            batter_id=f"batter_{sequence}",
            state_before=GameState(),
            runner_movements=movements,
            outs_recorded=(),
        ),
        pitcher_id=pitcher_id,
    )


def test_batter_runner_is_assigned_to_event_pitcher():
    ledger = CanonicalPitcherResponsibilityLedger()

    ledger.apply_event(
        event(
            sequence=0,
            pitcher_id="starter",
            event_type="single",
            movements=(
                RunnerMovement(
                    runner_id="runner_a",
                    start_base=0,
                    end_base=1,
                ),
            ),
        )
    )

    responsibility = (
        ledger.responsibility_for_runner(
            "runner_a"
        )
    )

    assert responsibility is not None
    assert responsibility.responsible_pitcher_id == (
        "starter"
    )
    assert responsibility.schema_version == (
        CANONICAL_PITCHER_RESPONSIBILITY_VERSION
    )


def test_existing_runner_keeps_original_pitcher():
    ledger = CanonicalPitcherResponsibilityLedger()

    ledger.apply_event(
        event(
            sequence=0,
            pitcher_id="starter",
            event_type="walk",
            movements=(
                RunnerMovement(
                    runner_id="runner_a",
                    start_base=0,
                    end_base=1,
                ),
            ),
        )
    )

    ledger.apply_event(
        event(
            sequence=1,
            pitcher_id="reliever",
            event_type="single",
            movements=(
                RunnerMovement(
                    runner_id="runner_a",
                    start_base=1,
                    end_base=2,
                ),
                RunnerMovement(
                    runner_id="runner_b",
                    start_base=0,
                    end_base=1,
                ),
            ),
        )
    )

    assert (
        ledger
        .responsibility_for_runner("runner_a")
        .responsible_pitcher_id
        == "starter"
    )
    assert (
        ledger
        .responsibility_for_runner("runner_b")
        .responsible_pitcher_id
        == "reliever"
    )


def test_scored_run_records_both_pitchers():
    ledger = CanonicalPitcherResponsibilityLedger()

    ledger.apply_event(
        event(
            sequence=0,
            pitcher_id="starter",
            event_type="double",
            movements=(
                RunnerMovement(
                    runner_id="runner_a",
                    start_base=0,
                    end_base=2,
                ),
            ),
        )
    )

    scored = ledger.apply_event(
        event(
            sequence=1,
            pitcher_id="reliever",
            event_type="single",
            movements=(
                RunnerMovement(
                    runner_id="runner_a",
                    start_base=2,
                    end_base=Base.HOME,
                    scored=True,
                ),
                RunnerMovement(
                    runner_id="runner_b",
                    start_base=0,
                    end_base=1,
                ),
            ),
        )
    )

    assert len(scored) == 1
    assert scored[0].runner_id == "runner_a"
    assert scored[0].responsible_pitcher_id == (
        "starter"
    )
    assert scored[0].pitcher_on_mound_id == (
        "reliever"
    )
    assert (
        ledger.responsibility_for_runner(
            "runner_a"
        )
        is None
    )


def test_retired_runner_is_removed():
    ledger = CanonicalPitcherResponsibilityLedger()

    ledger.apply_event(
        event(
            sequence=0,
            pitcher_id="starter",
            event_type="single",
            movements=(
                RunnerMovement(
                    runner_id="runner_a",
                    start_base=0,
                    end_base=1,
                ),
            ),
        )
    )

    retired_event = replace(
        build_play_event(
            sequence=1,
            event_type="fielder_choice",
            batter_id="batter_1",
            state_before=GameState(),
            runner_movements=(
                RunnerMovement(
                    runner_id="runner_a",
                    start_base=1,
                    end_base=None,
                    is_out=True,
                ),
            ),
            outs_recorded=(
                OutRecord(
                    runner_id="runner_a",
                    out_number=1,
                    reason="force_out",
                ),
            ),
        ),
        pitcher_id="starter",
    )

    ledger.apply_event(retired_event)

    assert (
        ledger.responsibility_for_runner(
            "runner_a"
        )
        is None
    )


def test_scoring_runner_without_responsibility_fails():
    ledger = CanonicalPitcherResponsibilityLedger()

    scoring_event = replace(
        build_play_event(
            sequence=0,
            event_type="single",
            batter_id="batter_0",
            state_before=GameState(),
            runner_movements=(
                RunnerMovement(
                    runner_id="unknown_runner",
                    start_base=2,
                    end_base=Base.HOME,
                    scored=True,
                ),
            ),
            outs_recorded=(),
        ),
        pitcher_id="reliever",
    )

    with pytest.raises(
        ValueError,
        match="no active pitcher responsibility",
    ):
        ledger.apply_event(scoring_event)


def test_duplicate_batter_runner_assignment_fails():
    ledger = CanonicalPitcherResponsibilityLedger()

    first = event(
        sequence=0,
        pitcher_id="starter",
        event_type="walk",
        movements=(
            RunnerMovement(
                runner_id="runner_a",
                start_base=0,
                end_base=1,
            ),
        ),
    )

    ledger.apply_event(first)

    with pytest.raises(
        ValueError,
        match="already has active",
    ):
        ledger.apply_event(first)
