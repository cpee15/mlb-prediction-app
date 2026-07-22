from dataclasses import replace

import pytest

from mlb_app.simulation.events import (
    Base,
    GameState,
    RunnerMovement,
    build_play_event,
)
from mlb_app.simulation.game import (
    CANONICAL_PITCHER_LIFECYCLE_VERSION,
    CanonicalPitcherLifecycleState,
    CanonicalPitcherRole,
    CanonicalPitchingDecision,
    CanonicalPitchingDecisionAction,
    CanonicalPitchingDecisionContext,
    reduce_pitcher_lifecycle,
    retire_pitcher,
)


def lifecycle():
    return CanonicalPitcherLifecycleState(
        team_side="home",
        pitcher_id="home_starter",
        role=CanonicalPitcherRole.STARTER,
        entered_inning=1,
        entered_half="top",
    )


def event(
    event_type,
    *,
    pitcher_id="home_starter",
    outs=0,
    runs=(),
):
    state = GameState(
        inning=1,
        half="top",
        outs=0,
    )

    movements = []

    for runner_id in runs:
        movements.append(
            RunnerMovement(
                runner_id=runner_id,
                start_base=Base.THIRD,
                end_base=Base.HOME,
                scored=True,
            )
        )

    if event_type in {
        "single",
        "double",
        "triple",
        "hr",
        "bb",
        "hbp",
    }:
        destination = {
            "single": Base.FIRST,
            "double": Base.SECOND,
            "triple": Base.THIRD,
            "hr": Base.HOME,
            "bb": Base.FIRST,
            "hbp": Base.FIRST,
        }[event_type]

        movements.append(
            RunnerMovement(
                runner_id="batter",
                start_base=Base.HOME,
                end_base=destination,
                scored=(
                    destination is Base.HOME
                ),
            )
        )

    resolved = build_play_event(
        sequence=0,
        event_type=event_type,
        batter_id="batter",
        state_before=state,
        runner_movements=tuple(movements),
        outs_recorded=(),
    )

    if outs:
        resolved = replace(
            resolved,
            state_after=replace(
                resolved.state_after,
                outs=outs,
            ),
        )

    return replace(
        resolved,
        pitcher_id=pitcher_id,
    )


def test_lifecycle_starts_with_stable_contract():
    value = lifecycle()

    assert value.schema_version == (
        CANONICAL_PITCHER_LIFECYCLE_VERSION
    )
    assert value.batters_faced == 0
    assert value.outs_recorded == 0
    assert value.innings_recorded == 0.0
    assert value.current_lineup_pass == 1
    assert value.active is True


@pytest.mark.parametrize(
    (
        "event_type",
        "hits",
        "walks",
        "hit_batters",
        "home_runs",
        "strikeouts",
    ),
    [
        ("single", 1, 0, 0, 0, 0),
        ("double", 1, 0, 0, 0, 0),
        ("triple", 1, 0, 0, 0, 0),
        ("hr", 1, 0, 0, 1, 0),
        ("bb", 0, 1, 0, 0, 0),
        ("hbp", 0, 0, 1, 0, 0),
        ("k", 0, 0, 0, 0, 1),
        ("out", 0, 0, 0, 0, 0),
    ],
)
def test_reducer_tracks_terminal_pa_outcomes(
    event_type,
    hits,
    walks,
    hit_batters,
    home_runs,
    strikeouts,
):
    updated = reduce_pitcher_lifecycle(
        lifecycle(),
        event(event_type),
    )

    assert updated.batters_faced == 1
    assert updated.hits_allowed == hits
    assert updated.walks_allowed == walks
    assert updated.hit_batters == hit_batters
    assert updated.home_runs_allowed == home_runs
    assert updated.strikeouts == strikeouts


def test_reducer_tracks_runs_during_stint_without_responsibility():
    updated = reduce_pitcher_lifecycle(
        lifecycle(),
        event(
            "single",
            runs=("inherited_runner",),
        ),
    )

    assert updated.runs_scored_during_stint == 1


def test_reducer_rejects_different_pitcher():
    with pytest.raises(
        ValueError,
        match="does not match",
    ):
        reduce_pitcher_lifecycle(
            lifecycle(),
            event(
                "single",
                pitcher_id="home_reliever",
            ),
        )


def test_retired_pitcher_cannot_receive_events():
    retired = retire_pitcher(lifecycle())

    assert retired.active is False

    with pytest.raises(
        ValueError,
        match="inactive pitcher",
    ):
        reduce_pitcher_lifecycle(
            retired,
            event("single"),
        )


def test_hold_decision_rejects_replacement():
    with pytest.raises(
        ValueError,
        match="cannot name a replacement",
    ):
        CanonicalPitchingDecision(
            action=(
                CanonicalPitchingDecisionAction.HOLD
            ),
            current_pitcher_id="home_starter",
            replacement_pitcher_id="home_reliever",
        )


def test_replace_decision_requires_new_pitcher():
    decision = CanonicalPitchingDecision(
        action=(
            CanonicalPitchingDecisionAction.REPLACE
        ),
        current_pitcher_id="home_starter",
        replacement_pitcher_id="home_reliever",
        reason="starter_hook",
    )

    assert decision.replacement_pitcher_id == (
        "home_reliever"
    )


def test_decision_context_preserves_available_bullpen():
    context = CanonicalPitchingDecisionContext(
        lifecycle=lifecycle(),
        inning=6,
        half="top",
        outs=1,
        batting_team_score=3,
        fielding_team_score=2,
        runners_on_base=2,
        upcoming_batter_id="away_batter_4",
        available_reliever_ids=(
            "home_reliever_1",
            "home_reliever_2",
        ),
    )

    assert context.lifecycle.pitcher_id == (
        "home_starter"
    )
    assert context.runners_on_base == 2
    assert context.available_reliever_ids == (
        "home_reliever_1",
        "home_reliever_2",
    )
