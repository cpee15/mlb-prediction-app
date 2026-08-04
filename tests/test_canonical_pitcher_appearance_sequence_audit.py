from dataclasses import dataclass

import pytest

from mlb_app.simulation.game.matchup_input import (
    CanonicalPitchingPlan,
)
from mlb_app.simulation.shadow.canonical_pitcher_appearance_sequence_audit import (
    audit_canonical_pitcher_appearance_sequence,
)


@dataclass(frozen=True)
class State:
    inning: int
    half: str
    outs: int = 0


@dataclass(frozen=True)
class Event:
    sequence: int
    pitcher_id: str
    state_before: State
    state_after: State
    outs_recorded: tuple = ()
    is_plate_appearance: bool = True


@dataclass(frozen=True)
class Game:
    events: tuple


def event(
    sequence,
    pitcher_id,
    *,
    inning,
    half,
    outs_before=0,
    outs_recorded=0,
    is_plate_appearance=True,
):
    return Event(
        sequence=sequence,
        pitcher_id=pitcher_id,
        state_before=State(
            inning=inning,
            half=half,
            outs=outs_before,
        ),
        state_after=State(
            inning=inning,
            half=half,
            outs=(
                outs_before + outs_recorded
            ),
        ),
        outs_recorded=tuple(
            object()
            for _ in range(outs_recorded)
        ),
        is_plate_appearance=(
            is_plate_appearance
        ),
    )


def traditional_plan(team_side):
    return CanonicalPitchingPlan(
        team_side=team_side,
        starter_id=f"{team_side}_starter",
        bullpen_pitcher_ids=(
            f"{team_side}_reliever",
        ),
    )


def opener_plan(team_side):
    return CanonicalPitchingPlan(
        team_side=team_side,
        starter_id=f"{team_side}_opener",
        bullpen_pitcher_ids=(
            f"{team_side}_bulk",
            f"{team_side}_reliever",
        ),
        plan_type="opener_bulk",
        preferred_replacement_pitcher_ids=(
            f"{team_side}_bulk",
        ),
    )


def normal_game():
    return Game(events=(
        event(
            0,
            "home_starter",
            inning=1,
            half="top",
            outs_recorded=1,
        ),
        event(
            1,
            "away_opener",
            inning=1,
            half="bottom",
            outs_recorded=1,
        ),
        event(
            2,
            "home_reliever",
            inning=6,
            half="top",
            outs_recorded=1,
        ),
        event(
            3,
            "away_bulk",
            inning=2,
            half="bottom",
            outs_recorded=1,
        ),
    ))


def audit(game=None):
    return (
        audit_canonical_pitcher_appearance_sequence(
            games=(game or normal_game(),),
            away_pitching_plan=opener_plan(
                "away"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )
    )


def record(result, pitcher_id):
    return next(
        value
        for value in result["records"]
        if value["pitcher_id"] == pitcher_id
    )


def test_observes_actual_pitcher_order():
    result = audit()

    assert result["status"] == "observed"
    assert result["trial_count"] == 1
    assert result["appearance_count"] == 4
    assert result["trials"][0][
        "away_pitcher_ids"
    ] == [
        "away_opener",
        "away_bulk",
    ]
    assert result["trials"][0][
        "home_pitcher_ids"
    ] == [
        "home_starter",
        "home_reliever",
    ]


def test_attributes_planned_roles():
    result = audit()

    assert record(
        result,
        "away_opener",
    )["planned_role"] == "opener"
    assert record(
        result,
        "away_bulk",
    )["planned_role"] == "bulk_follower"
    assert record(
        result,
        "home_starter",
    )["planned_role"] == "starter"
    assert record(
        result,
        "home_reliever",
    )["actual_role"] == "reliever"


def test_records_entry_and_workload():
    game = Game(events=(
        event(
            0,
            "home_starter",
            inning=1,
            half="top",
            outs_recorded=1,
        ),
        event(
            1,
            "home_starter",
            inning=1,
            half="top",
            outs_before=1,
            outs_recorded=2,
        ),
        event(
            2,
            "away_opener",
            inning=1,
            half="bottom",
            outs_recorded=1,
        ),
    ))

    result = audit(game)
    starter = record(
        result,
        "home_starter",
    )

    assert starter["entered_inning"] == 1
    assert starter["entered_outs"] == 0
    assert starter["batters_faced"] == 2
    assert starter["outs_recorded"] == 3
    assert starter["innings_equivalent"] == 1.0
    assert starter["entry_sequence"] == 0
    assert starter["exit_sequence"] == 1


def test_detects_starter_used_after_first_pitcher():
    game = Game(events=(
        event(
            0,
            "home_reliever",
            inning=1,
            half="top",
        ),
        event(
            1,
            "home_starter",
            inning=4,
            half="top",
        ),
        event(
            2,
            "away_opener",
            inning=1,
            half="bottom",
        ),
    ))

    result = audit(game)

    assert result[
        "starter_relief_detected"
    ] is True
    assert result[
        "starter_relief_appearance_count"
    ] == 1
    assert result["anomaly_counts"][
        "planned_starter_not_first"
    ] == 1
    assert result["anomaly_counts"][
        "planned_starter_used_in_relief"
    ] == 1


def test_detects_reentry_and_pitcher_outside_plan():
    game = Game(events=(
        event(
            0,
            "home_starter",
            inning=1,
            half="top",
        ),
        event(
            1,
            "home_unplanned",
            inning=5,
            half="top",
        ),
        event(
            2,
            "home_starter",
            inning=8,
            half="top",
        ),
        event(
            3,
            "away_opener",
            inning=1,
            half="bottom",
        ),
    ))

    result = audit(game)

    assert result["anomaly_counts"][
        "pitcher_outside_plan"
    ] == 1
    assert result["anomaly_counts"][
        "pitcher_reentry"
    ] == 1
    assert result["anomaly_counts"][
        "planned_starter_used_in_relief"
    ] == 1


def test_detects_skipped_bulk_follower():
    game = Game(events=(
        event(
            0,
            "home_starter",
            inning=1,
            half="top",
        ),
        event(
            1,
            "away_opener",
            inning=1,
            half="bottom",
        ),
        event(
            2,
            "away_reliever",
            inning=2,
            half="bottom",
        ),
    ))

    result = audit(game)

    assert result["anomaly_counts"][
        "away:preferred_follower_skipped"
    ] == 1


def test_ignores_non_plate_appearance_events():
    game = Game(events=(
        event(
            0,
            "home_starter",
            inning=1,
            half="top",
        ),
        event(
            1,
            "home_unplanned",
            inning=1,
            half="top",
            is_plate_appearance=False,
        ),
        event(
            2,
            "away_opener",
            inning=1,
            half="bottom",
        ),
    ))

    result = audit(game)

    assert result["appearance_count"] == 2
    assert (
        "pitcher_outside_plan"
        not in result["anomaly_counts"]
    )


def test_summarizes_role_workloads():
    result = audit()

    starter = result["role_summaries"][
        "starter"
    ]

    assert starter["appearance_count"] == 1
    assert starter[
        "team_trial_appearance_count"
    ] == 1
    assert starter["appearance_rate"] == 0.5
    assert starter["outs_recorded"]["mean"] == 1.0
    assert starter[
        "innings_equivalent"
    ]["mean"] == (1.0 / 3.0)


def test_is_read_only_and_non_authoritative():
    result = audit()

    assert (
        result["database_writes_performed"]
        is False
    )
    assert (
        result["production_authority_changed"]
        is False
    )
    assert result["decision"][
        "pitcher_sequence_activation_allowed"
    ] is False
    assert result["decision"][
        "production_activation_allowed"
    ] is False


def test_validates_required_inputs():
    with pytest.raises(
        ValueError,
        match="at least one",
    ):
        audit_canonical_pitcher_appearance_sequence(
            games=(),
            away_pitching_plan=opener_plan(
                "away"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )

    with pytest.raises(
        ValueError,
        match="team_side",
    ):
        audit_canonical_pitcher_appearance_sequence(
            games=(normal_game(),),
            away_pitching_plan=traditional_plan(
                "home"
            ),
            home_pitching_plan=traditional_plan(
                "home"
            ),
        )


def test_reentry_does_not_inflate_role_appearance_rate():
    game = Game(events=(
        event(
            0,
            "home_starter",
            inning=1,
            half="top",
        ),
        event(
            1,
            "home_reliever",
            inning=5,
            half="top",
        ),
        event(
            2,
            "home_starter",
            inning=8,
            half="top",
        ),
        event(
            3,
            "away_opener",
            inning=1,
            half="bottom",
        ),
    ))

    result = audit(game)
    starter = result["role_summaries"][
        "starter"
    ]

    assert starter["appearance_count"] == 2
    assert starter[
        "team_trial_appearance_count"
    ] == 1
    assert starter["appearance_rate"] == 0.5
