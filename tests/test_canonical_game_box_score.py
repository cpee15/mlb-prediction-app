from dataclasses import replace

from mlb_app.simulation.events import (
    Base,
    GameState,
    OutRecord,
    PlayEvent,
    RunnerMovement,
)
from mlb_app.simulation.game import (
    CanonicalGameConfig,
    CanonicalLineup,
    simulate_canonical_game,
)
from mlb_app.simulation.game.box_score import (
    reduce_canonical_game_box_score,
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


def home_run_event(
    state,
    batter_id,
    sequence,
    pitcher_id=None,
):
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
        pitcher_id=pitcher_id,
    )


def scripted_resolver(scoring_calls):
    calls = {"count": 0}

    def resolver(state, batter_id, sequence):
        call = calls["count"]
        calls["count"] += 1

        if call in scoring_calls:
            fielding_side = (
                "home"
                if state.half == "top"
                else "away"
            )
            return home_run_event(
                state,
                batter_id,
                sequence,
                pitcher_id=(
                    f"{fielding_side}_pitcher"
                ),
            )

        fielding_side = (
            "home"
            if state.half == "top"
            else "away"
        )
        event = out_event(
            state,
            batter_id,
            sequence,
        )
        return replace(
            event,
            pitcher_id=(
                f"{fielding_side}_pitcher"
            ),
        )

    return resolver


def test_full_game_box_score_reconciles_with_final_score():
    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=(
            scripted_resolver({0})
        ),
        config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
            automatic_runner_enabled=False,
        ),
    )

    reduced = reduce_canonical_game_box_score(
        result
    )

    assert reduced.box_score.away.runs == 1
    assert reduced.box_score.home.runs == 0
    assert reduced.box_score.away.hits == 1
    assert reduced.reconciliation.passed is True


def test_batter_rows_merge_across_half_innings():
    # Away batting-order progression:
    # inning 1: away_0 through away_3
    # inning 2: away_4 through away_6
    # inning 3: away_7, away_8, away_0
    #
    # Calls 0 and 15 therefore belong to away_0 in different
    # half-inning segments.
    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=(
            scripted_resolver({0, 15})
        ),
        config=CanonicalGameConfig(
            regulation_innings=3,
            max_extra_innings=0,
            automatic_runner_enabled=False,
        ),
    )

    reduced = reduce_canonical_game_box_score(
        result
    )

    away_zero = reduced.box_score.batter(
        "away_0"
    )

    assert away_zero.plate_appearances == 2
    assert away_zero.home_runs == 2
    assert away_zero.runs == 2


def test_pitcher_runs_match_opponent_when_complete():
    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=(
            scripted_resolver({0})
        ),
        config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=0,
            automatic_runner_enabled=False,
        ),
    )

    reduced = reduce_canonical_game_box_score(
        result
    )

    home_pitcher = reduced.box_score.pitcher(
        "home_pitcher"
    )

    assert home_pitcher.runs_allowed == 1
    assert (
        reduced.box_score
        .pitcher_attribution_complete
        is True
    )
    assert (
        reduced.reconciliation
        .pitcher_runs_match_when_complete
        is True
    )


def test_automatic_runner_run_is_credited_to_batter():
    calls = {
        "count": 0,
        "extra_run_scored": False,
    }

    def resolver(state, batter_id, sequence):
        call = calls["count"]
        calls["count"] += 1

        # Score the automatic runner exactly once in the top of
        # the extra inning, then allow the inning to terminate.
        if (
            state.inning == 2
            and state.half == "top"
            and state.outs == 0
            and not calls["extra_run_scored"]
        ):
            calls["extra_run_scored"] = True
            return home_run_event(
                state,
                batter_id,
                sequence,
                pitcher_id="home_pitcher",
            )

        fielding_side = (
            "home"
            if state.half == "top"
            else "away"
        )
        event = out_event(
            state,
            batter_id,
            sequence,
        )
        return replace(
            event,
            pitcher_id=(
                f"{fielding_side}_pitcher"
            ),
        )

    result = simulate_canonical_game(
        away_lineup=lineup("away"),
        home_lineup=lineup("home"),
        resolve_plate_appearance=resolver,
        config=CanonicalGameConfig(
            regulation_innings=1,
            max_extra_innings=1,
            automatic_runner_enabled=True,
        ),
    )

    reduced = reduce_canonical_game_box_score(
        result
    )

    automatic_runner = (
        reduced.box_score.batter("away_2")
    )

    assert automatic_runner.runs == 1
    assert reduced.box_score.away.runs == 2
    assert reduced.reconciliation.passed is True
