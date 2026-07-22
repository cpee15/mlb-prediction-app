from dataclasses import replace

import pytest

from mlb_app.simulation.events import (
    Base,
    OutRecord,
    PlayEvent,
    RunnerMovement,
)
from mlb_app.simulation.game import (
    CanonicalExecutedTrial,
    CanonicalPitcherRunLine,
    CanonicalGameConfig,
    CanonicalLineup,
    run_canonical_trials,
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
    fielding_side = (
        "home"
        if state.half == "top"
        else "away"
    )

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
        pitcher_id=f"{fielding_side}_pitcher",
    )


def home_run_event(state, batter_id, sequence):
    scorers = tuple(
        runner
        for runner in state.bases
        if runner is not None
    ) + (batter_id,)

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
        pitcher_id = "home_pitcher"
    else:
        home_score += len(scorers)
        pitcher_id = "away_pitcher"

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
        runs_scored=scorers,
        pitcher_id=pitcher_id,
    )


def game_factory(winners, observed_indices):
    from mlb_app.simulation.game import (
        simulate_canonical_game,
    )

    def factory(index):
        observed_indices.append(index)
        winner = winners[index]
        calls = {"count": 0}

        def resolver(state, batter_id, sequence):
            call = calls["count"]
            calls["count"] += 1

            if winner == "away" and call == 0:
                return home_run_event(
                    state,
                    batter_id,
                    sequence,
                )

            if winner == "home" and call == 3:
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

        return simulate_canonical_game(
            away_lineup=lineup("away"),
            home_lineup=lineup("home"),
            resolve_plate_appearance=resolver,
            config=CanonicalGameConfig(
                regulation_innings=1,
                max_extra_innings=0,
                automatic_runner_enabled=False,
            ),
        )

    return factory


def probability(metrics, name):
    return next(
        metric.probability
        for metric in metrics
        if metric.name == name
    )


def distribution(values):
    return {
        point.value: point.probability
        for point in values
    }


def test_trials_share_one_coherent_simulation_set():
    observed_indices = []

    batch = run_canonical_trials(
        trial_factory=game_factory(
            ("away", "home", "tie"),
            observed_indices,
        ),
        simulations=3,
        model_version="canonical_trial_test_v1",
    )

    assert observed_indices == [0, 1, 2]
    assert len(batch.games) == 3
    assert len(batch.box_scores) == 3
    assert batch.projections.simulation_count == 3
    assert batch.outcomes.simulation_count == 3
    assert (
        batch.diagnostics.game_validation_pass_rate
        == 1.0
    )
    assert (
        batch.diagnostics
        .box_score_reconciliation_pass_rate
        == 1.0
    )


def test_outcome_probabilities_are_observed_frequencies():
    batch = run_canonical_trials(
        trial_factory=game_factory(
            ("away", "home", "tie"),
            [],
        ),
        simulations=3,
        model_version="canonical_trial_test_v1",
    )

    assert (
        batch.outcomes.away_win_probability
        == pytest.approx(1 / 3, abs=0.000001)
    )
    assert (
        batch.outcomes.home_win_probability
        == pytest.approx(1 / 3, abs=0.000001)
    )
    assert (
        batch.outcomes.tie_probability
        == pytest.approx(1 / 3, abs=0.000001)
    )


def test_run_distributions_match_exact_trial_scores():
    batch = run_canonical_trials(
        trial_factory=game_factory(
            ("away", "home", "tie"),
            [],
        ),
        simulations=3,
        model_version="canonical_trial_test_v1",
    )

    assert distribution(
        batch.outcomes.away_run_distribution
    ) == {
        0: pytest.approx(2 / 3, abs=0.000001),
        1: pytest.approx(1 / 3, abs=0.000001),
    }
    assert distribution(
        batch.outcomes.home_run_distribution
    ) == {
        0: pytest.approx(2 / 3, abs=0.000001),
        1: pytest.approx(1 / 3, abs=0.000001),
    }
    assert distribution(
        batch.outcomes.total_run_distribution
    ) == {
        0: pytest.approx(1 / 3, abs=0.000001),
        1: pytest.approx(2 / 3, abs=0.000001),
    }


def test_player_projection_uses_same_reduced_trials():
    batch = run_canonical_trials(
        trial_factory=game_factory(
            ("away", "home", "tie"),
            [],
        ),
        simulations=3,
        model_version="canonical_trial_test_v1",
    )

    away_zero = next(
        player
        for player in batch.projections.batters
        if (
            player.team_side == "away"
            and player.player_id == "away_0"
        )
    )

    home_runs = next(
        metric
        for metric in away_zero.metrics
        if metric.name == "home_runs"
    )

    assert home_runs.summary.count == 3
    assert home_runs.summary.mean == pytest.approx(
        1 / 3,
        abs=0.000001,
    )


def test_threshold_probabilities_come_from_trials():
    batch = run_canonical_trials(
        trial_factory=game_factory(
            ("away", "home", "tie"),
            [],
        ),
        simulations=3,
        model_version="canonical_trial_test_v1",
    )

    assert probability(
        batch.outcomes.team_total_probabilities,
        "away_3_plus",
    ) == 0.0
    assert probability(
        batch.outcomes.total_probabilities,
        "over_6.5",
    ) == 0.0
    assert probability(
        batch.outcomes.total_probabilities,
        "under_6.5",
    ) == 1.0


def test_nonpositive_simulation_count_is_rejected():
    with pytest.raises(
        ValueError,
        match="simulations must be positive",
    ):
        run_canonical_trials(
            trial_factory=lambda index: None,
            simulations=0,
            model_version="canonical_trial_test_v1",
        )

def test_executed_trial_marks_pitcher_runs_reconstructed():
    observed_indices = []

    game = game_factory(
        winners=("away",),
        observed_indices=observed_indices,
    )(0)

    pitcher_ids = tuple(
        sorted(
            {
                event.pitcher_id
                for event in game.events
                if event.pitcher_id is not None
            }
        )
    )

    assert pitcher_ids
    assert observed_indices == [0]

    responsible_pitcher = next(
        event.pitcher_id
        for event in game.events
        if event.runs_scored
    )

    executed = CanonicalExecutedTrial(
        game=game,
        reconstructed_pitcher_run_lines=(
            CanonicalPitcherRunLine(
                pitcher_id=responsible_pitcher,
                runs_allowed=1,
                earned_runs=1,
                unearned_runs=0,
            ),
        ),
        earned_run_reconstruction_complete=True,
    )

    batch = run_canonical_trials(
        trial_factory=lambda _: executed,
        simulations=1,
        model_version="test-model",
    )

    assert (
        batch.projections.diagnostics
        .earned_run_status
        == "reconstructed"
    )
    assert (
        "earned_runs_not_fully_reconstructed"
        not in batch.projections.diagnostics.warnings
    )

    for line in batch.box_scores[0].pitchers:
        assert line.earned_run_status == (
            "reconstructed"
        )
        assert line.earned_runs is not None
