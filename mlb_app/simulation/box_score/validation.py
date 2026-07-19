"""Replay and box-score reconstruction validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from mlb_app.simulation.events import (
    GameState,
    PlayEvent,
    replay_events,
)

from .contracts import ReducedBoxScore
from .reducer import reduce_box_score


@dataclass(frozen=True)
class BoxScoreValidation:
    replay_matches_final_state: bool
    scoreboard_matches_team_runs: bool
    batter_runs_match_team_runs: bool
    deterministic_reduction: bool
    pitcher_attribution_complete: bool

    @property
    def passed(self) -> bool:
        return (
            self.replay_matches_final_state
            and self.scoreboard_matches_team_runs
            and self.batter_runs_match_team_runs
            and self.deterministic_reduction
        )


def validate_box_score_reconstruction(
    *,
    initial_state: GameState,
    events: Iterable[PlayEvent],
    box_score: ReducedBoxScore,
) -> BoxScoreValidation:
    event_tuple = tuple(events)

    replayed = replay_events(
        initial_state,
        event_tuple,
    )

    expected_final = (
        event_tuple[-1].state_after
        if event_tuple
        else initial_state
    )

    away_delta = (
        replayed.away_score
        - initial_state.away_score
    )
    home_delta = (
        replayed.home_score
        - initial_state.home_score
    )

    batter_away_runs = sum(
        line.runs
        for line in box_score.batters
        if line.team_side == "away"
    )
    batter_home_runs = sum(
        line.runs
        for line in box_score.batters
        if line.team_side == "home"
    )

    second_reduction = reduce_box_score(
        initial_state=initial_state,
        events=event_tuple,
    )

    return BoxScoreValidation(
        replay_matches_final_state=(
            replayed == expected_final
        ),
        scoreboard_matches_team_runs=(
            box_score.away.runs == away_delta
            and box_score.home.runs == home_delta
        ),
        batter_runs_match_team_runs=(
            batter_away_runs == box_score.away.runs
            and batter_home_runs == box_score.home.runs
        ),
        deterministic_reduction=(
            second_reduction == box_score
        ),
        pitcher_attribution_complete=(
            box_score.pitcher_attribution_complete
        ),
    )
