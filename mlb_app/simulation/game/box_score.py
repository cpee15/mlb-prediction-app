"""Reduce a segmented canonical game into one coherent box score."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from mlb_app.simulation.box_score import (
    ReducedBoxScore,
    reduce_box_score,
)
from mlb_app.simulation.box_score.merge import (
    merge_reduced_box_scores,
)

from .contracts import CanonicalGameResult
from .validation import validate_canonical_game


@dataclass(frozen=True)
class GameBoxScoreReconciliation:
    """Coherence checks for canonical projected-box-score output."""

    game_validation_passed: bool
    away_score_matches: bool
    home_score_matches: bool
    away_batter_runs_match: bool
    home_batter_runs_match: bool
    away_batter_hits_match: bool
    home_batter_hits_match: bool
    pitcher_runs_match_when_complete: bool
    warnings: Tuple[str, ...]

    @property
    def passed(self) -> bool:
        return (
            self.game_validation_passed
            and self.away_score_matches
            and self.home_score_matches
            and self.away_batter_runs_match
            and self.home_batter_runs_match
            and self.away_batter_hits_match
            and self.home_batter_hits_match
            and self.pitcher_runs_match_when_complete
        )


@dataclass(frozen=True)
class CanonicalGameBoxScore:
    """Reduced full-game box score and reconciliation report."""

    box_score: ReducedBoxScore
    reconciliation: GameBoxScoreReconciliation


def reduce_canonical_game_box_score(
    result: CanonicalGameResult,
) -> CanonicalGameBoxScore:
    """
    Reduce each valid half-inning ledger and merge the results.

    This preserves half-inning replay validity while producing one
    coherent full-game team/player box score.
    """

    game_validation = validate_canonical_game(result)

    if not game_validation.passed:
        raise ValueError(
            "cannot reduce an invalid canonical game"
        )

    half_box_scores = tuple(
        reduce_box_score(
            initial_state=half.initial_state,
            events=half.ledger.events,
        )
        for half in result.halves
    )

    merged = merge_reduced_box_scores(
        half_box_scores
    )

    reconciliation = (
        validate_game_box_score_reconciliation(
            result=result,
            box_score=merged,
            game_validation_passed=(
                game_validation.passed
            ),
        )
    )

    if not reconciliation.passed:
        raise ValueError(
            "canonical game box score failed "
            "reconciliation"
        )

    return CanonicalGameBoxScore(
        box_score=merged,
        reconciliation=reconciliation,
    )


def validate_game_box_score_reconciliation(
    *,
    result: CanonicalGameResult,
    box_score: ReducedBoxScore,
    game_validation_passed: bool = True,
) -> GameBoxScoreReconciliation:
    """Validate team, batter, and pitcher game-level coherence."""

    away_batters = tuple(
        line
        for line in box_score.batters
        if line.team_side == "away"
    )
    home_batters = tuple(
        line
        for line in box_score.batters
        if line.team_side == "home"
    )

    away_score_matches = (
        box_score.away.runs
        == result.away_score
    )
    home_score_matches = (
        box_score.home.runs
        == result.home_score
    )

    away_batter_runs_match = (
        sum(line.runs for line in away_batters)
        == box_score.away.runs
    )
    home_batter_runs_match = (
        sum(line.runs for line in home_batters)
        == box_score.home.runs
    )

    away_batter_hits_match = (
        sum(line.hits for line in away_batters)
        == box_score.away.hits
    )
    home_batter_hits_match = (
        sum(line.hits for line in home_batters)
        == box_score.home.hits
    )

    pitcher_runs_match_when_complete = True

    if box_score.pitcher_attribution_complete:
        away_pitcher_runs = sum(
            line.runs_allowed
            for line in box_score.pitchers
            if line.team_side == "away"
        )
        home_pitcher_runs = sum(
            line.runs_allowed
            for line in box_score.pitchers
            if line.team_side == "home"
        )

        pitcher_runs_match_when_complete = (
            away_pitcher_runs
            == box_score.home.runs
            and home_pitcher_runs
            == box_score.away.runs
        )

    warnings = []

    checks = {
        "game_validation_failed": (
            game_validation_passed
        ),
        "away_score_mismatch": (
            away_score_matches
        ),
        "home_score_mismatch": (
            home_score_matches
        ),
        "away_batter_run_mismatch": (
            away_batter_runs_match
        ),
        "home_batter_run_mismatch": (
            home_batter_runs_match
        ),
        "away_batter_hit_mismatch": (
            away_batter_hits_match
        ),
        "home_batter_hit_mismatch": (
            home_batter_hits_match
        ),
        "pitcher_run_mismatch": (
            pitcher_runs_match_when_complete
        ),
    }

    for warning, passed in checks.items():
        if not passed:
            warnings.append(warning)

    if not box_score.pitcher_attribution_complete:
        warnings.append(
            "pitcher_attribution_incomplete"
        )

    return GameBoxScoreReconciliation(
        game_validation_passed=(
            game_validation_passed
        ),
        away_score_matches=away_score_matches,
        home_score_matches=home_score_matches,
        away_batter_runs_match=(
            away_batter_runs_match
        ),
        home_batter_runs_match=(
            home_batter_runs_match
        ),
        away_batter_hits_match=(
            away_batter_hits_match
        ),
        home_batter_hits_match=(
            home_batter_hits_match
        ),
        pitcher_runs_match_when_complete=(
            pitcher_runs_match_when_complete
        ),
        warnings=tuple(warnings),
    )
