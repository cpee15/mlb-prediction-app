"""Canonical box-score reduction and DFS scoring."""

from .contracts import (
    BatterBoxScore,
    PitcherBoxScore,
    ReducedBoxScore,
    TeamBoxScore,
)
from .dfs_scoring import (
    BatterDfsScoringRules,
    PitcherDfsScoringRules,
    score_batter,
    score_pitcher,
)
from .reducer import reduce_box_score
from .validation import (
    BoxScoreValidation,
    validate_box_score_reconstruction,
)

__all__ = [
    "BatterBoxScore",
    "BatterDfsScoringRules",
    "BoxScoreValidation",
    "PitcherBoxScore",
    "PitcherDfsScoringRules",
    "ReducedBoxScore",
    "TeamBoxScore",
    "reduce_box_score",
    "score_batter",
    "score_pitcher",
    "validate_box_score_reconstruction",
]

from .merge import merge_reduced_box_scores
