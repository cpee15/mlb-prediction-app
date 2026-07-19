"""Canonical full-game orchestration."""

from .box_score import (
    CanonicalGameBoxScore,
    GameBoxScoreReconciliation,
    reduce_canonical_game_box_score,
    validate_game_box_score_reconciliation,
)
from .contracts import (
    CanonicalGameConfig,
    CanonicalGameResult,
    CanonicalLineup,
    GameCompletionReason,
    HalfInningRecord,
)
from .orchestrator import (
    PlateAppearanceResolver,
    simulate_canonical_game,
)
from .validation import (
    CanonicalGameValidation,
    validate_canonical_game,
)

__all__ = [
    "CanonicalGameBoxScore",
    "GameBoxScoreReconciliation",
    "CanonicalGameConfig",
    "CanonicalGameResult",
    "CanonicalGameValidation",
    "CanonicalLineup",
    "GameCompletionReason",
    "HalfInningRecord",
    "PlateAppearanceResolver",
    "simulate_canonical_game",
    "reduce_canonical_game_box_score",
    "validate_game_box_score_reconciliation",
    "validate_canonical_game",
]
