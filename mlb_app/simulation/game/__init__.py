"""Canonical full-game orchestration."""

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
    "CanonicalGameConfig",
    "CanonicalGameResult",
    "CanonicalGameValidation",
    "CanonicalLineup",
    "GameCompletionReason",
    "HalfInningRecord",
    "PlateAppearanceResolver",
    "simulate_canonical_game",
    "validate_canonical_game",
]
