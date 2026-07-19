"""Canonical event contracts for the MLB simulation engine."""

from .advancement_model import (
    AdvancementProbability,
    BaselineRunnerAdvancementModel,
    validate_baseline_advancement_rates,
)
from .advancement_sampler import (
    BaselineRunnerAdvancementSampler,
    RunnerAdvancementResult,
)
from .advancement_version import (
    BASELINE_RUNNER_ADVANCEMENT_METADATA,
    BASELINE_RUNNER_ADVANCEMENT_MODEL_VERSION,
    AdvancementModelMetadata,
)
from .batted_ball import (
    BASELINE_BATTED_BALL_MODEL_VERSION,
    BattedBallContext,
    BattedBallDepth,
    BattedBallType,
    BaselineBattedBallContextProvider,
    ContactQuality,
    SprayDirection,
    validate_baseline_batted_ball_distributions,
)
from .contracts import (
    Base,
    GameState,
    OutRecord,
    PlayEvent,
    PlayLedger,
    RunnerMovement,
)
from .legal_transitions import (
    LegalRunnerDestinations,
    enumerate_legal_runner_destinations,
    validate_runner_movements,
)
from .replay import replay_events
from .resolver import DeterministicPlayResolver

__all__ = [
    "AdvancementModelMetadata",
    "AdvancementProbability",
    "BASELINE_BATTED_BALL_MODEL_VERSION",
    "BASELINE_RUNNER_ADVANCEMENT_METADATA",
    "BASELINE_RUNNER_ADVANCEMENT_MODEL_VERSION",
    "Base",
    "BattedBallContext",
    "BattedBallDepth",
    "BattedBallType",
    "BaselineBattedBallContextProvider",
    "BaselineRunnerAdvancementModel",
    "BaselineRunnerAdvancementSampler",
    "ContactQuality",
    "DeterministicPlayResolver",
    "GameState",
    "LegalRunnerDestinations",
    "OutRecord",
    "PlayEvent",
    "PlayLedger",
    "RunnerAdvancementResult",
    "RunnerMovement",
    "SprayDirection",
    "enumerate_legal_runner_destinations",
    "replay_events",
    "validate_baseline_advancement_rates",
    "validate_baseline_batted_ball_distributions",
    "validate_runner_movements",
]
