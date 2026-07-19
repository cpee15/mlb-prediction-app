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

from .event_validation import validate_resolved_play_components
from .multi_out_resolver import MultiOutPlayResolver
from .play_resolution import build_play_event
from .attribution import (
    ErrorType,
    PlayAttribution,
    SacrificeType,
)
from .scoring_rules import (
    counted_scorers,
    third_out_is_force_or_batter_runner_out,
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
    "ErrorType",
    "MultiOutPlayResolver",
    "PlayAttribution",
    "SacrificeType",
    "build_play_event",
    "counted_scorers",
    "third_out_is_force_or_batter_runner_out",
    "validate_resolved_play_components",
    "replay_events",
    "validate_baseline_advancement_rates",
    "validate_baseline_batted_ball_distributions",
    "validate_runner_movements",
]
