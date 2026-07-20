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
from .factory_input import (
    CANONICAL_TRIAL_FACTORY_INPUT_VERSION,
    CANONICAL_TRIAL_SEED_VERSION,
    DEFAULT_CANONICAL_MODEL_VERSION,
    DEFAULT_CANONICAL_SIMULATION_COUNT,
    CanonicalTrialFactoryInput,
    build_canonical_trial_factory_input,
    derive_canonical_base_seed,
    derive_canonical_trial_seed,
)
from .matchup_input import (
    CANONICAL_MATCHUP_INPUT_VERSION,
    CanonicalMatchupInput,
    CanonicalPitchingPlan,
    CanonicalProbabilityProviderIdentity,
)
from .orchestrator import (
    PlateAppearanceResolver,
    simulate_canonical_game,
)
from .trial_factory import (
    CanonicalTrialExecutionPlan,
    CanonicalTrialResolverContext,
    CanonicalTrialResolverFactory,
    build_canonical_trial_resolver_context,
    run_canonical_trial_execution_plan,
)
from .trials import (
    CanonicalGameOutcomeProjection,
    CanonicalTrialBatch,
    CanonicalTrialDiagnostics,
    CanonicalTrialFactory,
    DistributionPoint,
    ProbabilityMetric,
    aggregate_game_outcomes,
    run_canonical_trials,
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
    "CanonicalGameOutcomeProjection",
    "CanonicalTrialBatch",
    "CanonicalTrialDiagnostics",
    "CanonicalTrialFactory",
    "CANONICAL_TRIAL_FACTORY_INPUT_VERSION",
    "CANONICAL_TRIAL_SEED_VERSION",
    "CANONICAL_MATCHUP_INPUT_VERSION",
    "DEFAULT_CANONICAL_MODEL_VERSION",
    "DEFAULT_CANONICAL_SIMULATION_COUNT",
    "CanonicalTrialFactoryInput",
    "CanonicalTrialExecutionPlan",
    "CanonicalTrialResolverContext",
    "CanonicalTrialResolverFactory",
    "DistributionPoint",
    "ProbabilityMetric",
    "CanonicalGameValidation",
    "CanonicalLineup",
    "CanonicalMatchupInput",
    "CanonicalPitchingPlan",
    "CanonicalProbabilityProviderIdentity",
    "GameCompletionReason",
    "HalfInningRecord",
    "PlateAppearanceResolver",
    "simulate_canonical_game",
    "aggregate_game_outcomes",
    "build_canonical_trial_factory_input",
    "build_canonical_trial_resolver_context",
    "derive_canonical_base_seed",
    "derive_canonical_trial_seed",
    "run_canonical_trials",
    "run_canonical_trial_execution_plan",
    "reduce_canonical_game_box_score",
    "validate_game_box_score_reconciliation",
    "validate_canonical_game",
]
