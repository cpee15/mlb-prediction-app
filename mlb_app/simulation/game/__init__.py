"""Canonical full-game orchestration."""

from .box_score import (
    CanonicalGameBoxScore,
    GameBoxScoreReconciliation,
    reduce_canonical_game_box_score,
    validate_game_box_score_reconciliation,
)
from .batted_ball_resolution import (
    CANONICAL_BATTED_BALL_RESOLUTION_VERSION,
    CANONICAL_OUT_SUBTYPES,
    SUPPORTED_BATTED_BALL_ADVANCEMENT_OUTCOMES,
    CanonicalBattedBallResolution,
    derive_canonical_batted_ball_seed,
    resolve_canonical_batted_ball_outcome,
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
from .outcome_resolution import (
    EMPTY_BASE_HIT_DESTINATIONS,
    resolve_canonical_sampled_plate_appearance,
)
from .pa_resolver_factory import (
    CANONICAL_PA_RESOLVER_FACTORY_VERSION,
    CanonicalPlateAppearanceResolverFactory,
    build_canonical_pa_resolver_factory,
)
from .probability import (
    CANONICAL_PA_OUTCOME_ORDER,
    CANONICAL_PA_PROBABILITY_VERSION,
    CANONICAL_PA_SAMPLING_VERSION,
    CanonicalOutcomeProbability,
    CanonicalPlateAppearanceOutcome,
    CanonicalPlateAppearanceProbabilities,
    CanonicalPlateAppearanceProbabilityProvider,
    CanonicalPlateAppearanceQuery,
    CanonicalSampledPlateAppearance,
    derive_canonical_pa_sampling_seed,
    sample_canonical_plate_appearance,
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
    "CanonicalBattedBallResolution",
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
    "CANONICAL_BATTED_BALL_RESOLUTION_VERSION",
    "CANONICAL_OUT_SUBTYPES",
    "CANONICAL_PA_OUTCOME_ORDER",
    "CANONICAL_PA_PROBABILITY_VERSION",
    "CANONICAL_PA_RESOLVER_FACTORY_VERSION",
    "CANONICAL_PA_SAMPLING_VERSION",
    "DEFAULT_CANONICAL_MODEL_VERSION",
    "DEFAULT_CANONICAL_SIMULATION_COUNT",
    "EMPTY_BASE_HIT_DESTINATIONS",
    "CanonicalTrialFactoryInput",
    "CanonicalTrialExecutionPlan",
    "CanonicalTrialResolverContext",
    "CanonicalTrialResolverFactory",
    "DistributionPoint",
    "ProbabilityMetric",
    "SUPPORTED_BATTED_BALL_ADVANCEMENT_OUTCOMES",
    "CanonicalGameValidation",
    "CanonicalLineup",
    "CanonicalMatchupInput",
    "CanonicalOutcomeProbability",
    "CanonicalPlateAppearanceOutcome",
    "CanonicalPlateAppearanceProbabilities",
    "CanonicalPlateAppearanceResolverFactory",
    "CanonicalPlateAppearanceProbabilityProvider",
    "CanonicalPlateAppearanceQuery",
    "CanonicalSampledPlateAppearance",
    "CanonicalPitchingPlan",
    "CanonicalProbabilityProviderIdentity",
    "GameCompletionReason",
    "HalfInningRecord",
    "PlateAppearanceResolver",
    "simulate_canonical_game",
    "sample_canonical_plate_appearance",
    "aggregate_game_outcomes",
    "build_canonical_pa_resolver_factory",
    "build_canonical_trial_factory_input",
    "build_canonical_trial_resolver_context",
    "derive_canonical_base_seed",
    "derive_canonical_trial_seed",
    "derive_canonical_batted_ball_seed",
    "derive_canonical_pa_sampling_seed",
    "run_canonical_trials",
    "run_canonical_trial_execution_plan",
    "resolve_canonical_batted_ball_outcome",
    "resolve_canonical_sampled_plate_appearance",
    "reduce_canonical_game_box_score",
    "validate_game_box_score_reconciliation",
    "validate_canonical_game",
]
