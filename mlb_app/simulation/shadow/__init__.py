"""Canonical production-shadow comparison integration."""

from .comparator import compare_shadow_payloads
from .bullpen_discovery import (
    CANONICAL_SHADOW_BULLPEN_DISCOVERY_VERSION,
    CanonicalShadowBullpenDiscovery,
    CanonicalShadowBullpenSideDiscovery,
    discover_canonical_shadow_bullpens,
)
from .bootstrap_readiness import (
    CANONICAL_SHADOW_BOOTSTRAP_READINESS_VERSION,
    build_canonical_shadow_bootstrap_readiness,
)
from .contracts import (
    SHADOW_SCHEMA_VERSION,
    CanonicalShadowDiagnostics,
    MetricComparison,
    RangeComparison,
    ShadowCoverage,
)
from .execution_bundle import (
    CANONICAL_SHADOW_EXECUTION_BUNDLE_VERSION,
    CanonicalShadowExecutionBundle,
    CanonicalShadowExecutionMaterial,
    canonical_shadow_execution_bundle_to_material,
)
from .execution_factory import (
    CANONICAL_SHADOW_EXECUTION_BUNDLE_FACTORY_VERSION,
    CanonicalShadowExecutionBundleFactory,
    build_canonical_shadow_execution_bundle_factory,
)
from .production_execution import (
    CANONICAL_PRODUCTION_SHADOW_EXECUTION_VERSION,
    DEFAULT_PRODUCTION_SHADOW_SIMULATION_COUNT,
    CanonicalProductionShadowExecution,
    run_canonical_production_shadow,
)
from .probability_provider_discovery import (
    CANONICAL_SHADOW_PROBABILITY_PROVIDER_DISCOVERY_VERSION,
    CanonicalShadowProbabilityProviderDiscovery,
    discover_canonical_shadow_probability_provider,
)
from .exact_artifact_discovery import (
    CANONICAL_SHADOW_EXACT_ARTIFACT_DISCOVERY_VERSION,
    MIN_EXACT_BATTER_RECORDS_PER_SIDE,
    CanonicalShadowExactArtifactDiscovery,
    discover_canonical_shadow_exact_artifact,
)
from .fallback_catalog_discovery import (
    CANONICAL_SHADOW_FALLBACK_CATALOG_DISCOVERY_VERSION,
    CanonicalShadowFallbackCatalogDiscovery,
    discover_canonical_shadow_fallback_catalog,
)
from .lineup_discovery import (
    CANONICAL_SHADOW_LINEUP_DISCOVERY_VERSION,
    CanonicalShadowLineupDiscovery,
    discover_canonical_shadow_lineups,
)
from .input_assembly import (
    CANONICAL_SHADOW_INPUT_ASSEMBLY_VERSION,
    CanonicalShadowExecutionInputs,
    assemble_canonical_shadow_execution_inputs,
)
from .input_serialization import (
    CANONICAL_SHADOW_INPUT_PROVENANCE_VERSION,
    canonical_shadow_input_provenance_to_dict,
)
from .integration import attach_canonical_shadow
from .trial_adapter import canonical_trial_batch_to_shadow_payload
from .serialization import (
    shadow_diagnostics_to_dict,
)
from .probability_serialization import (
    CANONICAL_PROBABILITY_DIAGNOSTICS_SHADOW_VERSION,
    probability_resolution_diagnostics_to_dict,
)

__all__ = [
    "SHADOW_SCHEMA_VERSION",
    "CANONICAL_SHADOW_BOOTSTRAP_READINESS_VERSION",
    "CANONICAL_SHADOW_BULLPEN_DISCOVERY_VERSION",
    "CANONICAL_SHADOW_EXECUTION_BUNDLE_VERSION",
    "CANONICAL_SHADOW_EXECUTION_BUNDLE_FACTORY_VERSION",
    "CANONICAL_SHADOW_INPUT_ASSEMBLY_VERSION",
    "CANONICAL_SHADOW_EXACT_ARTIFACT_DISCOVERY_VERSION",
    "CANONICAL_SHADOW_FALLBACK_CATALOG_DISCOVERY_VERSION",
    "CANONICAL_SHADOW_LINEUP_DISCOVERY_VERSION",
    "DEFAULT_PRODUCTION_SHADOW_SIMULATION_COUNT",
    "MIN_EXACT_BATTER_RECORDS_PER_SIDE",
    "CANONICAL_PRODUCTION_SHADOW_EXECUTION_VERSION",
    "CANONICAL_SHADOW_PROBABILITY_PROVIDER_DISCOVERY_VERSION",
    "CANONICAL_SHADOW_INPUT_PROVENANCE_VERSION",
    "CANONICAL_PROBABILITY_DIAGNOSTICS_SHADOW_VERSION",
    "CanonicalShadowDiagnostics",
    "CanonicalShadowBullpenDiscovery",
    "CanonicalShadowBullpenSideDiscovery",
    "CanonicalShadowExecutionBundle",
    "CanonicalShadowExecutionBundleFactory",
    "CanonicalShadowExecutionInputs",
    "CanonicalShadowExactArtifactDiscovery",
    "CanonicalShadowFallbackCatalogDiscovery",
    "CanonicalShadowLineupDiscovery",
    "CanonicalProductionShadowExecution",
    "CanonicalShadowProbabilityProviderDiscovery",
    "CanonicalShadowExecutionMaterial",
    "MetricComparison",
    "RangeComparison",
    "ShadowCoverage",
    "attach_canonical_shadow",
    "assemble_canonical_shadow_execution_inputs",
    "build_canonical_shadow_bootstrap_readiness",
    "discover_canonical_shadow_bullpens",
    "discover_canonical_shadow_exact_artifact",
    "discover_canonical_shadow_fallback_catalog",
    "discover_canonical_shadow_lineups",
    "discover_canonical_shadow_probability_provider",
    "run_canonical_production_shadow",
    "canonical_shadow_execution_bundle_to_material",
    "canonical_shadow_input_provenance_to_dict",
    "build_canonical_shadow_execution_bundle_factory",
    "canonical_trial_batch_to_shadow_payload",
    "compare_shadow_payloads",
    "probability_resolution_diagnostics_to_dict",
    "shadow_diagnostics_to_dict",
]
