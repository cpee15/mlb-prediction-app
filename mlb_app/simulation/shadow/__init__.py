"""Canonical production-shadow comparison integration."""

from .comparator import compare_shadow_payloads
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
    "CANONICAL_SHADOW_EXECUTION_BUNDLE_VERSION",
    "CANONICAL_SHADOW_EXECUTION_BUNDLE_FACTORY_VERSION",
    "CANONICAL_SHADOW_INPUT_ASSEMBLY_VERSION",
    "CANONICAL_SHADOW_LINEUP_DISCOVERY_VERSION",
    "CANONICAL_SHADOW_INPUT_PROVENANCE_VERSION",
    "CANONICAL_PROBABILITY_DIAGNOSTICS_SHADOW_VERSION",
    "CanonicalShadowDiagnostics",
    "CanonicalShadowExecutionBundle",
    "CanonicalShadowExecutionBundleFactory",
    "CanonicalShadowExecutionInputs",
    "CanonicalShadowLineupDiscovery",
    "CanonicalShadowExecutionMaterial",
    "MetricComparison",
    "RangeComparison",
    "ShadowCoverage",
    "attach_canonical_shadow",
    "assemble_canonical_shadow_execution_inputs",
    "build_canonical_shadow_bootstrap_readiness",
    "discover_canonical_shadow_lineups",
    "canonical_shadow_execution_bundle_to_material",
    "canonical_shadow_input_provenance_to_dict",
    "build_canonical_shadow_execution_bundle_factory",
    "canonical_trial_batch_to_shadow_payload",
    "compare_shadow_payloads",
    "probability_resolution_diagnostics_to_dict",
    "shadow_diagnostics_to_dict",
]
