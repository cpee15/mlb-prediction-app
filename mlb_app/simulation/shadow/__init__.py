"""Canonical production-shadow comparison integration."""

from .comparator import compare_shadow_payloads
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
from .input_assembly import (
    CANONICAL_SHADOW_INPUT_ASSEMBLY_VERSION,
    CanonicalShadowExecutionInputs,
    assemble_canonical_shadow_execution_inputs,
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
    "CANONICAL_SHADOW_EXECUTION_BUNDLE_VERSION",
    "CANONICAL_SHADOW_EXECUTION_BUNDLE_FACTORY_VERSION",
    "CANONICAL_SHADOW_INPUT_ASSEMBLY_VERSION",
    "CANONICAL_PROBABILITY_DIAGNOSTICS_SHADOW_VERSION",
    "CanonicalShadowDiagnostics",
    "CanonicalShadowExecutionBundle",
    "CanonicalShadowExecutionBundleFactory",
    "CanonicalShadowExecutionInputs",
    "CanonicalShadowExecutionMaterial",
    "MetricComparison",
    "RangeComparison",
    "ShadowCoverage",
    "attach_canonical_shadow",
    "assemble_canonical_shadow_execution_inputs",
    "canonical_shadow_execution_bundle_to_material",
    "build_canonical_shadow_execution_bundle_factory",
    "canonical_trial_batch_to_shadow_payload",
    "compare_shadow_payloads",
    "probability_resolution_diagnostics_to_dict",
    "shadow_diagnostics_to_dict",
]
