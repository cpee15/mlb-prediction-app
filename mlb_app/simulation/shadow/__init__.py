"""Canonical production-shadow comparison integration."""

from .comparator import compare_shadow_payloads
from .contracts import (
    SHADOW_SCHEMA_VERSION,
    CanonicalShadowDiagnostics,
    MetricComparison,
    RangeComparison,
    ShadowCoverage,
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
    "CANONICAL_PROBABILITY_DIAGNOSTICS_SHADOW_VERSION",
    "CanonicalShadowDiagnostics",
    "MetricComparison",
    "RangeComparison",
    "ShadowCoverage",
    "attach_canonical_shadow",
    "canonical_trial_batch_to_shadow_payload",
    "compare_shadow_payloads",
    "probability_resolution_diagnostics_to_dict",
    "shadow_diagnostics_to_dict",
]
