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
from .serialization import (
    shadow_diagnostics_to_dict,
)

__all__ = [
    "SHADOW_SCHEMA_VERSION",
    "CanonicalShadowDiagnostics",
    "MetricComparison",
    "RangeComparison",
    "ShadowCoverage",
    "attach_canonical_shadow",
    "compare_shadow_payloads",
    "shadow_diagnostics_to_dict",
]
