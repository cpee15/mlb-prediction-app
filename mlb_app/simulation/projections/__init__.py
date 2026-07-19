"""Canonical aggregate projection payloads."""

from .aggregator import (
    aggregate_projection_payload,
    summarize_values,
)
from .contracts import (
    CANONICAL_PROJECTION_SCHEMA_VERSION,
    CanonicalProjectionPayload,
    MetricProjection,
    PlayerProjection,
    ProjectionDiagnostics,
    StatisticalSummary,
    TeamProjection,
)
from .serialization import (
    projection_payload_to_dict,
)
from .validation import (
    ProjectionPayloadValidation,
    validate_projection_payload,
)

__all__ = [
    "CANONICAL_PROJECTION_SCHEMA_VERSION",
    "CanonicalProjectionPayload",
    "MetricProjection",
    "PlayerProjection",
    "ProjectionDiagnostics",
    "ProjectionPayloadValidation",
    "StatisticalSummary",
    "TeamProjection",
    "aggregate_projection_payload",
    "projection_payload_to_dict",
    "summarize_values",
    "validate_projection_payload",
]
