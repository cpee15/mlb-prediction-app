"""Immutable canonical-shadow comparison contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple


SHADOW_SCHEMA_VERSION = "canonical_shadow_v1"

VALID_SHADOW_STATUSES = frozenset(
    {
        "disabled",
        "unavailable",
        "partial",
        "complete",
        "error",
    }
)


@dataclass(frozen=True)
class MetricComparison:
    """Comparison between one legacy and canonical metric."""

    name: str
    legacy_value: Optional[float]
    canonical_value: Optional[float]
    absolute_difference: Optional[float]
    available: bool

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError(
                "comparison metric name is required"
            )

        if self.available:
            if (
                self.legacy_value is None
                or self.canonical_value is None
                or self.absolute_difference is None
            ):
                raise ValueError(
                    "available comparisons require both "
                    "values and a difference"
                )


@dataclass(frozen=True)
class RangeComparison:
    """Comparison between legacy and canonical ranges."""

    name: str
    legacy_minimum: Optional[float]
    legacy_maximum: Optional[float]
    canonical_minimum: Optional[float]
    canonical_maximum: Optional[float]
    available: bool

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError(
                "range comparison name is required"
            )

        if self.available:
            values = (
                self.legacy_minimum,
                self.legacy_maximum,
                self.canonical_minimum,
                self.canonical_maximum,
            )
            if any(value is None for value in values):
                raise ValueError(
                    "available range comparisons require "
                    "all boundaries"
                )


@dataclass(frozen=True)
class ShadowCoverage:
    """Coverage of supported comparison surfaces."""

    compared_metric_count: int
    possible_metric_count: int
    comparison_rate: float

    def __post_init__(self) -> None:
        if self.compared_metric_count < 0:
            raise ValueError(
                "compared_metric_count cannot be negative"
            )
        if self.possible_metric_count <= 0:
            raise ValueError(
                "possible_metric_count must be positive"
            )
        if (
            self.compared_metric_count
            > self.possible_metric_count
        ):
            raise ValueError(
                "compared count cannot exceed possible count"
            )
        if not 0.0 <= self.comparison_rate <= 1.0:
            raise ValueError(
                "comparison_rate must be between 0 and 1"
            )


@dataclass(frozen=True)
class CanonicalShadowDiagnostics:
    """Namespaced non-authoritative shadow diagnostics."""

    status: str
    enabled: bool
    canonical_available: bool
    authoritative_source: str
    comparisons: Tuple[MetricComparison, ...] = field(
        default_factory=tuple
    )
    ranges: Tuple[RangeComparison, ...] = field(
        default_factory=tuple
    )
    coverage: Optional[ShadowCoverage] = None
    legacy_simulation_count: Optional[int] = None
    canonical_simulation_count: Optional[int] = None
    pitcher_attribution_complete_rate: Optional[float] = None
    replay_validation_pass_rate: Optional[float] = None
    earned_run_status: Optional[str] = None
    warnings: Tuple[str, ...] = field(
        default_factory=tuple
    )
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    schema_version: str = SHADOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.status not in VALID_SHADOW_STATUSES:
            raise ValueError("invalid shadow status")

        if self.authoritative_source != "legacy":
            raise ValueError(
                "legacy must remain authoritative"
            )

        if self.schema_version != SHADOW_SCHEMA_VERSION:
            raise ValueError(
                "unsupported shadow schema version"
            )

        if self.status == "error":
            if not self.error_type:
                raise ValueError(
                    "error status requires error_type"
                )
