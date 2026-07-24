"""Summarize canonical baserunning validation across games."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from .baserunning_output_validation import (
    CanonicalBaserunningOutputValidation,
)


CANONICAL_BASERUNNING_SHADOW_SUMMARY_VERSION = (
    "canonical_baserunning_shadow_summary_v1"
)

_VALID_STATUSES = {
    "ready",
    "unavailable",
    "error",
}


@dataclass(frozen=True)
class CanonicalBaserunningShadowSummary:
    status: str = "unavailable"
    validation_count: int = 0
    ready_count: int = 0
    unavailable_count: int = 0
    error_count: int = 0
    simulation_count_total: int = 0
    runner_projection_count_total: int = 0
    stolen_base_mean_total: float = 0.0
    caught_stealing_mean_total: float = 0.0
    active_validation_count: int = 0
    catalog_digests: Tuple[str, ...] = ()
    warnings: Tuple[str, ...] = ()
    error_message: Optional[str] = None
    summary_version: str = (
        CANONICAL_BASERUNNING_SHADOW_SUMMARY_VERSION
    )

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(
                "unsupported baserunning shadow summary status"
            )

        counts = (
            self.validation_count,
            self.ready_count,
            self.unavailable_count,
            self.error_count,
            self.simulation_count_total,
            self.runner_projection_count_total,
            self.active_validation_count,
        )
        if any(value < 0 for value in counts):
            raise ValueError(
                "baserunning shadow summary counts "
                "must be nonnegative"
            )

        if (
            self.ready_count
            + self.unavailable_count
            + self.error_count
            != self.validation_count
        ):
            raise ValueError(
                "validation status counts must match "
                "validation_count"
            )

        if self.active_validation_count > self.ready_count:
            raise ValueError(
                "active_validation_count cannot exceed "
                "ready_count"
            )

        if self.summary_version != (
            CANONICAL_BASERUNNING_SHADOW_SUMMARY_VERSION
        ):
            raise ValueError(
                "unsupported baserunning shadow summary version"
            )

    @property
    def ready(self) -> bool:
        return self.status == "ready"

    @property
    def observed_activity(self) -> bool:
        return self.active_validation_count > 0

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.summary_version,
            "status": self.status,
            "ready": self.ready,
            "validation_count": self.validation_count,
            "ready_count": self.ready_count,
            "unavailable_count": self.unavailable_count,
            "error_count": self.error_count,
            "simulation_count_total": (
                self.simulation_count_total
            ),
            "runner_projection_count_total": (
                self.runner_projection_count_total
            ),
            "stolen_base_mean_total": (
                self.stolen_base_mean_total
            ),
            "caught_stealing_mean_total": (
                self.caught_stealing_mean_total
            ),
            "active_validation_count": (
                self.active_validation_count
            ),
            "observed_activity": self.observed_activity,
            "catalog_digests": self.catalog_digests,
            "warnings": self.warnings,
            "error_message": self.error_message,
            "activation_permitted": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def summarize_canonical_baserunning_shadow_validations(
    validations: Any,
) -> CanonicalBaserunningShadowSummary:
    """
    Aggregate per-game shadow validation into slate diagnostics.

    This reports coverage and projected activity only. It does not
    approve calibration or production activation.
    """

    if not isinstance(validations, tuple):
        return CanonicalBaserunningShadowSummary(
            status="error",
            error_message="validations must be a tuple",
        )

    if not validations:
        return CanonicalBaserunningShadowSummary(
            status="unavailable",
            error_message=(
                "no baserunning shadow validations were supplied"
            ),
        )

    if not all(
        isinstance(
            validation,
            CanonicalBaserunningOutputValidation,
        )
        for validation in validations
    ):
        return CanonicalBaserunningShadowSummary(
            status="error",
            validation_count=len(validations),
            error_count=len(validations),
            error_message=(
                "validations must contain "
                "CanonicalBaserunningOutputValidation"
            ),
        )

    ready = tuple(
        value
        for value in validations
        if value.status == "ready"
    )
    unavailable_count = sum(
        value.status == "unavailable"
        for value in validations
    )
    error_count = sum(
        value.status == "error"
        for value in validations
    )

    warnings = set(
        warning
        for value in validations
        for warning in value.warnings
    )

    if unavailable_count:
        warnings.add(
            "incomplete_baserunning_shadow_coverage"
        )
    if error_count:
        warnings.add(
            "baserunning_shadow_validation_errors"
        )

    catalog_digests = tuple(
        sorted(
            {
                value.catalog_digest
                for value in ready
                if value.catalog_digest is not None
            }
        )
    )

    if not ready:
        return CanonicalBaserunningShadowSummary(
            status=(
                "error"
                if error_count
                else "unavailable"
            ),
            validation_count=len(validations),
            ready_count=0,
            unavailable_count=unavailable_count,
            error_count=error_count,
            catalog_digests=catalog_digests,
            warnings=tuple(sorted(warnings)),
            error_message=(
                "no ready baserunning shadow validations"
            ),
        )

    return CanonicalBaserunningShadowSummary(
        status="ready",
        validation_count=len(validations),
        ready_count=len(ready),
        unavailable_count=unavailable_count,
        error_count=error_count,
        simulation_count_total=sum(
            value.simulation_count
            for value in ready
        ),
        runner_projection_count_total=sum(
            value.runner_projection_count
            for value in ready
        ),
        stolen_base_mean_total=round(
            sum(
                value.stolen_base_mean_total
                for value in ready
            ),
            6,
        ),
        caught_stealing_mean_total=round(
            sum(
                value.caught_stealing_mean_total
                for value in ready
            ),
            6,
        ),
        active_validation_count=sum(
            value.observed_activity
            for value in ready
        ),
        catalog_digests=catalog_digests,
        warnings=tuple(sorted(warnings)),
    )
