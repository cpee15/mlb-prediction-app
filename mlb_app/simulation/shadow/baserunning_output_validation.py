"""Validate end-to-end canonical baserunning shadow outputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

from .production_execution import (
    CanonicalProductionShadowExecution,
)


CANONICAL_BASERUNNING_OUTPUT_VALIDATION_VERSION = (
    "canonical_baserunning_output_validation_v1"
)

_VALID_STATUSES = {
    "ready",
    "unavailable",
    "error",
}


@dataclass(frozen=True)
class CanonicalBaserunningOutputValidation:
    status: str = "unavailable"
    simulation_count: int = 0
    catalog_digest: Optional[str] = None
    runner_projection_count: int = 0
    stolen_base_mean_total: float = 0.0
    caught_stealing_mean_total: float = 0.0
    warnings: Tuple[str, ...] = ()
    error_message: Optional[str] = None
    validation_version: str = (
        CANONICAL_BASERUNNING_OUTPUT_VALIDATION_VERSION
    )

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(
                "unsupported baserunning validation status"
            )

        if self.simulation_count < 0:
            raise ValueError(
                "simulation_count must be nonnegative"
            )

        if self.runner_projection_count < 0:
            raise ValueError(
                "runner_projection_count must be nonnegative"
            )

        if self.validation_version != (
            CANONICAL_BASERUNNING_OUTPUT_VALIDATION_VERSION
        ):
            raise ValueError(
                "unsupported baserunning output "
                "validation version"
            )

    @property
    def ready(self) -> bool:
        return self.status == "ready"

    @property
    def observed_activity(self) -> bool:
        return (
            self.stolen_base_mean_total > 0.0
            or self.caught_stealing_mean_total > 0.0
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.validation_version,
            "status": self.status,
            "ready": self.ready,
            "simulation_count": self.simulation_count,
            "catalog_digest": self.catalog_digest,
            "runner_projection_count": (
                self.runner_projection_count
            ),
            "stolen_base_mean_total": (
                self.stolen_base_mean_total
            ),
            "caught_stealing_mean_total": (
                self.caught_stealing_mean_total
            ),
            "observed_activity": self.observed_activity,
            "warnings": self.warnings,
            "error_message": self.error_message,
            "activation_permitted": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def _metric_summary(
    row: Mapping[str, Any],
    metric_name: str,
) -> Optional[Mapping[str, Any]]:
    metrics = row.get("metrics")

    if not isinstance(metrics, list):
        return None

    for metric in metrics:
        if (
            isinstance(metric, Mapping)
            and metric.get("name") == metric_name
            and isinstance(
                metric.get("summary"),
                Mapping,
            )
        ):
            return metric["summary"]

    return None


def validate_canonical_baserunning_shadow_outputs(
    execution: Any,
) -> CanonicalBaserunningOutputValidation:
    """
    Validate injected-catalog propagation through projection aggregation.

    This validates observability only. It does not approve calibration,
    canonical authority, or production activation.
    """

    if not isinstance(
        execution,
        CanonicalProductionShadowExecution,
    ):
        return CanonicalBaserunningOutputValidation(
            status="error",
            error_message=(
                "execution must be "
                "CanonicalProductionShadowExecution"
            ),
        )

    if not execution.executed or execution.material is None:
        return CanonicalBaserunningOutputValidation(
            status=(
                "error"
                if execution.status == "error"
                else "unavailable"
            ),
            simulation_count=execution.simulation_count,
            error_message=(
                execution.error_message
                or "canonical shadow execution is unavailable"
            ),
        )

    diagnostics = execution.to_diagnostics()
    catalog_digest = diagnostics.get(
        "baserunning_evidence_catalog_digest"
    )

    if not catalog_digest:
        return CanonicalBaserunningOutputValidation(
            status="unavailable",
            simulation_count=execution.simulation_count,
            error_message=(
                "baserunning evidence catalog was not injected"
            ),
        )

    payload = execution.material.canonical_payload
    batters = payload.get("batters")

    if not isinstance(batters, list):
        return CanonicalBaserunningOutputValidation(
            status="error",
            simulation_count=execution.simulation_count,
            catalog_digest=str(catalog_digest),
            error_message=(
                "canonical batter projections are unavailable"
            ),
        )

    runner_projection_count = 0
    stolen_base_mean_total = 0.0
    caught_stealing_mean_total = 0.0

    for row in batters:
        if not isinstance(row, Mapping):
            return CanonicalBaserunningOutputValidation(
                status="error",
                simulation_count=execution.simulation_count,
                catalog_digest=str(catalog_digest),
                error_message=(
                    "canonical batter projection must be "
                    "a mapping"
                ),
            )

        stolen_bases = _metric_summary(
            row,
            "stolen_bases",
        )
        caught_stealing = _metric_summary(
            row,
            "caught_stealing",
        )

        if (
            stolen_bases is None
            or caught_stealing is None
        ):
            return CanonicalBaserunningOutputValidation(
                status="error",
                simulation_count=execution.simulation_count,
                catalog_digest=str(catalog_digest),
                runner_projection_count=(
                    runner_projection_count
                ),
                error_message=(
                    "batter projections must expose "
                    "stolen_bases and caught_stealing"
                ),
            )

        for summary in (
            stolen_bases,
            caught_stealing,
        ):
            if int(summary.get("count", -1)) != (
                execution.simulation_count
            ):
                return CanonicalBaserunningOutputValidation(
                    status="error",
                    simulation_count=(
                        execution.simulation_count
                    ),
                    catalog_digest=str(catalog_digest),
                    runner_projection_count=(
                        runner_projection_count
                    ),
                    error_message=(
                        "baserunning metric count must match "
                        "simulation_count"
                    ),
                )

        runner_projection_count += 1
        stolen_base_mean_total += float(
            stolen_bases.get("mean", 0.0)
        )
        caught_stealing_mean_total += float(
            caught_stealing.get("mean", 0.0)
        )

    if runner_projection_count == 0:
        return CanonicalBaserunningOutputValidation(
            status="error",
            simulation_count=execution.simulation_count,
            catalog_digest=str(catalog_digest),
            error_message=(
                "no batter baserunning projections were produced"
            ),
        )

    warnings = ()
    if (
        stolen_base_mean_total == 0.0
        and caught_stealing_mean_total == 0.0
    ):
        warnings = (
            "zero_baserunning_activity_observed",
        )

    return CanonicalBaserunningOutputValidation(
        status="ready",
        simulation_count=execution.simulation_count,
        catalog_digest=str(catalog_digest),
        runner_projection_count=(
            runner_projection_count
        ),
        stolen_base_mean_total=round(
            stolen_base_mean_total,
            6,
        ),
        caught_stealing_mean_total=round(
            caught_stealing_mean_total,
            6,
        ),
        warnings=warnings,
    )
