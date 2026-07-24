"""Compare baserunning shadow projections with observed outcomes."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .baserunning_shadow_summary import (
    CanonicalBaserunningShadowSummary,
)


CANONICAL_BASERUNNING_CALIBRATION_COMPARISON_VERSION = (
    "canonical_baserunning_calibration_comparison_v1"
)

_VALID_STATUSES = {
    "ready",
    "unavailable",
    "error",
}


def _require_nonnegative_finite(
    value: float,
    field_name: str,
) -> None:
    if (
        not math.isfinite(value)
        or value < 0.0
    ):
        raise ValueError(
            f"{field_name} must be nonnegative and finite"
        )


@dataclass(frozen=True)
class CanonicalObservedBaserunningTotals:
    game_count: int
    stolen_bases: int
    caught_stealing: int
    source_version: str

    def __post_init__(self) -> None:
        if self.game_count <= 0:
            raise ValueError(
                "game_count must be positive"
            )

        if self.stolen_bases < 0:
            raise ValueError(
                "stolen_bases must be nonnegative"
            )

        if self.caught_stealing < 0:
            raise ValueError(
                "caught_stealing must be nonnegative"
            )

        if (
            not isinstance(self.source_version, str)
            or not self.source_version.strip()
            or self.source_version == "unavailable"
        ):
            raise ValueError(
                "source_version must identify "
                "an available source"
            )

    @property
    def attempts(self) -> int:
        return self.stolen_bases + self.caught_stealing

    @property
    def success_rate(self) -> Optional[float]:
        if self.attempts == 0:
            return None

        return round(
            self.stolen_bases / self.attempts,
            6,
        )


@dataclass(frozen=True)
class CanonicalBaserunningCalibrationComparison:
    status: str = "unavailable"
    game_count: int = 0
    projected_stolen_bases: float = 0.0
    observed_stolen_bases: int = 0
    stolen_base_absolute_error: float = 0.0
    projected_caught_stealing: float = 0.0
    observed_caught_stealing: int = 0
    caught_stealing_absolute_error: float = 0.0
    projected_attempts: float = 0.0
    observed_attempts: int = 0
    attempt_absolute_error: float = 0.0
    projected_success_rate: Optional[float] = None
    observed_success_rate: Optional[float] = None
    success_rate_absolute_error: Optional[float] = None
    observed_source_version: Optional[str] = None
    error_message: Optional[str] = None
    comparison_version: str = (
        CANONICAL_BASERUNNING_CALIBRATION_COMPARISON_VERSION
    )

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(
                "unsupported baserunning calibration status"
            )

        if self.game_count < 0:
            raise ValueError(
                "game_count must be nonnegative"
            )

        for field_name in (
            "projected_stolen_bases",
            "stolen_base_absolute_error",
            "projected_caught_stealing",
            "caught_stealing_absolute_error",
            "projected_attempts",
            "attempt_absolute_error",
        ):
            _require_nonnegative_finite(
                float(getattr(self, field_name)),
                field_name,
            )

        if self.observed_stolen_bases < 0:
            raise ValueError(
                "observed_stolen_bases must be nonnegative"
            )

        if self.observed_caught_stealing < 0:
            raise ValueError(
                "observed_caught_stealing must be nonnegative"
            )

        if self.observed_attempts < 0:
            raise ValueError(
                "observed_attempts must be nonnegative"
            )

        for field_name in (
            "projected_success_rate",
            "observed_success_rate",
            "success_rate_absolute_error",
        ):
            value = getattr(self, field_name)
            if value is not None and (
                not math.isfinite(value)
                or value < 0.0
                or value > 1.0
            ):
                raise ValueError(
                    f"{field_name} must be between zero and one"
                )

        if self.comparison_version != (
            CANONICAL_BASERUNNING_CALIBRATION_COMPARISON_VERSION
        ):
            raise ValueError(
                "unsupported baserunning calibration "
                "comparison version"
            )

    @property
    def ready(self) -> bool:
        return self.status == "ready"

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.comparison_version,
            "status": self.status,
            "ready": self.ready,
            "game_count": self.game_count,
            "projected_stolen_bases": (
                self.projected_stolen_bases
            ),
            "observed_stolen_bases": (
                self.observed_stolen_bases
            ),
            "stolen_base_absolute_error": (
                self.stolen_base_absolute_error
            ),
            "projected_caught_stealing": (
                self.projected_caught_stealing
            ),
            "observed_caught_stealing": (
                self.observed_caught_stealing
            ),
            "caught_stealing_absolute_error": (
                self.caught_stealing_absolute_error
            ),
            "projected_attempts": self.projected_attempts,
            "observed_attempts": self.observed_attempts,
            "attempt_absolute_error": (
                self.attempt_absolute_error
            ),
            "projected_success_rate": (
                self.projected_success_rate
            ),
            "observed_success_rate": (
                self.observed_success_rate
            ),
            "success_rate_absolute_error": (
                self.success_rate_absolute_error
            ),
            "observed_source_version": (
                self.observed_source_version
            ),
            "error_message": self.error_message,
            "calibration_approved": False,
            "activation_permitted": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def compare_baserunning_shadow_to_observed(
    summary: Any,
    observed: Any,
) -> CanonicalBaserunningCalibrationComparison:
    """
    Compare aligned slate projections and outcomes.

    This computes descriptive errors only. It does not define passing
    thresholds or permit production activation.
    """

    if not isinstance(
        summary,
        CanonicalBaserunningShadowSummary,
    ):
        return CanonicalBaserunningCalibrationComparison(
            status="error",
            error_message=(
                "summary must be "
                "CanonicalBaserunningShadowSummary"
            ),
        )

    if not isinstance(
        observed,
        CanonicalObservedBaserunningTotals,
    ):
        return CanonicalBaserunningCalibrationComparison(
            status="error",
            error_message=(
                "observed must be "
                "CanonicalObservedBaserunningTotals"
            ),
        )

    if not summary.ready:
        return CanonicalBaserunningCalibrationComparison(
            status=(
                "error"
                if summary.status == "error"
                else "unavailable"
            ),
            error_message=(
                summary.error_message
                or "baserunning shadow summary is unavailable"
            ),
        )

    if observed.game_count != summary.ready_count:
        return CanonicalBaserunningCalibrationComparison(
            status="unavailable",
            game_count=summary.ready_count,
            observed_source_version=(
                observed.source_version
            ),
            error_message=(
                "observed game_count must match "
                "ready shadow validation count"
            ),
        )

    projected_stolen_bases = round(
        summary.stolen_base_mean_total,
        6,
    )
    projected_caught_stealing = round(
        summary.caught_stealing_mean_total,
        6,
    )
    projected_attempts = round(
        projected_stolen_bases
        + projected_caught_stealing,
        6,
    )

    projected_success_rate = None
    if projected_attempts > 0.0:
        projected_success_rate = round(
            projected_stolen_bases
            / projected_attempts,
            6,
        )

    observed_success_rate = observed.success_rate

    success_rate_absolute_error = None
    if (
        projected_success_rate is not None
        and observed_success_rate is not None
    ):
        success_rate_absolute_error = round(
            abs(
                projected_success_rate
                - observed_success_rate
            ),
            6,
        )

    return CanonicalBaserunningCalibrationComparison(
        status="ready",
        game_count=observed.game_count,
        projected_stolen_bases=projected_stolen_bases,
        observed_stolen_bases=observed.stolen_bases,
        stolen_base_absolute_error=round(
            abs(
                projected_stolen_bases
                - observed.stolen_bases
            ),
            6,
        ),
        projected_caught_stealing=(
            projected_caught_stealing
        ),
        observed_caught_stealing=(
            observed.caught_stealing
        ),
        caught_stealing_absolute_error=round(
            abs(
                projected_caught_stealing
                - observed.caught_stealing
            ),
            6,
        ),
        projected_attempts=projected_attempts,
        observed_attempts=observed.attempts,
        attempt_absolute_error=round(
            abs(
                projected_attempts
                - observed.attempts
            ),
            6,
        ),
        projected_success_rate=(
            projected_success_rate
        ),
        observed_success_rate=(
            observed_success_rate
        ),
        success_rate_absolute_error=(
            success_rate_absolute_error
        ),
        observed_source_version=(
            observed.source_version
        ),
    )
