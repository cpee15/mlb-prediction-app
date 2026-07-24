"""Evaluate canonical baserunning calibration evidence."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from .baserunning_calibration_comparison import (
    CanonicalBaserunningCalibrationComparison,
)


CANONICAL_BASERUNNING_CALIBRATION_GATE_VERSION = (
    "canonical_baserunning_calibration_gate_v1"
)

_VALID_STATUSES = {
    "ready",
    "unavailable",
    "error",
}


def _validate_limit(
    value: float,
    field_name: str,
    *,
    bounded_rate: bool = False,
) -> None:
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(
            f"{field_name} must be nonnegative and finite"
        )

    if bounded_rate and value > 1.0:
        raise ValueError(
            f"{field_name} must not exceed one"
        )


@dataclass(frozen=True)
class CanonicalBaserunningCalibrationPolicy:
    minimum_game_count: int
    maximum_stolen_base_error_per_game: float
    maximum_caught_stealing_error_per_game: float
    maximum_attempt_error_per_game: float
    maximum_success_rate_absolute_error: float
    policy_version: str

    def __post_init__(self) -> None:
        if self.minimum_game_count <= 0:
            raise ValueError(
                "minimum_game_count must be positive"
            )

        _validate_limit(
            self.maximum_stolen_base_error_per_game,
            "maximum_stolen_base_error_per_game",
        )
        _validate_limit(
            self.maximum_caught_stealing_error_per_game,
            "maximum_caught_stealing_error_per_game",
        )
        _validate_limit(
            self.maximum_attempt_error_per_game,
            "maximum_attempt_error_per_game",
        )
        _validate_limit(
            self.maximum_success_rate_absolute_error,
            "maximum_success_rate_absolute_error",
            bounded_rate=True,
        )

        if (
            not isinstance(self.policy_version, str)
            or not self.policy_version.strip()
            or self.policy_version == "unavailable"
        ):
            raise ValueError(
                "policy_version must identify "
                "an available policy"
            )


@dataclass(frozen=True)
class CanonicalBaserunningCalibrationGate:
    status: str = "unavailable"
    eligible: bool = False
    calibration_gate_passed: bool = False
    game_count: int = 0
    stolen_base_error_per_game: float = 0.0
    caught_stealing_error_per_game: float = 0.0
    attempt_error_per_game: float = 0.0
    success_rate_absolute_error: Optional[float] = None
    failures: Tuple[str, ...] = ()
    policy_version: Optional[str] = None
    comparison_version: Optional[str] = None
    error_message: Optional[str] = None
    gate_version: str = (
        CANONICAL_BASERUNNING_CALIBRATION_GATE_VERSION
    )

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(
                "unsupported baserunning calibration gate status"
            )

        if self.game_count < 0:
            raise ValueError(
                "game_count must be nonnegative"
            )

        for field_name in (
            "stolen_base_error_per_game",
            "caught_stealing_error_per_game",
            "attempt_error_per_game",
        ):
            _validate_limit(
                float(getattr(self, field_name)),
                field_name,
            )

        if self.success_rate_absolute_error is not None:
            _validate_limit(
                self.success_rate_absolute_error,
                "success_rate_absolute_error",
                bounded_rate=True,
            )

        if self.calibration_gate_passed and (
            not self.eligible
            or self.status != "ready"
            or self.failures
        ):
            raise ValueError(
                "passing calibration gate must be "
                "ready, eligible, and failure-free"
            )

        if self.gate_version != (
            CANONICAL_BASERUNNING_CALIBRATION_GATE_VERSION
        ):
            raise ValueError(
                "unsupported baserunning calibration gate version"
            )

    @property
    def ready(self) -> bool:
        return self.status == "ready"

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.gate_version,
            "status": self.status,
            "ready": self.ready,
            "eligible": self.eligible,
            "calibration_gate_passed": (
                self.calibration_gate_passed
            ),
            "game_count": self.game_count,
            "stolen_base_error_per_game": (
                self.stolen_base_error_per_game
            ),
            "caught_stealing_error_per_game": (
                self.caught_stealing_error_per_game
            ),
            "attempt_error_per_game": (
                self.attempt_error_per_game
            ),
            "success_rate_absolute_error": (
                self.success_rate_absolute_error
            ),
            "failures": self.failures,
            "policy_version": self.policy_version,
            "comparison_version": self.comparison_version,
            "error_message": self.error_message,
            "activation_permitted": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def evaluate_baserunning_calibration_gate(
    comparison: Any,
    policy: Any,
) -> CanonicalBaserunningCalibrationGate:
    """
    Evaluate descriptive calibration against an explicit policy.

    A passing result is calibration evidence only. Production activation
    remains prohibited by this contract.
    """

    if not isinstance(
        comparison,
        CanonicalBaserunningCalibrationComparison,
    ):
        return CanonicalBaserunningCalibrationGate(
            status="error",
            error_message=(
                "comparison must be "
                "CanonicalBaserunningCalibrationComparison"
            ),
        )

    if not isinstance(
        policy,
        CanonicalBaserunningCalibrationPolicy,
    ):
        return CanonicalBaserunningCalibrationGate(
            status="error",
            comparison_version=(
                comparison.comparison_version
            ),
            error_message=(
                "policy must be "
                "CanonicalBaserunningCalibrationPolicy"
            ),
        )

    if not comparison.ready:
        return CanonicalBaserunningCalibrationGate(
            status=(
                "error"
                if comparison.status == "error"
                else "unavailable"
            ),
            game_count=comparison.game_count,
            policy_version=policy.policy_version,
            comparison_version=(
                comparison.comparison_version
            ),
            error_message=(
                comparison.error_message
                or "calibration comparison is unavailable"
            ),
        )

    game_count = comparison.game_count
    stolen_base_error_per_game = round(
        comparison.stolen_base_absolute_error
        / game_count,
        6,
    )
    caught_stealing_error_per_game = round(
        comparison.caught_stealing_absolute_error
        / game_count,
        6,
    )
    attempt_error_per_game = round(
        comparison.attempt_absolute_error
        / game_count,
        6,
    )

    failures = []

    if game_count < policy.minimum_game_count:
        failures.append("minimum_game_count_not_met")

    if stolen_base_error_per_game > (
        policy.maximum_stolen_base_error_per_game
    ):
        failures.append(
            "stolen_base_error_per_game_exceeded"
        )

    if caught_stealing_error_per_game > (
        policy.maximum_caught_stealing_error_per_game
    ):
        failures.append(
            "caught_stealing_error_per_game_exceeded"
        )

    if attempt_error_per_game > (
        policy.maximum_attempt_error_per_game
    ):
        failures.append(
            "attempt_error_per_game_exceeded"
        )

    success_rate_error = (
        comparison.success_rate_absolute_error
    )
    if success_rate_error is None:
        failures.append(
            "success_rate_error_unavailable"
        )
    elif success_rate_error > (
        policy.maximum_success_rate_absolute_error
    ):
        failures.append(
            "success_rate_absolute_error_exceeded"
        )

    eligible = (
        game_count >= policy.minimum_game_count
        and success_rate_error is not None
    )

    return CanonicalBaserunningCalibrationGate(
        status="ready",
        eligible=eligible,
        calibration_gate_passed=(
            eligible and not failures
        ),
        game_count=game_count,
        stolen_base_error_per_game=(
            stolen_base_error_per_game
        ),
        caught_stealing_error_per_game=(
            caught_stealing_error_per_game
        ),
        attempt_error_per_game=attempt_error_per_game,
        success_rate_absolute_error=success_rate_error,
        failures=tuple(failures),
        policy_version=policy.policy_version,
        comparison_version=(
            comparison.comparison_version
        ),
    )
