"""Assemble canonical baserunning calibration diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .baserunning_calibration_comparison import (
    CanonicalBaserunningCalibrationComparison,
    compare_baserunning_shadow_to_observed,
)
from .baserunning_calibration_gate import (
    CanonicalBaserunningCalibrationGate,
    evaluate_baserunning_calibration_gate,
)
from .baserunning_shadow_summary import (
    CanonicalBaserunningShadowSummary,
    summarize_canonical_baserunning_shadow_validations,
)


CANONICAL_BASERUNNING_CALIBRATION_REPORT_VERSION = (
    "canonical_baserunning_calibration_report_v1"
)

_VALID_STATUSES = {
    "ready",
    "unavailable",
    "error",
}


@dataclass(frozen=True)
class CanonicalBaserunningCalibrationReport:
    status: str = "unavailable"
    summary: Optional[
        CanonicalBaserunningShadowSummary
    ] = None
    comparison: Optional[
        CanonicalBaserunningCalibrationComparison
    ] = None
    gate: Optional[
        CanonicalBaserunningCalibrationGate
    ] = None
    error_message: Optional[str] = None
    report_version: str = (
        CANONICAL_BASERUNNING_CALIBRATION_REPORT_VERSION
    )

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(
                "unsupported baserunning calibration "
                "report status"
            )

        if self.report_version != (
            CANONICAL_BASERUNNING_CALIBRATION_REPORT_VERSION
        ):
            raise ValueError(
                "unsupported baserunning calibration "
                "report version"
            )

        if self.status == "ready" and (
            self.summary is None
            or self.comparison is None
            or self.gate is None
            or not self.summary.ready
            or not self.comparison.ready
            or not self.gate.ready
        ):
            raise ValueError(
                "ready calibration report requires "
                "ready summary, comparison, and gate"
            )

    @property
    def ready(self) -> bool:
        return self.status == "ready"

    @property
    def calibration_gate_passed(self) -> bool:
        return bool(
            self.gate is not None
            and self.gate.calibration_gate_passed
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.report_version,
            "status": self.status,
            "ready": self.ready,
            "calibration_gate_passed": (
                self.calibration_gate_passed
            ),
            "summary": (
                self.summary.to_diagnostics()
                if self.summary is not None
                else None
            ),
            "comparison": (
                self.comparison.to_diagnostics()
                if self.comparison is not None
                else None
            ),
            "gate": (
                self.gate.to_diagnostics()
                if self.gate is not None
                else None
            ),
            "error_message": self.error_message,
            "activation_permitted": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def assemble_baserunning_calibration_report(
    *,
    validations: Any,
    observed: Any,
    policy: Any,
) -> CanonicalBaserunningCalibrationReport:
    """
    Run the complete offline baserunning calibration pipeline.

    This packages diagnostic evidence only. It performs no fetching,
    persistence, simulator mutation, or production activation.
    """

    summary = (
        summarize_canonical_baserunning_shadow_validations(
            validations
        )
    )

    if not summary.ready:
        return CanonicalBaserunningCalibrationReport(
            status=summary.status,
            summary=summary,
            error_message=(
                summary.error_message
                or "baserunning shadow summary is unavailable"
            ),
        )

    comparison = compare_baserunning_shadow_to_observed(
        summary,
        observed,
    )

    if not comparison.ready:
        return CanonicalBaserunningCalibrationReport(
            status=comparison.status,
            summary=summary,
            comparison=comparison,
            error_message=(
                comparison.error_message
                or "baserunning comparison is unavailable"
            ),
        )

    gate = evaluate_baserunning_calibration_gate(
        comparison,
        policy,
    )

    if not gate.ready:
        return CanonicalBaserunningCalibrationReport(
            status=gate.status,
            summary=summary,
            comparison=comparison,
            gate=gate,
            error_message=(
                gate.error_message
                or "baserunning calibration gate is unavailable"
            ),
        )

    return CanonicalBaserunningCalibrationReport(
        status="ready",
        summary=summary,
        comparison=comparison,
        gate=gate,
    )
