"""Execute an immutable baserunning calibration artifact."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Mapping, Optional

from .baserunning_calibration_comparison import (
    CanonicalObservedBaserunningTotals,
)
from .baserunning_calibration_gate import (
    CanonicalBaserunningCalibrationPolicy,
)
from .baserunning_calibration_report import (
    CanonicalBaserunningCalibrationReport,
    assemble_baserunning_calibration_report,
)
from .baserunning_output_validation import (
    CanonicalBaserunningOutputValidation,
)


CANONICAL_BASERUNNING_CALIBRATION_INPUT_VERSION = (
    "canonical_baserunning_calibration_input_v1"
)
CANONICAL_BASERUNNING_CALIBRATION_ARTIFACT_VERSION = (
    "canonical_baserunning_calibration_artifact_v1"
)

_VALID_STATUSES = {
    "ready",
    "unavailable",
    "error",
}


@dataclass(frozen=True)
class CanonicalBaserunningCalibrationArtifact:
    status: str = "unavailable"
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    input_version: Optional[str] = None
    report: Optional[
        CanonicalBaserunningCalibrationReport
    ] = None
    error_message: Optional[str] = None
    artifact_version: str = (
        CANONICAL_BASERUNNING_CALIBRATION_ARTIFACT_VERSION
    )

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUSES:
            raise ValueError(
                "unsupported baserunning calibration "
                "artifact status"
            )

        if self.status == "ready" and (
            self.report is None
            or not self.report.ready
        ):
            raise ValueError(
                "ready artifact requires ready report"
            )

        if self.artifact_version != (
            CANONICAL_BASERUNNING_CALIBRATION_ARTIFACT_VERSION
        ):
            raise ValueError(
                "unsupported baserunning calibration "
                "artifact version"
            )

    @property
    def ready(self) -> bool:
        return self.status == "ready"

    @property
    def calibration_gate_passed(self) -> bool:
        return bool(
            self.report is not None
            and self.report.calibration_gate_passed
        )

    def to_diagnostics(self) -> Dict[str, Any]:
        return {
            "schema_version": self.artifact_version,
            "status": self.status,
            "ready": self.ready,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "input_version": self.input_version,
            "calibration_gate_passed": (
                self.calibration_gate_passed
            ),
            "report": (
                self.report.to_diagnostics()
                if self.report is not None
                else None
            ),
            "error_message": self.error_message,
            "external_fetch_performed": False,
            "persistence_performed": False,
            "activation_permitted": False,
            "production_authority_changed": False,
            "authoritative_source": "legacy",
        }


def _mapping(
    value: Any,
    field_name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(
            f"{field_name} must be a mapping"
        )
    return value


def _window_dates(
    payload: Mapping[str, Any],
) -> tuple[str, str]:
    window = _mapping(
        payload.get("window"),
        "window",
    )
    start_value = window.get("start_date")
    end_value = window.get("end_date")

    if not isinstance(start_value, str):
        raise TypeError(
            "window.start_date must be a string"
        )
    if not isinstance(end_value, str):
        raise TypeError(
            "window.end_date must be a string"
        )

    start_date = date.fromisoformat(start_value)
    end_date = date.fromisoformat(end_value)

    if end_date < start_date:
        raise ValueError(
            "window.end_date must not precede "
            "window.start_date"
        )

    return (
        start_date.isoformat(),
        end_date.isoformat(),
    )


def _validations(
    payload: Mapping[str, Any],
) -> tuple[
    CanonicalBaserunningOutputValidation,
    ...,
]:
    rows = payload.get("validations")

    if not isinstance(rows, list):
        raise TypeError(
            "validations must be a list"
        )

    values = []
    for row in rows:
        item = _mapping(
            row,
            "validation",
        )
        warnings = item.get("warnings", [])

        if not isinstance(warnings, list):
            raise TypeError(
                "validation.warnings must be a list"
            )

        values.append(
            CanonicalBaserunningOutputValidation(
                status=str(
                    item.get("status", "unavailable")
                ),
                simulation_count=int(
                    item.get("simulation_count", 0)
                ),
                catalog_digest=item.get(
                    "catalog_digest"
                ),
                runner_projection_count=int(
                    item.get(
                        "runner_projection_count",
                        0,
                    )
                ),
                stolen_base_mean_total=float(
                    item.get(
                        "stolen_base_mean_total",
                        0.0,
                    )
                ),
                caught_stealing_mean_total=float(
                    item.get(
                        "caught_stealing_mean_total",
                        0.0,
                    )
                ),
                warnings=tuple(
                    str(value)
                    for value in warnings
                ),
                error_message=item.get(
                    "error_message"
                ),
            )
        )

    return tuple(values)


def _observed(
    payload: Mapping[str, Any],
) -> CanonicalObservedBaserunningTotals:
    value = _mapping(
        payload.get("observed"),
        "observed",
    )

    return CanonicalObservedBaserunningTotals(
        game_count=int(value["game_count"]),
        stolen_bases=int(value["stolen_bases"]),
        caught_stealing=int(
            value["caught_stealing"]
        ),
        source_version=str(
            value["source_version"]
        ),
    )


def _policy(
    payload: Mapping[str, Any],
) -> CanonicalBaserunningCalibrationPolicy:
    value = _mapping(
        payload.get("policy"),
        "policy",
    )

    return CanonicalBaserunningCalibrationPolicy(
        minimum_game_count=int(
            value["minimum_game_count"]
        ),
        maximum_stolen_base_error_per_game=float(
            value[
                "maximum_stolen_base_error_per_game"
            ]
        ),
        maximum_caught_stealing_error_per_game=float(
            value[
                "maximum_caught_stealing_error_per_game"
            ]
        ),
        maximum_attempt_error_per_game=float(
            value[
                "maximum_attempt_error_per_game"
            ]
        ),
        maximum_success_rate_absolute_error=float(
            value[
                "maximum_success_rate_absolute_error"
            ]
        ),
        policy_version=str(
            value["policy_version"]
        ),
    )


def execute_baserunning_calibration_artifact(
    payload: Any,
) -> CanonicalBaserunningCalibrationArtifact:
    """
    Decode and execute one immutable offline calibration payload.

    No data is fetched or persisted. Production authority is unchanged.
    """

    window_start = None
    window_end = None
    input_version = None

    try:
        root = _mapping(payload, "payload")
        input_version = root.get("schema_version")

        if input_version != (
            CANONICAL_BASERUNNING_CALIBRATION_INPUT_VERSION
        ):
            raise ValueError(
                "unsupported baserunning calibration "
                "input version"
            )

        window_start, window_end = _window_dates(root)

        report = assemble_baserunning_calibration_report(
            validations=_validations(root),
            observed=_observed(root),
            policy=_policy(root),
        )
    except (
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        return CanonicalBaserunningCalibrationArtifact(
            status="error",
            window_start=window_start,
            window_end=window_end,
            input_version=(
                str(input_version)
                if input_version is not None
                else None
            ),
            error_message=str(exc),
        )

    return CanonicalBaserunningCalibrationArtifact(
        status=report.status,
        window_start=window_start,
        window_end=window_end,
        input_version=str(input_version),
        report=report,
        error_message=report.error_message,
    )
