"""
Environment diagnostic composition contract.

This module composes venue, weather, field-vector, and atmospheric-carry
diagnostic payloads into one deterministic metadata envelope.

It does not activate environment effects in production, alter simulation
inputs or probabilities, map carry diagnostics to distance, or change
batted-ball outcomes.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Callable, Mapping


COMPOSITION_VERSION = "7L-v1"

COMPONENT_STAGE_ORDER = (
    "venue_resolution",
    "weather_resolution",
    "field_vector_resolution",
    "carry_diagnostic_resolution",
)

FULL_STAGE_ORDER = (
    *COMPONENT_STAGE_ORDER,
    "composition_aggregation",
)

SUPPORTED_STAGE_STATUSES = {
    "resolved",
    "neutral",
    "unavailable",
    "invalid",
    "disabled",
}

StageProvider = Callable[[], Mapping[str, Any] | None]


@dataclass(frozen=True)
class EnvironmentCompositionInput:
    game_start_time_utc: datetime
    game_date: date
    canonical_venue_id: str | None
    composition_version: str = COMPOSITION_VERSION


@dataclass(frozen=True)
class EnvironmentDiagnosticEnvelope:
    enabled: bool
    composition_version: str
    canonical_venue_id: str | None
    game_start_time_utc: str
    game_date: str
    venue_resolution: dict[str, Any] | None
    weather_resolution: dict[str, Any] | None
    vector_resolution: dict[str, Any] | None
    carry_resolution: dict[str, Any] | None
    stage_statuses: dict[str, str]
    resolved_stage_count: int
    neutral_stage_count: int
    unavailable_stage_count: int
    invalid_stage_count: int
    composition_status: str
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    provenance: dict[str, Any]
    production_authority: bool = False
    simulation_inputs_changed: bool = False
    canonical_probability_authority_changed: bool = False
    production_environment_activated: bool = False
    batted_ball_distance_changed: bool = False
    batted_ball_outcomes_changed: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)

        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        payload["validation_errors"] = list(
            self.validation_errors
        )

        return payload


def _sorted_unique_strings(
    values: list[Any],
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                value
                for value in values
                if isinstance(value, str)
                and value
            }
        )
    )


def _extract_strings(
    payload: Mapping[str, Any] | None,
    key: str,
) -> list[str]:
    if payload is None:
        return []

    value = payload.get(key)

    if isinstance(value, str):
        return [value]

    if isinstance(value, (list, tuple, set)):
        return [
            item
            for item in value
            if isinstance(item, str)
        ]

    return []


def _infer_stage_status(
    stage_name: str,
    payload: Mapping[str, Any] | None,
) -> str:
    if payload is None:
        return "unavailable"

    candidate_keys = (
        "resolution_status",
        "vector_resolution_status",
        "status",
    )

    for key in candidate_keys:
        candidate = payload.get(key)

        if (
            isinstance(candidate, str)
            and candidate in SUPPORTED_STAGE_STATUSES
        ):
            return candidate

    if payload.get("enabled") is False:
        return "disabled"

    validation_errors = _extract_strings(
        payload,
        "validation_errors",
    )

    if validation_errors:
        return "invalid"

    if stage_name == "venue_resolution":
        if (
            payload.get("canonical_venue_id")
            or payload.get("venue_id")
        ):
            return "resolved"

    return "resolved"


def _extract_provenance(
    stage_name: str,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if payload is None:
        return {
            "stage": stage_name,
            "payload_present": False,
        }

    provenance = payload.get("provenance")

    if isinstance(provenance, Mapping):
        copied = deepcopy(
            dict(provenance)
        )
    else:
        copied = {}

    copied["stage"] = stage_name
    copied["payload_present"] = True

    return copied


def _validate_composition_input(
    composition_input: EnvironmentCompositionInput,
) -> tuple[str, ...]:
    errors: list[str] = []

    if not composition_input.composition_version.strip():
        errors.append(
            "composition_version_present"
        )

    if (
        composition_input.game_start_time_utc.date()
        != composition_input.game_date
    ):
        errors.append(
            "game_date_matches_game_start_time"
        )

    return _sorted_unique_strings(errors)


def _composition_status(
    component_statuses: tuple[str, ...],
    input_validation_errors: tuple[str, ...],
) -> str:
    if input_validation_errors:
        return "invalid"

    if all(
        status == "resolved"
        for status in component_statuses
    ):
        return "resolved"

    if all(
        status == "neutral"
        for status in component_statuses
    ):
        return "neutral"

    if all(
        status == "unavailable"
        for status in component_statuses
    ):
        return "unavailable"

    if (
        any(
            status == "resolved"
            for status in component_statuses
        )
        and any(
            status != "resolved"
            for status in component_statuses
        )
    ):
        return "partial"

    if all(
        status == "invalid"
        for status in component_statuses
    ):
        return "invalid"

    return "partial"


def _disabled_envelope(
    composition_input: EnvironmentCompositionInput,
) -> EnvironmentDiagnosticEnvelope:
    return EnvironmentDiagnosticEnvelope(
        enabled=False,
        composition_version=(
            composition_input.composition_version
        ),
        canonical_venue_id=(
            composition_input.canonical_venue_id
        ),
        game_start_time_utc=(
            composition_input.game_start_time_utc.isoformat()
        ),
        game_date=(
            composition_input.game_date.isoformat()
        ),
        venue_resolution=None,
        weather_resolution=None,
        vector_resolution=None,
        carry_resolution=None,
        stage_statuses={
            stage_name: "disabled"
            for stage_name in FULL_STAGE_ORDER
        },
        resolved_stage_count=0,
        neutral_stage_count=0,
        unavailable_stage_count=0,
        invalid_stage_count=0,
        composition_status="disabled",
        diagnostic_codes=(
            "environment_composition_diagnostic_disabled",
        ),
        validation_errors=(),
        provenance={
            "composition_stage_order": list(
                FULL_STAGE_ORDER
            ),
            "component_execution_skipped": True,
        },
    )


def compose_environment_diagnostics(
    *,
    enabled: bool,
    composition_input: EnvironmentCompositionInput,
    stage_providers: Mapping[str, StageProvider],
) -> EnvironmentDiagnosticEnvelope:
    if not enabled:
        return _disabled_envelope(
            composition_input
        )

    input_validation_errors = (
        _validate_composition_input(
            composition_input
        )
    )

    stage_payloads: dict[
        str,
        dict[str, Any] | None,
    ] = {}

    stage_statuses: dict[str, str] = {}
    aggregated_codes: list[Any] = []
    aggregated_errors: list[Any] = list(
        input_validation_errors
    )
    provenance: dict[str, Any] = {
        "composition_stage_order": list(
            FULL_STAGE_ORDER
        ),
        "component_execution_skipped": False,
    }

    for stage_name in COMPONENT_STAGE_ORDER:
        provider = stage_providers.get(
            stage_name
        )

        if provider is None:
            stage_payloads[stage_name] = None
            stage_statuses[stage_name] = (
                "unavailable"
            )
            aggregated_codes.append(
                f"{stage_name}_provider_missing"
            )
            provenance[stage_name] = {
                "stage": stage_name,
                "payload_present": False,
                "provider_present": False,
            }
            continue

        try:
            raw_payload = provider()

            payload = (
                deepcopy(
                    dict(raw_payload)
                )
                if raw_payload is not None
                else None
            )

            stage_payloads[stage_name] = payload

            stage_status = _infer_stage_status(
                stage_name,
                payload,
            )

            stage_statuses[stage_name] = (
                stage_status
            )

            aggregated_codes.extend(
                _extract_strings(
                    payload,
                    "diagnostic_codes",
                )
            )
            aggregated_errors.extend(
                _extract_strings(
                    payload,
                    "validation_errors",
                )
            )

            provenance[stage_name] = (
                _extract_provenance(
                    stage_name,
                    payload,
                )
            )
            provenance[stage_name][
                "provider_present"
            ] = True

        except Exception as exc:
            stage_payloads[stage_name] = None
            stage_statuses[stage_name] = (
                "invalid"
            )

            aggregated_codes.extend(
                [
                    "environment_stage_exception_isolated",
                    f"{stage_name}_exception",
                ]
            )
            aggregated_errors.append(
                f"{stage_name}_exception:{type(exc).__name__}"
            )

            provenance[stage_name] = {
                "stage": stage_name,
                "payload_present": False,
                "provider_present": True,
                "exception_type": (
                    type(exc).__name__
                ),
            }

    component_statuses = tuple(
        stage_statuses[stage_name]
        for stage_name in COMPONENT_STAGE_ORDER
    )

    composition_status = _composition_status(
        component_statuses,
        input_validation_errors,
    )

    if composition_status == "partial":
        aggregated_codes.append(
            "environment_composition_partial"
        )

    if composition_status == "invalid":
        aggregation_status = "invalid"
    else:
        aggregation_status = "resolved"

    stage_statuses[
        "composition_aggregation"
    ] = aggregation_status

    stage_status_values = tuple(
        stage_statuses[stage_name]
        for stage_name in FULL_STAGE_ORDER
    )

    resolved_count = sum(
        status == "resolved"
        for status in stage_status_values
    )
    neutral_count = sum(
        status == "neutral"
        for status in stage_status_values
    )
    unavailable_count = sum(
        status == "unavailable"
        for status in stage_status_values
    )
    invalid_count = sum(
        status == "invalid"
        for status in stage_status_values
    )

    provenance[
        "composition_aggregation"
    ] = {
        "stage": "composition_aggregation",
        "payload_present": True,
        "component_stage_count": len(
            COMPONENT_STAGE_ORDER
        ),
        "composition_status": (
            composition_status
        ),
    }

    venue_payload = stage_payloads.get(
        "venue_resolution"
    )
    weather_payload = stage_payloads.get(
        "weather_resolution"
    )
    vector_payload = stage_payloads.get(
        "field_vector_resolution"
    )
    carry_payload = stage_payloads.get(
        "carry_diagnostic_resolution"
    )

    canonical_venue_id = (
        composition_input.canonical_venue_id
    )

    if (
        canonical_venue_id is None
        and venue_payload is not None
    ):
        venue_candidate = (
            venue_payload.get(
                "canonical_venue_id"
            )
            or venue_payload.get(
                "venue_id"
            )
        )

        if isinstance(
            venue_candidate,
            str,
        ):
            canonical_venue_id = (
                venue_candidate
            )

    return EnvironmentDiagnosticEnvelope(
        enabled=True,
        composition_version=(
            composition_input.composition_version
        ),
        canonical_venue_id=(
            canonical_venue_id
        ),
        game_start_time_utc=(
            composition_input.game_start_time_utc.isoformat()
        ),
        game_date=(
            composition_input.game_date.isoformat()
        ),
        venue_resolution=venue_payload,
        weather_resolution=weather_payload,
        vector_resolution=vector_payload,
        carry_resolution=carry_payload,
        stage_statuses={
            stage_name: stage_statuses[
                stage_name
            ]
            for stage_name in FULL_STAGE_ORDER
        },
        resolved_stage_count=(
            resolved_count
        ),
        neutral_stage_count=(
            neutral_count
        ),
        unavailable_stage_count=(
            unavailable_count
        ),
        invalid_stage_count=(
            invalid_count
        ),
        composition_status=(
            composition_status
        ),
        diagnostic_codes=(
            _sorted_unique_strings(
                aggregated_codes
            )
        ),
        validation_errors=(
            _sorted_unique_strings(
                aggregated_errors
            )
        ),
        provenance=deepcopy(provenance),
    )
