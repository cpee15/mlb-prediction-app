"""
Wind, field-orientation, and batted-ball vector diagnostic contract.

This module implements deterministic geometry and vector decomposition only.
It does not calculate aerodynamic carry, alter batted-ball outcomes, or modify
simulation state, parameters, or probabilities.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
import math
from typing import Any


def normalize_degrees(value: float) -> float:
    normalized = value % 360.0

    if math.isclose(
        normalized,
        360.0,
        abs_tol=1e-12,
    ):
        return 0.0

    return normalized


def meteorological_from_to_toward(
    wind_from_degrees: float,
) -> float:
    return normalize_degrees(
        wind_from_degrees + 180.0
    )


def bearing_unit_vector(
    bearing_degrees_true: float,
) -> tuple[float, float]:
    radians = math.radians(
        normalize_degrees(
            bearing_degrees_true
        )
    )

    east = math.sin(radians)
    north = math.cos(radians)

    return east, north


def vector_from_speed_and_bearing(
    speed: float,
    bearing_degrees_true: float,
) -> tuple[float, float]:
    east_unit, north_unit = (
        bearing_unit_vector(
            bearing_degrees_true
        )
    )

    return (
        speed * east_unit,
        speed * north_unit,
    )


def dot(
    first: tuple[float, float],
    second: tuple[float, float],
) -> float:
    return (
        first[0] * second[0]
        + first[1] * second[1]
    )


def right_perpendicular(
    vector: tuple[float, float],
) -> tuple[float, float]:
    east, north = vector

    return (
        north,
        -east,
    )


@dataclass(frozen=True)
class FieldOrientation:
    canonical_venue_id: str
    orientation_version: str
    home_plate_latitude: float | None
    home_plate_longitude: float | None
    center_field_bearing_degrees_true: float | None
    left_field_line_bearing_degrees_true: float | None
    right_field_line_bearing_degrees_true: float | None
    fair_territory_span_degrees: float | None
    orientation_source_name: str
    orientation_source_record_id: str | None
    retrieved_at_utc: datetime
    orientation_valid_from: date | None
    orientation_valid_through: date | None
    diagnostic_codes: tuple[str, ...]

    def validate(
        self,
        game_date: date,
    ) -> tuple[str, ...]:
        errors: list[str] = []

        if not self.canonical_venue_id.strip():
            errors.append(
                "canonical_venue_id_nonempty"
            )

        if not self.orientation_version.strip():
            errors.append(
                "orientation_version_present"
            )

        if not self.orientation_source_name.strip():
            errors.append(
                "orientation_source_name_present"
            )

        for name, value in [
            (
                "center_field_bearing_degrees_true",
                self.center_field_bearing_degrees_true,
            ),
            (
                "left_field_line_bearing_degrees_true",
                self.left_field_line_bearing_degrees_true,
            ),
            (
                "right_field_line_bearing_degrees_true",
                self.right_field_line_bearing_degrees_true,
            ),
        ]:
            if (
                value is None
                or not math.isfinite(value)
                or not (
                    0.0
                    <= value
                    <= 360.0
                )
            ):
                errors.append(
                    f"{name}_in_range"
                )

        if (
            self.fair_territory_span_degrees
            is None
            or not math.isfinite(
                self.fair_territory_span_degrees
            )
            or not (
                60.0
                <= self.fair_territory_span_degrees
                <= 120.0
            )
        ):
            errors.append(
                "fair_territory_span_physically_valid"
            )

        if (
            self.orientation_valid_from is not None
            and self.orientation_valid_through is not None
            and self.orientation_valid_from
            > self.orientation_valid_through
        ):
            errors.append(
                "orientation_date_range_valid"
            )

        if (
            self.orientation_valid_from is not None
            and game_date
            < self.orientation_valid_from
        ):
            errors.append(
                "orientation_not_yet_valid"
            )

        if (
            self.orientation_valid_through is not None
            and game_date
            > self.orientation_valid_through
        ):
            errors.append(
                "orientation_expired"
            )

        return tuple(
            sorted(
                set(errors)
            )
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)

        payload["retrieved_at_utc"] = (
            self.retrieved_at_utc.isoformat()
        )
        payload["orientation_valid_from"] = (
            self.orientation_valid_from.isoformat()
            if self.orientation_valid_from
            else None
        )
        payload["orientation_valid_through"] = (
            self.orientation_valid_through.isoformat()
            if self.orientation_valid_through
            else None
        )
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )

        return payload


@dataclass(frozen=True)
class WindVectorResolution:
    meteorological_wind_from_degrees: float | None
    wind_toward_degrees_true: float | None
    wind_speed_mps: float
    wind_east_mps: float
    wind_north_mps: float
    wind_outfield_mps: float | None
    wind_crossfield_mps: float | None
    batted_ball_spray_angle_degrees: float | None
    batted_ball_bearing_degrees_true: float | None
    wind_along_ball_path_mps: float | None
    wind_across_ball_path_mps: float | None
    vector_resolution_status: str
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    production_authority: bool = False
    simulation_inputs_changed: bool = False
    canonical_probability_authority_changed: bool = False
    aerodynamic_carry_calculated: bool = False
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


def _unavailable_resolution(
    *,
    diagnostic_code: str,
    validation_errors: tuple[str, ...] = (),
) -> WindVectorResolution:
    return WindVectorResolution(
        meteorological_wind_from_degrees=None,
        wind_toward_degrees_true=None,
        wind_speed_mps=0.0,
        wind_east_mps=0.0,
        wind_north_mps=0.0,
        wind_outfield_mps=None,
        wind_crossfield_mps=None,
        batted_ball_spray_angle_degrees=None,
        batted_ball_bearing_degrees_true=None,
        wind_along_ball_path_mps=None,
        wind_across_ball_path_mps=None,
        vector_resolution_status="unavailable",
        diagnostic_codes=(
            diagnostic_code,
        ),
        validation_errors=validation_errors,
    )


def _neutral_resolution(
    *,
    diagnostic_code: str,
    spray_angle_degrees: float | None,
    center_field_bearing_degrees_true: float | None,
    validation_errors: tuple[str, ...] = (),
) -> WindVectorResolution:
    ball_bearing = None

    if (
        spray_angle_degrees is not None
        and center_field_bearing_degrees_true
        is not None
    ):
        ball_bearing = normalize_degrees(
            center_field_bearing_degrees_true
            + spray_angle_degrees
        )

    return WindVectorResolution(
        meteorological_wind_from_degrees=None,
        wind_toward_degrees_true=None,
        wind_speed_mps=0.0,
        wind_east_mps=0.0,
        wind_north_mps=0.0,
        wind_outfield_mps=0.0,
        wind_crossfield_mps=0.0,
        batted_ball_spray_angle_degrees=(
            spray_angle_degrees
        ),
        batted_ball_bearing_degrees_true=(
            ball_bearing
        ),
        wind_along_ball_path_mps=0.0,
        wind_across_ball_path_mps=0.0,
        vector_resolution_status="neutral",
        diagnostic_codes=(
            diagnostic_code,
        ),
        validation_errors=validation_errors,
    )


def resolve_wind_field_vector(
    *,
    orientation: FieldOrientation | None,
    game_date: date,
    meteorological_wind_from_degrees: float | None,
    wind_speed_mps: float | None,
    batted_ball_spray_angle_degrees: float | None,
    indoor_effective: bool,
) -> WindVectorResolution:
    if orientation is None:
        return _unavailable_resolution(
            diagnostic_code=(
                "field_orientation_missing_vector_unavailable"
            )
        )

    orientation_errors = (
        orientation.validate(
            game_date
        )
    )

    if orientation_errors:
        return _unavailable_resolution(
            diagnostic_code=(
                "field_orientation_invalid_vector_unavailable"
            ),
            validation_errors=(
                orientation_errors
            ),
        )

    center_field_bearing = (
        orientation.center_field_bearing_degrees_true
    )

    if indoor_effective:
        return _neutral_resolution(
            diagnostic_code=(
                "indoor_environment_zero_wind_vector"
            ),
            spray_angle_degrees=(
                batted_ball_spray_angle_degrees
            ),
            center_field_bearing_degrees_true=(
                center_field_bearing
            ),
        )

    if (
        meteorological_wind_from_degrees
        is None
        or wind_speed_mps is None
    ):
        return _neutral_resolution(
            diagnostic_code=(
                "wind_missing_neutral_vector"
            ),
            spray_angle_degrees=(
                batted_ball_spray_angle_degrees
            ),
            center_field_bearing_degrees_true=(
                center_field_bearing
            ),
        )

    wind_errors: list[str] = []

    if (
        not math.isfinite(
            meteorological_wind_from_degrees
        )
        or not (
            0.0
            <= meteorological_wind_from_degrees
            <= 360.0
        )
    ):
        wind_errors.append(
            "wind_direction_in_range"
        )

    if (
        not math.isfinite(
            wind_speed_mps
        )
        or wind_speed_mps < 0.0
    ):
        wind_errors.append(
            "wind_speed_finite_and_nonnegative"
        )

    if (
        batted_ball_spray_angle_degrees
        is not None
        and (
            not math.isfinite(
                batted_ball_spray_angle_degrees
            )
            or not (
                -90.0
                <= batted_ball_spray_angle_degrees
                <= 90.0
            )
        )
    ):
        wind_errors.append(
            "spray_angle_within_supported_range"
        )

    if wind_errors:
        return _neutral_resolution(
            diagnostic_code=(
                "wind_invalid_neutral_vector"
            ),
            spray_angle_degrees=(
                None
                if (
                    "spray_angle_within_supported_range"
                    in wind_errors
                )
                else batted_ball_spray_angle_degrees
            ),
            center_field_bearing_degrees_true=(
                center_field_bearing
            ),
            validation_errors=tuple(
                sorted(
                    set(wind_errors)
                )
            ),
        )

    toward_bearing = (
        meteorological_from_to_toward(
            meteorological_wind_from_degrees
        )
    )

    wind_vector = (
        vector_from_speed_and_bearing(
            wind_speed_mps,
            toward_bearing,
        )
    )

    center_unit = bearing_unit_vector(
        center_field_bearing
    )

    right_unit = right_perpendicular(
        center_unit
    )

    outfield_component = dot(
        wind_vector,
        center_unit,
    )

    crossfield_component = dot(
        wind_vector,
        right_unit,
    )

    ball_bearing = None
    along_component = None
    across_component = None

    if (
        batted_ball_spray_angle_degrees
        is not None
    ):
        ball_bearing = normalize_degrees(
            center_field_bearing
            + batted_ball_spray_angle_degrees
        )

        ball_unit = bearing_unit_vector(
            ball_bearing
        )

        ball_right_unit = (
            right_perpendicular(
                ball_unit
            )
        )

        along_component = dot(
            wind_vector,
            ball_unit,
        )

        across_component = dot(
            wind_vector,
            ball_right_unit,
        )

    components = [
        wind_vector[0],
        wind_vector[1],
        outfield_component,
        crossfield_component,
    ]

    if along_component is not None:
        components.append(
            along_component
        )

    if across_component is not None:
        components.append(
            across_component
        )

    if not all(
        math.isfinite(value)
        for value in components
    ):
        return _neutral_resolution(
            diagnostic_code=(
                "wind_invalid_neutral_vector"
            ),
            spray_angle_degrees=(
                batted_ball_spray_angle_degrees
            ),
            center_field_bearing_degrees_true=(
                center_field_bearing
            ),
            validation_errors=(
                "vector_components_finite",
            ),
        )

    return WindVectorResolution(
        meteorological_wind_from_degrees=(
            meteorological_wind_from_degrees
        ),
        wind_toward_degrees_true=(
            toward_bearing
        ),
        wind_speed_mps=wind_speed_mps,
        wind_east_mps=wind_vector[0],
        wind_north_mps=wind_vector[1],
        wind_outfield_mps=(
            outfield_component
        ),
        wind_crossfield_mps=(
            crossfield_component
        ),
        batted_ball_spray_angle_degrees=(
            batted_ball_spray_angle_degrees
        ),
        batted_ball_bearing_degrees_true=(
            ball_bearing
        ),
        wind_along_ball_path_mps=(
            along_component
        ),
        wind_across_ball_path_mps=(
            across_component
        ),
        vector_resolution_status="resolved",
        diagnostic_codes=(
            "wind_field_vector_resolved",
        ),
        validation_errors=(),
    )


def evaluate_wind_field_vector_diagnostic(
    *,
    enabled: bool,
    orientation: FieldOrientation | None,
    game_date: date,
    meteorological_wind_from_degrees: float | None,
    wind_speed_mps: float | None,
    batted_ball_spray_angle_degrees: float | None,
    indoor_effective: bool,
) -> dict[str, Any]:
    if not enabled:
        return {
            "enabled": False,
            "diagnostic_code": (
                "wind_field_vector_diagnostic_disabled"
            ),
            "production_authority": False,
            "simulation_inputs_changed": False,
            "canonical_probability_authority_changed": False,
            "aerodynamic_carry_calculated": False,
            "batted_ball_outcomes_changed": False,
        }

    resolution = resolve_wind_field_vector(
        orientation=orientation,
        game_date=game_date,
        meteorological_wind_from_degrees=(
            meteorological_wind_from_degrees
        ),
        wind_speed_mps=wind_speed_mps,
        batted_ball_spray_angle_degrees=(
            batted_ball_spray_angle_degrees
        ),
        indoor_effective=indoor_effective,
    )

    return {
        "enabled": True,
        "wind_field_vector_resolution": (
            resolution.to_dict()
        ),
        "production_authority": False,
        "simulation_inputs_changed": False,
        "canonical_probability_authority_changed": False,
        "aerodynamic_carry_calculated": False,
        "batted_ball_outcomes_changed": False,
    }
