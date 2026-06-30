"""
Atmospheric-density and carry diagnostic contract.

This module computes deterministic atmospheric metadata and a bounded,
dimensionless carry index. It does not map that index to distance, modify
batted-ball outcomes, or alter simulation probabilities.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import math
from typing import Any


DRY_AIR_GAS_CONSTANT_J_KG_K = 287.05
WATER_VAPOR_GAS_CONSTANT_J_KG_K = 461.495
REFERENCE_AIR_DENSITY_KG_M3 = 1.225
REFERENCE_TEMPERATURE_C = 15.0
REFERENCE_PRESSURE_HPA = 1013.25

DENSITY_INDEX_SCALE_KG_M3 = 0.20
WIND_INDEX_SCALE_MPS = 10.0
DENSITY_INDEX_BOUND = 1.0
WIND_INDEX_BOUND = 1.0
COMBINED_INDEX_BOUND = 1.0


@dataclass(frozen=True)
class AtmosphericCarryInput:
    temperature_c: float | None
    relative_humidity_pct: float | None
    dew_point_c: float | None
    station_pressure_hpa: float | None
    sea_level_pressure_hpa: float | None
    venue_elevation_m: float | None
    wind_along_ball_path_mps: float | None
    indoor_effective: bool
    weather_source_class: str | None
    weather_source_record_id: str | None
    observation_time_utc: datetime | None
    freshness_minutes: float | None

    def validate(self) -> tuple[str, ...]:
        errors: list[str] = []

        if (
            self.temperature_c is not None
            and (
                not math.isfinite(self.temperature_c)
                or not (-90.0 <= self.temperature_c <= 60.0)
            )
        ):
            errors.append(
                "temperature_finite_and_within_contract_range"
            )

        if (
            self.relative_humidity_pct is not None
            and (
                not math.isfinite(self.relative_humidity_pct)
                or not (0.0 <= self.relative_humidity_pct <= 100.0)
            )
        ):
            errors.append(
                "humidity_between_zero_and_one_hundred"
            )

        if (
            self.dew_point_c is not None
            and (
                not math.isfinite(self.dew_point_c)
                or (
                    self.temperature_c is not None
                    and self.dew_point_c > self.temperature_c + 1.0
                )
            )
        ):
            errors.append(
                "dew_point_not_materially_above_temperature"
            )

        if (
            self.station_pressure_hpa is not None
            and (
                not math.isfinite(self.station_pressure_hpa)
                or self.station_pressure_hpa <= 0.0
            )
        ):
            errors.append(
                "station_pressure_finite_and_positive"
            )

        if (
            self.sea_level_pressure_hpa is not None
            and (
                not math.isfinite(self.sea_level_pressure_hpa)
                or self.sea_level_pressure_hpa <= 0.0
            )
        ):
            errors.append(
                "sea_level_pressure_finite_and_positive"
            )

        if (
            self.wind_along_ball_path_mps is not None
            and not math.isfinite(
                self.wind_along_ball_path_mps
            )
        ):
            errors.append(
                "wind_component_finite"
            )

        if (
            self.freshness_minutes is not None
            and (
                not math.isfinite(self.freshness_minutes)
                or self.freshness_minutes < 0.0
            )
        ):
            errors.append(
                "freshness_minutes_nonnegative"
            )

        return tuple(sorted(set(errors)))


@dataclass(frozen=True)
class AtmosphericCarryResolution:
    temperature_k: float | None
    saturation_vapor_pressure_hpa: float | None
    actual_vapor_pressure_hpa: float | None
    dry_air_pressure_hpa: float | None
    air_density_kg_m3: float | None
    reference_air_density_kg_m3: float
    air_density_ratio: float | None
    density_delta_pct: float | None
    density_altitude_m: float | None
    wind_along_ball_path_mps: float | None
    density_component_index: float | None
    wind_component_index: float | None
    combined_carry_index: float | None
    resolution_status: str
    pressure_source: str
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    provenance: dict[str, Any]
    production_authority: bool = False
    simulation_inputs_changed: bool = False
    canonical_probability_authority_changed: bool = False
    production_carry_activated: bool = False
    batted_ball_distance_changed: bool = False
    batted_ball_outcomes_changed: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(self.diagnostic_codes)
        payload["validation_errors"] = list(self.validation_errors)
        payload["provenance"] = dict(self.provenance)
        return payload


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def saturation_vapor_pressure_hpa(
    temperature_c: float,
) -> float:
    return 6.112 * math.exp(
        (17.67 * temperature_c)
        / (temperature_c + 243.5)
    )


def moist_air_density_kg_m3(
    *,
    temperature_c: float,
    relative_humidity_pct: float,
    pressure_hpa: float,
) -> tuple[float, float, float, float]:
    temperature_k = temperature_c + 273.15
    saturation_hpa = saturation_vapor_pressure_hpa(
        temperature_c
    )
    vapor_hpa = (
        relative_humidity_pct / 100.0
    ) * saturation_hpa
    dry_hpa = pressure_hpa - vapor_hpa

    if dry_hpa <= 0.0:
        raise ValueError(
            "vapor_pressure_not_above_total_pressure"
        )

    dry_pa = dry_hpa * 100.0
    vapor_pa = vapor_hpa * 100.0

    density = (
        dry_pa
        / (
            DRY_AIR_GAS_CONSTANT_J_KG_K
            * temperature_k
        )
        + vapor_pa
        / (
            WATER_VAPOR_GAS_CONSTANT_J_KG_K
            * temperature_k
        )
    )

    return (
        temperature_k,
        saturation_hpa,
        vapor_hpa,
        density,
    )


def density_altitude_m(
    air_density_kg_m3: float,
) -> float:
    ratio = (
        air_density_kg_m3
        / REFERENCE_AIR_DENSITY_KG_M3
    )

    if ratio <= 0.0:
        raise ValueError(
            "air_density_finite_and_positive"
        )

    exponent = 1.0 / 4.25588

    return 44330.0 * (
        1.0 - ratio ** exponent
    )


def density_component_index(
    air_density_kg_m3: float,
) -> float:
    raw = (
        REFERENCE_AIR_DENSITY_KG_M3
        - air_density_kg_m3
    ) / DENSITY_INDEX_SCALE_KG_M3

    return clamp(
        raw,
        -DENSITY_INDEX_BOUND,
        DENSITY_INDEX_BOUND,
    )


def wind_component_index(
    wind_along_ball_path_mps: float,
) -> float:
    raw = (
        wind_along_ball_path_mps
        / WIND_INDEX_SCALE_MPS
    )

    return clamp(
        raw,
        -WIND_INDEX_BOUND,
        WIND_INDEX_BOUND,
    )


def combined_carry_index(
    density_index: float,
    wind_index: float,
) -> float:
    combined = (
        0.5 * density_index
        + 0.5 * wind_index
    )

    return clamp(
        combined,
        -COMBINED_INDEX_BOUND,
        COMBINED_INDEX_BOUND,
    )


def _neutral_resolution(
    *,
    diagnostic_codes: tuple[str, ...],
    validation_errors: tuple[str, ...] = (),
    pressure_source: str = "reference_pressure",
) -> AtmosphericCarryResolution:
    return AtmosphericCarryResolution(
        temperature_k=REFERENCE_TEMPERATURE_C + 273.15,
        saturation_vapor_pressure_hpa=None,
        actual_vapor_pressure_hpa=None,
        dry_air_pressure_hpa=None,
        air_density_kg_m3=REFERENCE_AIR_DENSITY_KG_M3,
        reference_air_density_kg_m3=REFERENCE_AIR_DENSITY_KG_M3,
        air_density_ratio=1.0,
        density_delta_pct=0.0,
        density_altitude_m=0.0,
        wind_along_ball_path_mps=0.0,
        density_component_index=0.0,
        wind_component_index=0.0,
        combined_carry_index=0.0,
        resolution_status="neutral",
        pressure_source=pressure_source,
        diagnostic_codes=tuple(
            sorted(set(diagnostic_codes))
        ),
        validation_errors=validation_errors,
        provenance={
            "pressure_source": pressure_source,
            "distance_mapping_applied": False,
        },
    )


def _unavailable_resolution(
    *,
    diagnostic_code: str,
) -> AtmosphericCarryResolution:
    return AtmosphericCarryResolution(
        temperature_k=None,
        saturation_vapor_pressure_hpa=None,
        actual_vapor_pressure_hpa=None,
        dry_air_pressure_hpa=None,
        air_density_kg_m3=None,
        reference_air_density_kg_m3=REFERENCE_AIR_DENSITY_KG_M3,
        air_density_ratio=None,
        density_delta_pct=None,
        density_altitude_m=None,
        wind_along_ball_path_mps=None,
        density_component_index=None,
        wind_component_index=None,
        combined_carry_index=None,
        resolution_status="unavailable",
        pressure_source="unavailable",
        diagnostic_codes=(diagnostic_code,),
        validation_errors=(),
        provenance={
            "pressure_source": "unavailable",
            "distance_mapping_applied": False,
        },
    )


def resolve_atmospheric_density_carry(
    atmospheric_input: AtmosphericCarryInput,
) -> AtmosphericCarryResolution:
    if atmospheric_input.indoor_effective:
        return _neutral_resolution(
            diagnostic_codes=(
                "indoor_environment_neutral_carry_index",
            )
        )

    if (
        atmospheric_input.temperature_c is None
        or atmospheric_input.relative_humidity_pct is None
    ):
        return _unavailable_resolution(
            diagnostic_code=(
                "atmospheric_inputs_missing_carry_unavailable"
            )
        )

    validation_errors = atmospheric_input.validate()

    if validation_errors:
        return _neutral_resolution(
            diagnostic_codes=(
                "atmospheric_inputs_invalid_neutral_carry",
            ),
            validation_errors=validation_errors,
        )

    pressure_hpa: float
    pressure_source: str
    diagnostic_codes: list[str] = []

    if atmospheric_input.station_pressure_hpa is not None:
        pressure_hpa = atmospheric_input.station_pressure_hpa
        pressure_source = "station_pressure_hpa"
    elif atmospheric_input.sea_level_pressure_hpa is not None:
        pressure_hpa = atmospheric_input.sea_level_pressure_hpa
        pressure_source = "sea_level_pressure_hpa"
        diagnostic_codes.append(
            "sea_level_pressure_fallback_selected"
        )
    else:
        pressure_hpa = REFERENCE_PRESSURE_HPA
        pressure_source = "reference_pressure"
        diagnostic_codes.append(
            "pressure_missing_reference_fallback"
        )

    wind_value = atmospheric_input.wind_along_ball_path_mps

    if wind_value is None:
        wind_value = 0.0
        diagnostic_codes.append(
            "along_path_wind_missing_zero_component"
        )
    elif not math.isfinite(wind_value):
        wind_value = 0.0
        diagnostic_codes.append(
            "along_path_wind_invalid_zero_component"
        )

    try:
        (
            temperature_k,
            saturation_hpa,
            vapor_hpa,
            density,
        ) = moist_air_density_kg_m3(
            temperature_c=atmospheric_input.temperature_c,
            relative_humidity_pct=(
                atmospheric_input.relative_humidity_pct
            ),
            pressure_hpa=pressure_hpa,
        )
    except (ValueError, OverflowError):
        return _neutral_resolution(
            diagnostic_codes=(
                "computed_density_invalid_neutral_carry",
            ),
            pressure_source=pressure_source,
        )

    dry_hpa = pressure_hpa - vapor_hpa

    if (
        not math.isfinite(density)
        or density <= 0.0
    ):
        return _neutral_resolution(
            diagnostic_codes=(
                "computed_density_invalid_neutral_carry",
            ),
            validation_errors=(
                "air_density_finite_and_positive",
            ),
            pressure_source=pressure_source,
        )

    density_ratio = (
        density
        / REFERENCE_AIR_DENSITY_KG_M3
    )

    if not (0.5 <= density_ratio <= 1.5):
        return _neutral_resolution(
            diagnostic_codes=(
                "computed_density_invalid_neutral_carry",
            ),
            validation_errors=(
                "density_ratio_within_diagnostic_bounds",
            ),
            pressure_source=pressure_source,
        )

    density_delta = (
        density_ratio - 1.0
    ) * 100.0

    density_altitude = density_altitude_m(
        density
    )

    density_index = density_component_index(
        density
    )
    wind_index = wind_component_index(
        wind_value
    )
    carry_index = combined_carry_index(
        density_index,
        wind_index,
    )

    if not all(
        math.isfinite(value)
        for value in [
            temperature_k,
            saturation_hpa,
            vapor_hpa,
            dry_hpa,
            density,
            density_ratio,
            density_delta,
            density_altitude,
            density_index,
            wind_index,
            carry_index,
        ]
    ):
        return _neutral_resolution(
            diagnostic_codes=(
                "computed_density_invalid_neutral_carry",
            ),
            validation_errors=(
                "carry_index_bounded",
            ),
            pressure_source=pressure_source,
        )

    diagnostic_codes.append(
        "atmospheric_density_carry_diagnostic_resolved"
    )

    return AtmosphericCarryResolution(
        temperature_k=temperature_k,
        saturation_vapor_pressure_hpa=saturation_hpa,
        actual_vapor_pressure_hpa=vapor_hpa,
        dry_air_pressure_hpa=dry_hpa,
        air_density_kg_m3=density,
        reference_air_density_kg_m3=REFERENCE_AIR_DENSITY_KG_M3,
        air_density_ratio=density_ratio,
        density_delta_pct=density_delta,
        density_altitude_m=density_altitude,
        wind_along_ball_path_mps=wind_value,
        density_component_index=density_index,
        wind_component_index=wind_index,
        combined_carry_index=carry_index,
        resolution_status="resolved",
        pressure_source=pressure_source,
        diagnostic_codes=tuple(
            sorted(set(diagnostic_codes))
        ),
        validation_errors=(),
        provenance={
            "weather_source_class": (
                atmospheric_input.weather_source_class
            ),
            "weather_source_record_id": (
                atmospheric_input.weather_source_record_id
            ),
            "observation_time_utc": (
                atmospheric_input.observation_time_utc.isoformat()
                if atmospheric_input.observation_time_utc
                else None
            ),
            "freshness_minutes": (
                atmospheric_input.freshness_minutes
            ),
            "pressure_source": pressure_source,
            "distance_mapping_applied": False,
        },
    )


def evaluate_atmospheric_density_carry_diagnostic(
    *,
    enabled: bool,
    atmospheric_input: AtmosphericCarryInput,
) -> dict[str, Any]:
    if not enabled:
        return {
            "enabled": False,
            "diagnostic_code": (
                "atmospheric_density_carry_diagnostic_disabled"
            ),
            "production_authority": False,
            "simulation_inputs_changed": False,
            "canonical_probability_authority_changed": False,
            "production_carry_activated": False,
            "batted_ball_distance_changed": False,
            "batted_ball_outcomes_changed": False,
        }

    resolution = resolve_atmospheric_density_carry(
        atmospheric_input
    )

    return {
        "enabled": True,
        "atmospheric_density_carry_resolution": (
            resolution.to_dict()
        ),
        "production_authority": False,
        "simulation_inputs_changed": False,
        "canonical_probability_authority_changed": False,
        "production_carry_activated": False,
        "batted_ball_distance_changed": False,
        "batted_ball_outcomes_changed": False,
    }
