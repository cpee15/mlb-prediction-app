"""
Roof, dome, weather, and atmospheric diagnostic contract.

This module provides deterministic, metadata-only resolution for:
- roof and indoor state;
- weather source precedence;
- observation and forecast freshness;
- physical-range validation;
- indoor neutralization;
- missing, invalid, stale, and secondary-source fallbacks.

It is not connected to production simulation authority and does not calculate
batted-ball carry or modify simulation probabilities.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import math
from typing import Any, Mapping, Sequence


SUPPORTED_ROOF_TYPES = {
    "open_air",
    "fixed_dome",
    "retractable",
    "unknown",
}

SUPPORTED_ROOF_STATES = {
    "open",
    "closed",
    "fixed_closed",
    "not_applicable",
    "unknown",
}

SUPPORTED_PRECIPITATION_STATES = {
    "none",
    "rain",
    "snow",
    "mixed",
    "other",
    "unknown",
}

OBSERVATION_SOURCE_CLASS = "confirmed_observation"
FORECAST_SOURCE_CLASS = "near_game_forecast"
SECONDARY_SOURCE_CLASS = "approved_secondary"
NEUTRAL_SOURCE_CLASS = "neutral_fallback"

SOURCE_CLASS_PRIORITY = {
    OBSERVATION_SOURCE_CLASS: 1,
    FORECAST_SOURCE_CLASS: 2,
    SECONDARY_SOURCE_CLASS: 3,
    NEUTRAL_SOURCE_CLASS: 4,
}


def _iso_datetime(
    value: datetime | None,
) -> str | None:
    return (
        value.isoformat()
        if value is not None
        else None
    )


def _finite_or_none(
    value: float | None,
) -> bool:
    return (
        value is None
        or math.isfinite(value)
    )


@dataclass(frozen=True)
class RoofStateResolution:
    roof_type: str
    roof_state: str
    valid: bool
    indoor_effective: bool
    weather_behavior: str
    diagnostic_codes: tuple[str, ...]
    production_authority: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        return payload


@dataclass(frozen=True)
class WeatherRecord:
    canonical_venue_id: str
    game_start_time_utc: datetime
    observation_time_utc: datetime | None
    retrieved_at_utc: datetime
    roof_type: str
    roof_state: str
    indoor_effective: bool
    temperature_c: float | None
    relative_humidity_pct: float | None
    dew_point_c: float | None
    station_pressure_hpa: float | None
    sea_level_pressure_hpa: float | None
    wind_speed_mps: float | None
    wind_gust_mps: float | None
    wind_direction_degrees: float | None
    precipitation_state: str
    precipitation_rate_mm_hr: float | None
    weather_source_name: str
    weather_source_record_id: str | None
    weather_source_class: str
    is_forecast: bool
    freshness_minutes: float | None
    fallback_used: bool
    diagnostic_codes: tuple[str, ...]

    def validate(
        self,
        expected_game_start_time_utc: datetime,
    ) -> tuple[str, ...]:
        errors: list[str] = []

        if not self.canonical_venue_id.strip():
            errors.append(
                "canonical_venue_id_nonempty"
            )

        if self.roof_type not in SUPPORTED_ROOF_TYPES:
            errors.append(
                "roof_type_supported"
            )

        roof_resolution = resolve_roof_state(
            self.roof_type,
            self.roof_state,
        )

        if not roof_resolution.valid:
            errors.append(
                "roof_state_compatible_with_roof_type"
            )

        if (
            self.game_start_time_utc
            != expected_game_start_time_utc
        ):
            errors.append(
                "game_start_time_matches_request"
            )

        if (
            self.weather_source_class
            not in SOURCE_CLASS_PRIORITY
        ):
            errors.append(
                "weather_source_class_supported"
            )

        if not self.weather_source_name.strip():
            errors.append(
                "weather_source_name_present"
            )

        if (
            self.weather_source_class
            != NEUTRAL_SOURCE_CLASS
            and self.observation_time_utc is None
        ):
            errors.append(
                "observation_time_required_for_nonfallback"
            )

        if (
            not self.is_forecast
            and self.observation_time_utc is not None
            and self.observation_time_utc
            > expected_game_start_time_utc
        ):
            errors.append(
                "future_observation_prohibited"
            )

        if (
            self.is_forecast
            and self.retrieved_at_utc
            > expected_game_start_time_utc
        ):
            errors.append(
                "forecast_issue_time_before_game"
            )

        if (
            not _finite_or_none(
                self.temperature_c
            )
            or (
                self.temperature_c is not None
                and not (
                    -90.0
                    <= self.temperature_c
                    <= 60.0
                )
            )
        ):
            errors.append(
                "temperature_within_physical_bounds"
            )

        if (
            not _finite_or_none(
                self.relative_humidity_pct
            )
            or (
                self.relative_humidity_pct
                is not None
                and not (
                    0.0
                    <= self.relative_humidity_pct
                    <= 100.0
                )
            )
        ):
            errors.append(
                "humidity_between_zero_and_one_hundred"
            )

        for name, value in [
            (
                "station_pressure_hpa",
                self.station_pressure_hpa,
            ),
            (
                "sea_level_pressure_hpa",
                self.sea_level_pressure_hpa,
            ),
        ]:
            if (
                not _finite_or_none(value)
                or (
                    value is not None
                    and value <= 0.0
                )
            ):
                errors.append(
                    f"{name}_finite_and_positive"
                )

        for name, value in [
            (
                "wind_speed_mps",
                self.wind_speed_mps,
            ),
            (
                "wind_gust_mps",
                self.wind_gust_mps,
            ),
        ]:
            if (
                not _finite_or_none(value)
                or (
                    value is not None
                    and value < 0.0
                )
            ):
                errors.append(
                    f"{name}_nonnegative"
                )

        if (
            not _finite_or_none(
                self.wind_direction_degrees
            )
            or (
                self.wind_direction_degrees
                is not None
                and not (
                    0.0
                    <= self.wind_direction_degrees
                    <= 360.0
                )
            )
        ):
            errors.append(
                "wind_direction_in_range"
            )

        if (
            self.precipitation_state
            not in SUPPORTED_PRECIPITATION_STATES
        ):
            errors.append(
                "precipitation_state_supported"
            )

        if (
            not _finite_or_none(
                self.precipitation_rate_mm_hr
            )
            or (
                self.precipitation_rate_mm_hr
                is not None
                and self.precipitation_rate_mm_hr
                < 0.0
            )
        ):
            errors.append(
                "precipitation_rate_nonnegative"
            )

        if (
            self.freshness_minutes is not None
            and (
                not math.isfinite(
                    self.freshness_minutes
                )
                or self.freshness_minutes < 0.0
            )
        ):
            errors.append(
                "freshness_minutes_nonnegative"
            )

        return tuple(
            sorted(
                set(errors)
            )
        )

    def calculated_freshness_minutes(
        self,
    ) -> float | None:
        if self.observation_time_utc is None:
            return None

        seconds = abs(
            (
                self.game_start_time_utc
                - self.observation_time_utc
            ).total_seconds()
        )

        return seconds / 60.0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)

        for field in [
            "game_start_time_utc",
            "observation_time_utc",
            "retrieved_at_utc",
        ]:
            payload[field] = _iso_datetime(
                getattr(self, field)
            )

        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )

        return payload


@dataclass(frozen=True)
class AtmosphericStateResolution:
    canonical_venue_id: str
    game_start_time_utc: datetime
    roof_type: str
    roof_state: str
    indoor_effective: bool
    temperature_c: float | None
    relative_humidity_pct: float | None
    dew_point_c: float | None
    station_pressure_hpa: float | None
    sea_level_pressure_hpa: float | None
    wind_speed_mps: float | None
    wind_gust_mps: float | None
    wind_direction_degrees: float | None
    precipitation_state: str
    precipitation_rate_mm_hr: float | None
    weather_source_name: str
    weather_source_record_id: str | None
    weather_source_class: str
    is_forecast: bool
    observation_time_utc: datetime | None
    retrieved_at_utc: datetime | None
    freshness_minutes: float | None
    stale: bool
    fallback_used: bool
    diagnostic_codes: tuple[str, ...]
    validation_errors: tuple[str, ...]
    provenance: Mapping[str, Any]
    production_authority: bool = False
    simulation_inputs_changed: bool = False
    canonical_probability_authority_changed: bool = False
    batted_ball_carry_calculated: bool = False

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)

        for field in [
            "game_start_time_utc",
            "observation_time_utc",
            "retrieved_at_utc",
        ]:
            payload[field] = _iso_datetime(
                getattr(self, field)
            )

        payload["diagnostic_codes"] = list(
            self.diagnostic_codes
        )
        payload["validation_errors"] = list(
            self.validation_errors
        )
        payload["provenance"] = dict(
            self.provenance
        )

        return payload


def resolve_roof_state(
    roof_type: str,
    roof_state: str,
) -> RoofStateResolution:
    valid = False
    indoor_effective = False
    behavior = (
        "roof_state_invalid"
    )
    diagnostic_codes: list[str] = []

    if (
        roof_type == "open_air"
        and roof_state == "not_applicable"
    ):
        valid = True
        behavior = (
            "outdoor_weather_retained"
        )
    elif (
        roof_type == "fixed_dome"
        and roof_state == "fixed_closed"
    ):
        valid = True
        indoor_effective = True
        behavior = (
            "outdoor_weather_neutralized"
        )
        diagnostic_codes.append(
            "indoor_environment_outdoor_weather_neutralized"
        )
    elif (
        roof_type == "retractable"
        and roof_state == "open"
    ):
        valid = True
        behavior = (
            "outdoor_weather_retained"
        )
    elif (
        roof_type == "retractable"
        and roof_state == "closed"
    ):
        valid = True
        indoor_effective = True
        behavior = (
            "outdoor_weather_neutralized"
        )
        diagnostic_codes.append(
            "indoor_environment_outdoor_weather_neutralized"
        )
    elif (
        roof_type == "retractable"
        and roof_state == "unknown"
    ):
        valid = True
        behavior = (
            "state_unknown_diagnostic_only"
        )
        diagnostic_codes.append(
            "roof_state_unknown_no_indoor_neutralization"
        )
    elif (
        roof_type == "unknown"
        and roof_state == "unknown"
    ):
        valid = True
        behavior = (
            "venue_roof_unknown_diagnostic_only"
        )
        diagnostic_codes.append(
            "roof_state_unknown_no_indoor_neutralization"
        )

    if roof_type not in SUPPORTED_ROOF_TYPES:
        diagnostic_codes.append(
            "roof_type_unsupported"
        )
    elif roof_state not in SUPPORTED_ROOF_STATES:
        diagnostic_codes.append(
            "roof_state_unsupported"
        )
    elif not valid:
        diagnostic_codes.append(
            "roof_state_incompatible_with_roof_type"
        )

    return RoofStateResolution(
        roof_type=roof_type,
        roof_state=roof_state,
        valid=valid,
        indoor_effective=indoor_effective,
        weather_behavior=behavior,
        diagnostic_codes=tuple(
            sorted(
                set(diagnostic_codes)
            )
        ),
    )


def _neutral_resolution(
    *,
    canonical_venue_id: str,
    game_start_time_utc: datetime,
    roof_resolution: RoofStateResolution,
    diagnostic_codes: tuple[str, ...],
    validation_errors: tuple[str, ...] = (),
) -> AtmosphericStateResolution:
    return AtmosphericStateResolution(
        canonical_venue_id=canonical_venue_id,
        game_start_time_utc=(
            game_start_time_utc
        ),
        roof_type=roof_resolution.roof_type,
        roof_state=roof_resolution.roof_state,
        indoor_effective=(
            roof_resolution.indoor_effective
        ),
        temperature_c=None,
        relative_humidity_pct=None,
        dew_point_c=None,
        station_pressure_hpa=None,
        sea_level_pressure_hpa=None,
        wind_speed_mps=None,
        wind_gust_mps=None,
        wind_direction_degrees=None,
        precipitation_state="unknown",
        precipitation_rate_mm_hr=None,
        weather_source_name=(
            "neutral_fallback"
        ),
        weather_source_record_id=None,
        weather_source_class=(
            NEUTRAL_SOURCE_CLASS
        ),
        is_forecast=False,
        observation_time_utc=None,
        retrieved_at_utc=None,
        freshness_minutes=None,
        stale=False,
        fallback_used=True,
        diagnostic_codes=tuple(
            sorted(
                set(
                    diagnostic_codes
                    + roof_resolution.diagnostic_codes
                )
            )
        ),
        validation_errors=validation_errors,
        provenance={
            "weather_source_name": (
                "neutral_fallback"
            ),
            "weather_source_record_id": None,
            "observation_time_utc": None,
            "retrieved_at_utc": None,
        },
    )


def resolve_atmospheric_state(
    *,
    canonical_venue_id: str,
    game_start_time_utc: datetime,
    roof_type: str,
    roof_state: str,
    records: Sequence[WeatherRecord],
    max_freshness_minutes: float = 180.0,
) -> AtmosphericStateResolution:
    roof_resolution = resolve_roof_state(
        roof_type,
        roof_state,
    )

    if not roof_resolution.valid:
        return _neutral_resolution(
            canonical_venue_id=(
                canonical_venue_id
            ),
            game_start_time_utc=(
                game_start_time_utc
            ),
            roof_resolution=roof_resolution,
            diagnostic_codes=(
                "roof_state_invalid_neutral_fallback",
            ),
            validation_errors=(
                "roof_state_compatible_with_roof_type",
            ),
        )

    if roof_resolution.indoor_effective:
        return _neutral_resolution(
            canonical_venue_id=(
                canonical_venue_id
            ),
            game_start_time_utc=(
                game_start_time_utc
            ),
            roof_resolution=roof_resolution,
            diagnostic_codes=(
                "indoor_environment_outdoor_weather_neutralized",
            ),
        )

    matching_records = [
        record
        for record in records
        if (
            record.canonical_venue_id
            == canonical_venue_id
        )
    ]

    valid_records: list[
        tuple[WeatherRecord, float]
    ] = []

    invalid_errors: list[str] = []

    for record in matching_records:
        errors = record.validate(
            game_start_time_utc
        )

        if errors:
            invalid_errors.extend(
                errors
            )
            continue

        freshness = (
            record.calculated_freshness_minutes()
        )

        if freshness is None:
            invalid_errors.append(
                "freshness_unavailable"
            )
            continue

        valid_records.append(
            (
                record,
                freshness,
            )
        )

    fresh_records = [
        (
            record,
            freshness,
        )
        for record, freshness
        in valid_records
        if freshness
        <= max_freshness_minutes
    ]

    fresh_records.sort(
        key=lambda item: (
            SOURCE_CLASS_PRIORITY[
                item[0].weather_source_class
            ],
            item[1],
            item[0].weather_source_name,
            (
                item[0].weather_source_record_id
                or ""
            ),
        )
    )

    if not fresh_records:
        if valid_records:
            return _neutral_resolution(
                canonical_venue_id=(
                    canonical_venue_id
                ),
                game_start_time_utc=(
                    game_start_time_utc
                ),
                roof_resolution=roof_resolution,
                diagnostic_codes=(
                    "weather_stale_neutral_fallback",
                ),
            )

        return _neutral_resolution(
            canonical_venue_id=(
                canonical_venue_id
            ),
            game_start_time_utc=(
                game_start_time_utc
            ),
            roof_resolution=roof_resolution,
            diagnostic_codes=(
                (
                    "weather_invalid_neutral_fallback"
                    if invalid_errors
                    else "weather_missing_neutral_fallback"
                ),
            ),
            validation_errors=tuple(
                sorted(
                    set(
                        invalid_errors
                    )
                )
            ),
        )

    selected, freshness = fresh_records[0]

    diagnostic_codes = list(
        roof_resolution.diagnostic_codes
    )

    fallback_used = False

    if (
        selected.weather_source_class
        == SECONDARY_SOURCE_CLASS
    ):
        fallback_used = True
        diagnostic_codes.append(
            "secondary_weather_source_fallback"
        )
    elif (
        selected.weather_source_class
        == FORECAST_SOURCE_CLASS
    ):
        fallback_used = True
        diagnostic_codes.append(
            "forecast_weather_source_selected"
        )
    else:
        diagnostic_codes.append(
            "confirmed_weather_observation_selected"
        )

    return AtmosphericStateResolution(
        canonical_venue_id=(
            canonical_venue_id
        ),
        game_start_time_utc=(
            game_start_time_utc
        ),
        roof_type=roof_type,
        roof_state=roof_state,
        indoor_effective=False,
        temperature_c=selected.temperature_c,
        relative_humidity_pct=(
            selected.relative_humidity_pct
        ),
        dew_point_c=selected.dew_point_c,
        station_pressure_hpa=(
            selected.station_pressure_hpa
        ),
        sea_level_pressure_hpa=(
            selected.sea_level_pressure_hpa
        ),
        wind_speed_mps=(
            selected.wind_speed_mps
        ),
        wind_gust_mps=(
            selected.wind_gust_mps
        ),
        wind_direction_degrees=(
            selected.wind_direction_degrees
        ),
        precipitation_state=(
            selected.precipitation_state
        ),
        precipitation_rate_mm_hr=(
            selected.precipitation_rate_mm_hr
        ),
        weather_source_name=(
            selected.weather_source_name
        ),
        weather_source_record_id=(
            selected.weather_source_record_id
        ),
        weather_source_class=(
            selected.weather_source_class
        ),
        is_forecast=selected.is_forecast,
        observation_time_utc=(
            selected.observation_time_utc
        ),
        retrieved_at_utc=(
            selected.retrieved_at_utc
        ),
        freshness_minutes=freshness,
        stale=False,
        fallback_used=fallback_used,
        diagnostic_codes=tuple(
            sorted(
                set(
                    diagnostic_codes
                )
            )
        ),
        validation_errors=(),
        provenance={
            "weather_source_name": (
                selected.weather_source_name
            ),
            "weather_source_record_id": (
                selected.weather_source_record_id
            ),
            "weather_source_class": (
                selected.weather_source_class
            ),
            "observation_time_utc": (
                _iso_datetime(
                    selected.observation_time_utc
                )
            ),
            "retrieved_at_utc": (
                _iso_datetime(
                    selected.retrieved_at_utc
                )
            ),
        },
    )


def evaluate_weather_atmospheric_diagnostic(
    *,
    enabled: bool,
    canonical_venue_id: str,
    game_start_time_utc: datetime,
    roof_type: str,
    roof_state: str,
    records: Sequence[WeatherRecord],
    max_freshness_minutes: float = 180.0,
) -> dict[str, Any]:
    if not enabled:
        return {
            "enabled": False,
            "diagnostic_code": (
                "weather_atmospheric_diagnostic_disabled"
            ),
            "production_authority": False,
            "simulation_inputs_changed": False,
            "canonical_probability_authority_changed": False,
            "batted_ball_carry_calculated": False,
        }

    resolution = resolve_atmospheric_state(
        canonical_venue_id=(
            canonical_venue_id
        ),
        game_start_time_utc=(
            game_start_time_utc
        ),
        roof_type=roof_type,
        roof_state=roof_state,
        records=records,
        max_freshness_minutes=(
            max_freshness_minutes
        ),
    )

    return {
        "enabled": True,
        "atmospheric_state_resolution": (
            resolution.to_dict()
        ),
        "production_authority": False,
        "simulation_inputs_changed": False,
        "canonical_probability_authority_changed": False,
        "batted_ball_carry_calculated": False,
    }
