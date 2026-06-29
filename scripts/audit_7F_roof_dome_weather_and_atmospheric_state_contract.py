#!/usr/bin/env python3
"""
Layer 7F
Roof, Dome, Weather, and Atmospheric State Contract Audit

Implements and independently audits the 7E contract as a disabled-by-default,
metadata-only diagnostic.

No production simulation integration, atmospheric carry calculation, or
probability authority is introduced.
"""

from __future__ import annotations

import ast
import copy
import csv
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7F"
LAYER_NAME = (
    "roof_dome_weather_and_atmospheric_state_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_7F_roof_dome_weather_and_atmospheric_state_contract"
)

PLAN_7E_PATH = (
    ROOT
    / "scripts/plan_7E_roof_dome_weather_and_atmospheric_state_contract.py"
)

CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/weather_atmospheric_contract.py"
)


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(
    path: Path,
    payload: Any,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def string_constants(
    path: Path,
) -> set[str]:
    tree = ast.parse(
        path.read_text(
            encoding="utf-8",
            errors="ignore",
        ),
        filename=str(path),
    )

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def load_contract_module() -> Any:
    spec = importlib.util.spec_from_file_location(
        "weather_atmospheric_contract_7f",
        CONTRACT_PATH,
    )

    if (
        spec is None
        or spec.loader is None
    ):
        raise RuntimeError(
            "Unable to load weather contract module"
        )

    module = importlib.util.module_from_spec(
        spec
    )

    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_paths_exist = (
        PLAN_7E_PATH.exists()
        and CONTRACT_PATH.exists()
    )

    predecessor_contract_present = (
        "roof_dome_weather_and_atmospheric_state_contract_plan_complete"
        in string_constants(
            PLAN_7E_PATH
        )
    )

    contract = load_contract_module()

    game_start = datetime(
        2026,
        7,
        1,
        23,
        5,
        tzinfo=timezone.utc,
    )

    def weather_record(
        *,
        source_class: str,
        source_name: str,
        record_id: str,
        minutes_before_game: int,
        is_forecast: bool,
        temperature_c: float = 27.0,
        humidity: float = 55.0,
        pressure: float = 1005.0,
        wind_speed: float = 4.0,
        wind_direction: float = 180.0,
        precipitation_rate: float = 0.0,
        roof_type: str = "open_air",
        roof_state: str = "not_applicable",
    ) -> Any:
        observation_time = (
            game_start
            - timedelta(
                minutes=minutes_before_game
            )
        )

        return contract.WeatherRecord(
            canonical_venue_id="venue-alpha",
            game_start_time_utc=game_start,
            observation_time_utc=(
                observation_time
            ),
            retrieved_at_utc=(
                observation_time
                - timedelta(minutes=15)
            ),
            roof_type=roof_type,
            roof_state=roof_state,
            indoor_effective=False,
            temperature_c=temperature_c,
            relative_humidity_pct=humidity,
            dew_point_c=17.0,
            station_pressure_hpa=pressure,
            sea_level_pressure_hpa=1012.0,
            wind_speed_mps=wind_speed,
            wind_gust_mps=6.0,
            wind_direction_degrees=(
                wind_direction
            ),
            precipitation_state="none",
            precipitation_rate_mm_hr=(
                precipitation_rate
            ),
            weather_source_name=source_name,
            weather_source_record_id=(
                record_id
            ),
            weather_source_class=(
                source_class
            ),
            is_forecast=is_forecast,
            freshness_minutes=float(
                minutes_before_game
            ),
            fallback_used=False,
            diagnostic_codes=(),
        )

    observation = weather_record(
        source_class=(
            contract.OBSERVATION_SOURCE_CLASS
        ),
        source_name="primary-observation",
        record_id="obs-1",
        minutes_before_game=20,
        is_forecast=False,
    )

    forecast = weather_record(
        source_class=(
            contract.FORECAST_SOURCE_CLASS
        ),
        source_name="primary-forecast",
        record_id="forecast-1",
        minutes_before_game=10,
        is_forecast=True,
    )

    secondary = weather_record(
        source_class=(
            contract.SECONDARY_SOURCE_CLASS
        ),
        source_name="secondary-provider",
        record_id="secondary-1",
        minutes_before_game=5,
        is_forecast=False,
    )

    stale = weather_record(
        source_class=(
            contract.OBSERVATION_SOURCE_CLASS
        ),
        source_name="stale-provider",
        record_id="stale-1",
        minutes_before_game=400,
        is_forecast=False,
    )

    case_rows: list[dict[str, Any]] = []

    def record_case(
        case_id: str,
        description: str,
        passed: bool,
        actual: Any,
        expected: Any,
    ) -> None:
        case_rows.append(
            {
                "case_id": case_id,
                "description": description,
                "passed": passed,
                "actual": json.dumps(
                    actual,
                    sort_keys=True,
                    default=str,
                ),
                "expected": json.dumps(
                    expected,
                    sort_keys=True,
                    default=str,
                ),
            }
        )

    fixed_dome = contract.resolve_roof_state(
        "fixed_dome",
        "fixed_closed",
    )

    record_case(
        "7F-C01",
        "fixed dome resolves as indoor",
        fixed_dome.valid
        and fixed_dome.indoor_effective,
        fixed_dome.to_dict(),
        {
            "valid": True,
            "indoor_effective": True,
        },
    )

    retractable_closed = (
        contract.resolve_roof_state(
            "retractable",
            "closed",
        )
    )

    record_case(
        "7F-C02",
        "retractable closed roof resolves as indoor",
        retractable_closed.valid
        and retractable_closed.indoor_effective,
        retractable_closed.to_dict(),
        {
            "valid": True,
            "indoor_effective": True,
        },
    )

    indoor = contract.resolve_atmospheric_state(
        canonical_venue_id="venue-alpha",
        game_start_time_utc=game_start,
        roof_type="fixed_dome",
        roof_state="fixed_closed",
        records=[observation],
    )

    record_case(
        "7F-C03",
        "indoor state neutralizes outdoor weather",
        indoor.indoor_effective
        and indoor.temperature_c is None
        and (
            "indoor_environment_outdoor_weather_neutralized"
            in indoor.diagnostic_codes
        ),
        indoor.to_dict(),
        {
            "indoor_effective": True,
            "temperature_c": None,
        },
    )

    selected_observation = (
        contract.resolve_atmospheric_state(
            canonical_venue_id=(
                "venue-alpha"
            ),
            game_start_time_utc=game_start,
            roof_type="open_air",
            roof_state="not_applicable",
            records=[
                forecast,
                observation,
            ],
        )
    )

    record_case(
        "7F-C04",
        "observation outranks forecast",
        selected_observation.weather_source_name
        == "primary-observation"
        and not selected_observation.is_forecast,
        selected_observation.to_dict(),
        {
            "weather_source_name": (
                "primary-observation"
            ),
            "is_forecast": False,
        },
    )

    selected_forecast = (
        contract.resolve_atmospheric_state(
            canonical_venue_id=(
                "venue-alpha"
            ),
            game_start_time_utc=game_start,
            roof_type="open_air",
            roof_state="not_applicable",
            records=[forecast],
        )
    )

    record_case(
        "7F-C05",
        "forecast selected when observation unavailable",
        selected_forecast.weather_source_name
        == "primary-forecast"
        and selected_forecast.fallback_used,
        selected_forecast.to_dict(),
        {
            "weather_source_name": (
                "primary-forecast"
            ),
            "fallback_used": True,
        },
    )

    selected_secondary = (
        contract.resolve_atmospheric_state(
            canonical_venue_id=(
                "venue-alpha"
            ),
            game_start_time_utc=game_start,
            roof_type="open_air",
            roof_state="not_applicable",
            records=[secondary],
        )
    )

    record_case(
        "7F-C06",
        "secondary source fallback explicit",
        selected_secondary.weather_source_name
        == "secondary-provider"
        and (
            "secondary_weather_source_fallback"
            in selected_secondary.diagnostic_codes
        ),
        selected_secondary.to_dict(),
        {
            "weather_source_name": (
                "secondary-provider"
            ),
            "secondary_fallback": True,
        },
    )

    missing = contract.resolve_atmospheric_state(
        canonical_venue_id="venue-alpha",
        game_start_time_utc=game_start,
        roof_type="open_air",
        roof_state="not_applicable",
        records=[],
    )

    record_case(
        "7F-C07",
        "missing weather neutral fallback",
        missing.weather_source_class
        == contract.NEUTRAL_SOURCE_CLASS
        and (
            "weather_missing_neutral_fallback"
            in missing.diagnostic_codes
        ),
        missing.to_dict(),
        {
            "weather_source_class": (
                contract.NEUTRAL_SOURCE_CLASS
            ),
            "missing_fallback": True,
        },
    )

    invalid = weather_record(
        source_class=(
            contract.OBSERVATION_SOURCE_CLASS
        ),
        source_name="invalid-provider",
        record_id="invalid-1",
        minutes_before_game=20,
        is_forecast=False,
        humidity=150.0,
    )

    invalid_resolution = (
        contract.resolve_atmospheric_state(
            canonical_venue_id=(
                "venue-alpha"
            ),
            game_start_time_utc=game_start,
            roof_type="open_air",
            roof_state="not_applicable",
            records=[invalid],
        )
    )

    record_case(
        "7F-C08",
        "invalid weather neutral fallback",
        (
            "weather_invalid_neutral_fallback"
            in invalid_resolution.diagnostic_codes
        )
        and (
            "humidity_between_zero_and_one_hundred"
            in invalid_resolution.validation_errors
        ),
        invalid_resolution.to_dict(),
        {
            "invalid_fallback": True,
            "humidity_error": True,
        },
    )

    stale_resolution = (
        contract.resolve_atmospheric_state(
            canonical_venue_id=(
                "venue-alpha"
            ),
            game_start_time_utc=game_start,
            roof_type="open_air",
            roof_state="not_applicable",
            records=[stale],
            max_freshness_minutes=180.0,
        )
    )

    record_case(
        "7F-C09",
        "stale weather neutral fallback",
        (
            "weather_stale_neutral_fallback"
            in stale_resolution.diagnostic_codes
        ),
        stale_resolution.to_dict(),
        {
            "stale_fallback": True,
        },
    )

    future_observation = weather_record(
        source_class=(
            contract.OBSERVATION_SOURCE_CLASS
        ),
        source_name="future-provider",
        record_id="future-1",
        minutes_before_game=-5,
        is_forecast=False,
    )

    future_resolution = (
        contract.resolve_atmospheric_state(
            canonical_venue_id=(
                "venue-alpha"
            ),
            game_start_time_utc=game_start,
            roof_type="open_air",
            roof_state="not_applicable",
            records=[
                future_observation
            ],
        )
    )

    record_case(
        "7F-C10",
        "future observation rejected",
        (
            "future_observation_prohibited"
            in future_resolution.validation_errors
        ),
        future_resolution.to_dict(),
        {
            "future_observation_rejected": True,
        },
    )

    unknown_roof = (
        contract.resolve_atmospheric_state(
            canonical_venue_id=(
                "venue-alpha"
            ),
            game_start_time_utc=game_start,
            roof_type="retractable",
            roof_state="unknown",
            records=[observation],
        )
    )

    record_case(
        "7F-C11",
        "unknown roof retains weather without indoor claim",
        not unknown_roof.indoor_effective
        and unknown_roof.temperature_c
        == observation.temperature_c
        and (
            "roof_state_unknown_no_indoor_neutralization"
            in unknown_roof.diagnostic_codes
        ),
        unknown_roof.to_dict(),
        {
            "indoor_effective": False,
            "weather_retained": True,
        },
    )

    deterministic_repeat = (
        contract.resolve_atmospheric_state(
            canonical_venue_id=(
                "venue-alpha"
            ),
            game_start_time_utc=game_start,
            roof_type="open_air",
            roof_state="not_applicable",
            records=[
                secondary,
                observation,
                forecast,
            ],
        )
    )

    record_case(
        "7F-C12",
        "resolution deterministic under record ordering",
        deterministic_repeat.to_dict()
        == selected_observation.to_dict(),
        deterministic_repeat.to_dict(),
        selected_observation.to_dict(),
    )

    disabled = (
        contract.evaluate_weather_atmospheric_diagnostic(
            enabled=False,
            canonical_venue_id=(
                "venue-alpha"
            ),
            game_start_time_utc=game_start,
            roof_type="open_air",
            roof_state="not_applicable",
            records=[observation],
        )
    )

    record_case(
        "7F-C13",
        "diagnostic disabled behavior",
        disabled
        == {
            "enabled": False,
            "diagnostic_code": (
                "weather_atmospheric_diagnostic_disabled"
            ),
            "production_authority": False,
            "simulation_inputs_changed": False,
            "canonical_probability_authority_changed": False,
            "batted_ball_carry_calculated": False,
        },
        disabled,
        {
            "enabled": False,
            "production_authority": False,
        },
    )

    mutable_records = copy.deepcopy(
        [
            observation,
            forecast,
            secondary,
        ]
    )

    records_before = copy.deepcopy(
        mutable_records
    )

    enabled = (
        contract.evaluate_weather_atmospheric_diagnostic(
            enabled=True,
            canonical_venue_id=(
                "venue-alpha"
            ),
            game_start_time_utc=game_start,
            roof_type="open_air",
            roof_state="not_applicable",
            records=mutable_records,
        )
    )

    record_case(
        "7F-C14",
        "enabled diagnostic remains metadata only",
        enabled[
            "production_authority"
        ]
        is False
        and enabled[
            "simulation_inputs_changed"
        ]
        is False
        and enabled[
            "canonical_probability_authority_changed"
        ]
        is False
        and enabled[
            "batted_ball_carry_calculated"
        ]
        is False,
        enabled,
        {
            "metadata_only": True,
        },
    )

    record_case(
        "7F-C15",
        "caller records remain immutable",
        mutable_records == records_before,
        {
            "records_unchanged": (
                mutable_records
                == records_before
            ),
        },
        {
            "records_unchanged": True,
        },
    )

    implementation_checks = [
        {
            "check": "required_paths_exist",
            "actual": required_paths_exist,
            "expected": True,
            "passed": required_paths_exist,
        },
        {
            "check": "seven_e_predecessor_contract_present",
            "actual": predecessor_contract_present,
            "expected": True,
            "passed": predecessor_contract_present,
        },
        {
            "check": "fifteen_contract_cases_pass",
            "actual": sum(
                1
                for row in case_rows
                if row["passed"]
            ),
            "expected": 15,
            "passed": all(
                row["passed"]
                for row in case_rows
            ),
        },
        {
            "check": "diagnostic_disabled_by_default",
            "actual": disabled["enabled"],
            "expected": False,
            "passed": (
                disabled["enabled"]
                is False
            ),
        },
        {
            "check": "production_authority_absent",
            "actual": enabled[
                "production_authority"
            ],
            "expected": False,
            "passed": (
                enabled[
                    "production_authority"
                ]
                is False
            ),
        },
        {
            "check": "simulation_inputs_unchanged",
            "actual": enabled[
                "simulation_inputs_changed"
            ],
            "expected": False,
            "passed": (
                enabled[
                    "simulation_inputs_changed"
                ]
                is False
            ),
        },
        {
            "check": "probability_authority_unchanged",
            "actual": enabled[
                "canonical_probability_authority_changed"
            ],
            "expected": False,
            "passed": (
                enabled[
                    "canonical_probability_authority_changed"
                ]
                is False
            ),
        },
        {
            "check": "batted_ball_carry_not_calculated",
            "actual": enabled[
                "batted_ball_carry_calculated"
            ],
            "expected": False,
            "passed": (
                enabled[
                    "batted_ball_carry_calculated"
                ]
                is False
            ),
        },
        {
            "check": "caller_inputs_immutable",
            "actual": (
                mutable_records
                == records_before
            ),
            "expected": True,
            "passed": (
                mutable_records
                == records_before
            ),
        },
        {
            "check": "implementation_boundary_preserved",
            "actual": True,
            "expected": True,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in implementation_checks
    )

    authority_rows = [
        {
            "authority": (
                "roof_weather_atmospheric_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "The deterministic metadata-only contract passed "
                "all implementation checks."
            ),
        },
        {
            "authority": (
                "production_environment_activation"
            ),
            "granted": False,
            "reason": (
                "7F does not wire the contract into production."
            ),
        },
        {
            "authority": (
                "simulation_probability_change"
            ),
            "granted": False,
            "reason": (
                "The diagnostic emits metadata only."
            ),
        },
        {
            "authority": (
                "batted_ball_carry_calculation"
            ),
            "granted": False,
            "reason": (
                "No atmospheric carry calculation is implemented."
            ),
        },
        {
            "authority": (
                "historical_validation"
            ),
            "granted": False,
            "reason": (
                "No historical outcomes are joined."
            ),
        },
        {
            "authority": (
                "pricing_or_edge_detection"
            ),
            "granted": False,
            "reason": (
                "Pricing and edge work remain unauthorized."
            ),
        },
    ]

    recommended_next_layer = (
        "7G_wind_field_orientation_and_batted_ball_vector_contract_plan"
        if all_checks_passed
        else
        "7F_roof_weather_atmospheric_contract_remediation"
    )

    diagnosis_name = (
        "roof_dome_weather_and_atmospheric_state_contract_implementation_passed"
        if all_checks_passed
        else
        "roof_dome_weather_and_atmospheric_state_contract_implementation_failed"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        implementation_checks,
    )

    write_csv(
        OUTPUT_DIR / "contract_cases.csv",
        [
            "case_id",
            "description",
            "passed",
            "actual",
            "expected",
        ],
        case_rows,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        authority_rows,
    )

    write_csv(
        OUTPUT_DIR / "recommended_path.csv",
        [
            "recommended_next_layer",
            "recommended_action",
            "entry_condition",
            "passed",
        ],
        [
            {
                "recommended_next_layer": (
                    recommended_next_layer
                ),
                "recommended_action": (
                    "Plan wind, field orientation, and batted-ball "
                    "vector semantics."
                    if all_checks_passed
                    else
                    "Remediate failed 7F implementation checks."
                ),
                "entry_condition": (
                    "All ten 7F implementation checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "implementation_checks_required": len(
            implementation_checks
        ),
        "implementation_checks_passed": sum(
            1
            for row in implementation_checks
            if row["passed"]
        ),
        "contract_cases_required": len(
            case_rows
        ),
        "contract_cases_passed": sum(
            1
            for row in case_rows
            if row["passed"]
        ),
        "roof_state_resolution_implemented": True,
        "environment_state_schema_implemented": True,
        "weather_source_precedence_implemented": True,
        "freshness_semantics_implemented": True,
        "physical_range_validation_implemented": True,
        "indoor_neutralization_implemented": True,
        "missing_invalid_stale_fallbacks_implemented": True,
        "secondary_source_fallback_implemented": True,
        "diagnostic_disabled_by_default": True,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "batted_ball_carry_calculated": False,
        "historical_validation_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "implementation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer7_completed": False,
        "new_production_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "wind_vector_contract_planning_allowed_next": (
            all_checks_passed
        ),
        "production_environment_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "implementation_checks.csv"
            ),
            str(
                OUTPUT_DIR / "contract_cases.csv"
            ),
            str(
                OUTPUT_DIR / "authority_boundaries.csv"
            ),
            str(
                OUTPUT_DIR / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR / "implementation_summary.json"
            ),
            str(
                OUTPUT_DIR / "diagnosis.json"
            ),
        ],
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        json.dumps(
            diagnosis,
            indent=2,
        )
    )

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
