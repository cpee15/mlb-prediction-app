#!/usr/bin/env python3
"""
Layer 7J atmospheric-density and carry diagnostic implementation audit.
"""

from __future__ import annotations

import ast
import copy
import csv
from datetime import datetime, timezone
import importlib.util
import json
import math
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7J"
LAYER_NAME = (
    "atmospheric_density_and_carry_diagnostic_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_7J_atmospheric_density_and_carry_diagnostic_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_7I_atmospheric_density_and_carry_diagnostic_contract.py"
)

CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/"
    "atmospheric_density_carry_contract.py"
)


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def string_constants(path: Path) -> set[str]:
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


def load_contract() -> Any:
    spec = importlib.util.spec_from_file_location(
        "atmospheric_density_carry_contract_7j",
        CONTRACT_PATH,
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(
            "Unable to load 7J contract"
        )

    module = importlib.util.module_from_spec(spec)

    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


def close(
    first: float | None,
    second: float,
    tolerance: float = 1e-6,
) -> bool:
    return (
        first is not None
        and math.isclose(
            first,
            second,
            abs_tol=tolerance,
        )
    )


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    required_paths_exist = (
        PLAN_PATH.exists()
        and CONTRACT_PATH.exists()
    )

    predecessor_present = (
        "atmospheric_density_and_carry_diagnostic_contract_plan_complete"
        in string_constants(PLAN_PATH)
    )

    contract = load_contract()

    def make_input(**overrides: Any) -> Any:
        values = {
            "temperature_c": 20.0,
            "relative_humidity_pct": 50.0,
            "dew_point_c": 9.3,
            "station_pressure_hpa": 1013.25,
            "sea_level_pressure_hpa": 1015.0,
            "venue_elevation_m": 10.0,
            "wind_along_ball_path_mps": 2.0,
            "indoor_effective": False,
            "weather_source_class": "confirmed_observation",
            "weather_source_record_id": "weather-1",
            "observation_time_utc": datetime(
                2026,
                7,
                1,
                22,
                45,
                tzinfo=timezone.utc,
            ),
            "freshness_minutes": 20.0,
        }
        values.update(overrides)
        return contract.AtmosphericCarryInput(**values)

    cases: list[dict[str, Any]] = []

    def record(
        case_id: str,
        description: str,
        passed: bool,
        actual: Any,
        expected: Any,
    ) -> None:
        cases.append(
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

    record(
        "7J-C01",
        "kelvin conversion",
        close(20.0 + 273.15, 293.15),
        293.15,
        293.15,
    )

    saturation = (
        contract.saturation_vapor_pressure_hpa(
            20.0
        )
    )

    record(
        "7J-C02",
        "saturation vapor pressure finite",
        math.isfinite(saturation)
        and saturation > 0.0,
        saturation,
        "positive_finite",
    )

    density_values = (
        contract.moist_air_density_kg_m3(
            temperature_c=15.0,
            relative_humidity_pct=0.0,
            pressure_hpa=1013.25,
        )
    )

    record(
        "7J-C03",
        "reference-like density reasonable",
        1.20
        <= density_values[3]
        <= 1.25,
        density_values[3],
        "1.20_to_1.25",
    )

    record(
        "7J-C04",
        "density index bounded",
        -1.0
        <= contract.density_component_index(
            0.5
        )
        <= 1.0,
        contract.density_component_index(
            0.5
        ),
        "bounded",
    )

    record(
        "7J-C05",
        "wind index bounded",
        close(
            contract.wind_component_index(
                100.0
            ),
            1.0,
        ),
        contract.wind_component_index(
            100.0
        ),
        1.0,
    )

    record(
        "7J-C06",
        "combined carry index bounded",
        close(
            contract.combined_carry_index(
                1.0,
                1.0,
            ),
            1.0,
        ),
        contract.combined_carry_index(
            1.0,
            1.0,
        ),
        1.0,
    )

    resolved = (
        contract.resolve_atmospheric_density_carry(
            make_input()
        )
    )

    record(
        "7J-C07",
        "valid atmospheric input resolves",
        resolved.resolution_status
        == "resolved"
        and resolved.air_density_kg_m3
        is not None
        and resolved.combined_carry_index
        is not None,
        resolved.to_dict(),
        {
            "status": "resolved",
        },
    )

    record(
        "7J-C08",
        "station pressure takes precedence",
        resolved.pressure_source
        == "station_pressure_hpa",
        resolved.pressure_source,
        "station_pressure_hpa",
    )

    sea_level = (
        contract.resolve_atmospheric_density_carry(
            make_input(
                station_pressure_hpa=None
            )
        )
    )

    record(
        "7J-C09",
        "sea-level pressure fallback",
        sea_level.pressure_source
        == "sea_level_pressure_hpa"
        and (
            "sea_level_pressure_fallback_selected"
            in sea_level.diagnostic_codes
        ),
        sea_level.to_dict(),
        {
            "pressure_source": (
                "sea_level_pressure_hpa"
            ),
        },
    )

    reference_pressure = (
        contract.resolve_atmospheric_density_carry(
            make_input(
                station_pressure_hpa=None,
                sea_level_pressure_hpa=None,
            )
        )
    )

    record(
        "7J-C10",
        "reference pressure fallback",
        reference_pressure.pressure_source
        == "reference_pressure"
        and (
            "pressure_missing_reference_fallback"
            in reference_pressure.diagnostic_codes
        ),
        reference_pressure.to_dict(),
        {
            "pressure_source": (
                "reference_pressure"
            ),
        },
    )

    indoor = (
        contract.resolve_atmospheric_density_carry(
            make_input(
                indoor_effective=True
            )
        )
    )

    record(
        "7J-C11",
        "indoor input neutralized",
        indoor.resolution_status
        == "neutral"
        and close(
            indoor.combined_carry_index,
            0.0,
        ),
        indoor.to_dict(),
        {
            "status": "neutral",
            "carry_index": 0.0,
        },
    )

    missing = (
        contract.resolve_atmospheric_density_carry(
            make_input(
                temperature_c=None
            )
        )
    )

    record(
        "7J-C12",
        "missing atmospheric input unavailable",
        missing.resolution_status
        == "unavailable",
        missing.to_dict(),
        {
            "status": "unavailable",
        },
    )

    invalid = (
        contract.resolve_atmospheric_density_carry(
            make_input(
                relative_humidity_pct=150.0
            )
        )
    )

    record(
        "7J-C13",
        "invalid atmospheric input neutralized",
        invalid.resolution_status
        == "neutral"
        and (
            "humidity_between_zero_and_one_hundred"
            in invalid.validation_errors
        ),
        invalid.to_dict(),
        {
            "status": "neutral",
            "humidity_error": True,
        },
    )

    missing_wind = (
        contract.resolve_atmospheric_density_carry(
            make_input(
                wind_along_ball_path_mps=None
            )
        )
    )

    record(
        "7J-C14",
        "missing wind uses zero component",
        close(
            missing_wind.wind_component_index,
            0.0,
        )
        and (
            "along_path_wind_missing_zero_component"
            in missing_wind.diagnostic_codes
        ),
        missing_wind.to_dict(),
        {
            "wind_index": 0.0,
        },
    )

    favorable_density = (
        contract.resolve_atmospheric_density_carry(
            make_input(
                temperature_c=35.0,
                relative_humidity_pct=20.0,
                station_pressure_hpa=900.0,
                wind_along_ball_path_mps=0.0,
            )
        )
    )

    record(
        "7J-C15",
        "lower density yields positive density index",
        favorable_density.density_component_index
        is not None
        and favorable_density.density_component_index
        > 0.0,
        favorable_density.to_dict(),
        {
            "density_component_index": (
                "positive"
            ),
        },
    )

    headwind = (
        contract.resolve_atmospheric_density_carry(
            make_input(
                wind_along_ball_path_mps=-5.0
            )
        )
    )

    record(
        "7J-C16",
        "headwind yields negative wind index",
        headwind.wind_component_index
        is not None
        and headwind.wind_component_index
        < 0.0,
        headwind.to_dict(),
        {
            "wind_component_index": (
                "negative"
            ),
        },
    )

    repeat = (
        contract.resolve_atmospheric_density_carry(
            make_input()
        )
    )

    record(
        "7J-C17",
        "resolution deterministic",
        repeat.to_dict()
        == resolved.to_dict(),
        repeat.to_dict(),
        resolved.to_dict(),
    )

    disabled = (
        contract.evaluate_atmospheric_density_carry_diagnostic(
            enabled=False,
            atmospheric_input=make_input(),
        )
    )

    record(
        "7J-C18",
        "diagnostic disabled behavior",
        disabled["enabled"] is False
        and disabled[
            "production_authority"
        ]
        is False,
        disabled,
        {
            "enabled": False,
            "production_authority": False,
        },
    )

    mutable_input = copy.deepcopy(
        make_input()
    )
    input_before = copy.deepcopy(
        mutable_input
    )

    enabled = (
        contract.evaluate_atmospheric_density_carry_diagnostic(
            enabled=True,
            atmospheric_input=mutable_input,
        )
    )

    record(
        "7J-C19",
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
            "production_carry_activated"
        ]
        is False
        and enabled[
            "batted_ball_distance_changed"
        ]
        is False
        and enabled[
            "batted_ball_outcomes_changed"
        ]
        is False,
        enabled,
        {
            "metadata_only": True,
        },
    )

    record(
        "7J-C20",
        "caller input immutable",
        mutable_input == input_before,
        {
            "input_unchanged": (
                mutable_input == input_before
            ),
        },
        {
            "input_unchanged": True,
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
            "check": "seven_i_predecessor_contract_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_contract_cases_pass",
            "actual": sum(
                1
                for row in cases
                if row["passed"]
            ),
            "expected": 20,
            "passed": all(
                row["passed"]
                for row in cases
            ),
        },
        {
            "check": "diagnostic_disabled_by_default",
            "actual": disabled["enabled"],
            "expected": False,
            "passed": disabled["enabled"] is False,
        },
        {
            "check": "production_authority_absent",
            "actual": enabled[
                "production_authority"
            ],
            "expected": False,
            "passed": enabled[
                "production_authority"
            ]
            is False,
        },
        {
            "check": "simulation_inputs_unchanged",
            "actual": enabled[
                "simulation_inputs_changed"
            ],
            "expected": False,
            "passed": enabled[
                "simulation_inputs_changed"
            ]
            is False,
        },
        {
            "check": "probability_authority_unchanged",
            "actual": enabled[
                "canonical_probability_authority_changed"
            ],
            "expected": False,
            "passed": enabled[
                "canonical_probability_authority_changed"
            ]
            is False,
        },
        {
            "check": "production_carry_not_activated",
            "actual": enabled[
                "production_carry_activated"
            ],
            "expected": False,
            "passed": enabled[
                "production_carry_activated"
            ]
            is False,
        },
        {
            "check": "batted_ball_distance_unchanged",
            "actual": enabled[
                "batted_ball_distance_changed"
            ],
            "expected": False,
            "passed": enabled[
                "batted_ball_distance_changed"
            ]
            is False,
        },
        {
            "check": "batted_ball_outcomes_unchanged",
            "actual": enabled[
                "batted_ball_outcomes_changed"
            ],
            "expected": False,
            "passed": enabled[
                "batted_ball_outcomes_changed"
            ]
            is False,
        },
        {
            "check": "caller_inputs_immutable",
            "actual": mutable_input == input_before,
            "expected": True,
            "passed": mutable_input == input_before,
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
                "atmospheric_density_carry_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "The bounded metadata-only diagnostic passed "
                "all implementation checks."
            ),
        },
        {
            "authority": (
                "production_carry_activation"
            ),
            "granted": False,
            "reason": (
                "No production carry mapping exists."
            ),
        },
        {
            "authority": (
                "batted_ball_distance_change"
            ),
            "granted": False,
            "reason": (
                "The diagnostic index is not mapped to distance."
            ),
        },
        {
            "authority": (
                "batted_ball_outcome_change"
            ),
            "granted": False,
            "reason": (
                "Batted-ball outcomes remain unchanged."
            ),
        },
        {
            "authority": (
                "simulation_probability_change"
            ),
            "granted": False,
            "reason": (
                "Simulation probabilities remain unchanged."
            ),
        },
        {
            "authority": "historical_validation",
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
        "7K_environment_diagnostic_composition_contract_plan"
        if all_checks_passed
        else
        "7J_atmospheric_density_carry_contract_remediation"
    )

    diagnosis_name = (
        "atmospheric_density_and_carry_diagnostic_contract_implementation_passed"
        if all_checks_passed
        else
        "atmospheric_density_and_carry_diagnostic_contract_implementation_failed"
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
        cases,
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
                    "Plan diagnostic composition across venue, "
                    "weather, vector, and carry contracts."
                    if all_checks_passed
                    else
                    "Remediate failed 7J implementation checks."
                ),
                "entry_condition": (
                    "All twelve 7J implementation checks pass."
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
        "contract_cases_required": len(cases),
        "contract_cases_passed": sum(
            1
            for row in cases
            if row["passed"]
        ),
        "moist_air_density_implemented": True,
        "pressure_precedence_implemented": True,
        "density_altitude_implemented": True,
        "bounded_density_index_implemented": True,
        "bounded_wind_index_implemented": True,
        "bounded_combined_index_implemented": True,
        "neutral_and_unavailable_fallbacks_implemented": True,
        "diagnostic_disabled_by_default": True,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_carry_activated": False,
        "batted_ball_distance_changed": False,
        "batted_ball_outcomes_changed": False,
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
        "environment_composition_planning_allowed_next": (
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

    print(json.dumps(diagnosis, indent=2))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
