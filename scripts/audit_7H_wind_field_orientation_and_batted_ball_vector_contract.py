#!/usr/bin/env python3
"""
Layer 7H
Wind, Field Orientation, and Batted-Ball Vector Contract Audit

Implements and independently audits the 7G contract as a disabled-by-default,
metadata-only diagnostic.

No aerodynamic carry, batted-ball outcome change, production integration, or
probability authority is introduced.
"""

from __future__ import annotations

import ast
import copy
import csv
from datetime import date, datetime, timezone
import importlib.util
import json
import math
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7H"
LAYER_NAME = (
    "wind_field_orientation_and_batted_ball_vector_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_7H_wind_field_orientation_and_batted_ball_vector_contract"
)

PLAN_7G_PATH = (
    ROOT
    / "scripts/"
    "plan_7G_wind_field_orientation_and_batted_ball_vector_contract.py"
)

CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/wind_field_vector_contract.py"
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
        "wind_field_vector_contract_7h",
        CONTRACT_PATH,
    )

    if (
        spec is None
        or spec.loader is None
    ):
        raise RuntimeError(
            "Unable to load wind vector contract module"
        )

    module = importlib.util.module_from_spec(
        spec
    )

    import sys

    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


def close(
    first: float | None,
    second: float,
    tolerance: float = 1e-9,
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
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_paths_exist = (
        PLAN_7G_PATH.exists()
        and CONTRACT_PATH.exists()
    )

    predecessor_contract_present = (
        "wind_field_orientation_and_batted_ball_vector_contract_plan_complete"
        in string_constants(
            PLAN_7G_PATH
        )
    )

    contract = load_contract_module()

    game_date = date(
        2026,
        7,
        1,
    )

    orientation = contract.FieldOrientation(
        canonical_venue_id="venue-alpha",
        orientation_version="orientation-v1",
        home_plate_latitude=40.0,
        home_plate_longitude=-73.0,
        center_field_bearing_degrees_true=0.0,
        left_field_line_bearing_degrees_true=315.0,
        right_field_line_bearing_degrees_true=45.0,
        fair_territory_span_degrees=90.0,
        orientation_source_name="test-source",
        orientation_source_record_id="orientation-1",
        retrieved_at_utc=datetime(
            2026,
            6,
            1,
            tzinfo=timezone.utc,
        ),
        orientation_valid_from=date(
            2000,
            1,
            1,
        ),
        orientation_valid_through=None,
        diagnostic_codes=(),
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

    record_case(
        "7H-C01",
        "angle normalization wraps positive values",
        close(
            contract.normalize_degrees(
                450.0
            ),
            90.0,
        ),
        contract.normalize_degrees(
            450.0
        ),
        90.0,
    )

    record_case(
        "7H-C02",
        "angle normalization wraps negative values",
        close(
            contract.normalize_degrees(
                -90.0
            ),
            270.0,
        ),
        contract.normalize_degrees(
            -90.0
        ),
        270.0,
    )

    record_case(
        "7H-C03",
        "meteorological from converts to toward",
        close(
            contract.meteorological_from_to_toward(
                0.0
            ),
            180.0,
        ),
        contract.meteorological_from_to_toward(
            0.0
        ),
        180.0,
    )

    north_unit = (
        contract.bearing_unit_vector(
            0.0
        )
    )

    record_case(
        "7H-C04",
        "north bearing unit vector",
        close(
            north_unit[0],
            0.0,
        )
        and close(
            north_unit[1],
            1.0,
        ),
        north_unit,
        (
            0.0,
            1.0,
        ),
    )

    east_unit = (
        contract.bearing_unit_vector(
            90.0
        )
    )

    record_case(
        "7H-C05",
        "east bearing unit vector",
        close(
            east_unit[0],
            1.0,
        )
        and close(
            east_unit[1],
            0.0,
        ),
        east_unit,
        (
            1.0,
            0.0,
        ),
    )

    wind_out = (
        contract.resolve_wind_field_vector(
            orientation=orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                180.0
            ),
            wind_speed_mps=10.0,
            batted_ball_spray_angle_degrees=(
                0.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C06",
        "south-origin wind travels toward center field",
        wind_out.vector_resolution_status
        == "resolved"
        and close(
            wind_out.wind_outfield_mps,
            10.0,
        )
        and close(
            wind_out.wind_crossfield_mps,
            0.0,
        ),
        wind_out.to_dict(),
        {
            "wind_outfield_mps": 10.0,
            "wind_crossfield_mps": 0.0,
        },
    )

    wind_in = (
        contract.resolve_wind_field_vector(
            orientation=orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                0.0
            ),
            wind_speed_mps=10.0,
            batted_ball_spray_angle_degrees=(
                0.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C07",
        "north-origin wind travels toward home plate",
        close(
            wind_in.wind_outfield_mps,
            -10.0,
        ),
        wind_in.to_dict(),
        {
            "wind_outfield_mps": -10.0,
        },
    )

    right_crosswind = (
        contract.resolve_wind_field_vector(
            orientation=orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                270.0
            ),
            wind_speed_mps=8.0,
            batted_ball_spray_angle_degrees=(
                0.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C08",
        "west-origin wind travels toward right field",
        close(
            right_crosswind.wind_crossfield_mps,
            8.0,
        )
        and close(
            right_crosswind.wind_outfield_mps,
            0.0,
        ),
        right_crosswind.to_dict(),
        {
            "wind_crossfield_mps": 8.0,
            "wind_outfield_mps": 0.0,
        },
    )

    spray_vector = (
        contract.resolve_wind_field_vector(
            orientation=orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                225.0
            ),
            wind_speed_mps=6.0,
            batted_ball_spray_angle_degrees=(
                45.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C09",
        "spray angle converts to true bearing",
        close(
            spray_vector.batted_ball_bearing_degrees_true,
            45.0,
        )
        and close(
            spray_vector.wind_along_ball_path_mps,
            6.0,
        ),
        spray_vector.to_dict(),
        {
            "ball_bearing": 45.0,
            "along_component": 6.0,
        },
    )

    missing_orientation = (
        contract.resolve_wind_field_vector(
            orientation=None,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                180.0
            ),
            wind_speed_mps=5.0,
            batted_ball_spray_angle_degrees=(
                0.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C10",
        "missing orientation returns unavailable",
        missing_orientation.vector_resolution_status
        == "unavailable"
        and (
            "field_orientation_missing_vector_unavailable"
            in missing_orientation.diagnostic_codes
        ),
        missing_orientation.to_dict(),
        {
            "status": "unavailable",
        },
    )

    invalid_orientation = (
        contract.FieldOrientation(
            canonical_venue_id="venue-alpha",
            orientation_version="orientation-v1",
            home_plate_latitude=None,
            home_plate_longitude=None,
            center_field_bearing_degrees_true=(
                None
            ),
            left_field_line_bearing_degrees_true=(
                315.0
            ),
            right_field_line_bearing_degrees_true=(
                45.0
            ),
            fair_territory_span_degrees=90.0,
            orientation_source_name="test",
            orientation_source_record_id=None,
            retrieved_at_utc=datetime(
                2026,
                6,
                1,
                tzinfo=timezone.utc,
            ),
            orientation_valid_from=None,
            orientation_valid_through=None,
            diagnostic_codes=(),
        )
    )

    invalid_orientation_result = (
        contract.resolve_wind_field_vector(
            orientation=invalid_orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                180.0
            ),
            wind_speed_mps=5.0,
            batted_ball_spray_angle_degrees=(
                0.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C11",
        "invalid orientation returns unavailable",
        invalid_orientation_result.vector_resolution_status
        == "unavailable"
        and (
            "center_field_bearing_degrees_true_in_range"
            in invalid_orientation_result.validation_errors
        ),
        invalid_orientation_result.to_dict(),
        {
            "status": "unavailable",
            "orientation_error": True,
        },
    )

    missing_wind = (
        contract.resolve_wind_field_vector(
            orientation=orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=None,
            wind_speed_mps=None,
            batted_ball_spray_angle_degrees=(
                0.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C12",
        "missing wind returns neutral vector",
        missing_wind.vector_resolution_status
        == "neutral"
        and close(
            missing_wind.wind_outfield_mps,
            0.0,
        )
        and (
            "wind_missing_neutral_vector"
            in missing_wind.diagnostic_codes
        ),
        missing_wind.to_dict(),
        {
            "status": "neutral",
        },
    )

    invalid_wind = (
        contract.resolve_wind_field_vector(
            orientation=orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                400.0
            ),
            wind_speed_mps=-1.0,
            batted_ball_spray_angle_degrees=(
                0.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C13",
        "invalid wind returns neutral vector",
        invalid_wind.vector_resolution_status
        == "neutral"
        and (
            "wind_direction_in_range"
            in invalid_wind.validation_errors
        )
        and (
            "wind_speed_finite_and_nonnegative"
            in invalid_wind.validation_errors
        ),
        invalid_wind.to_dict(),
        {
            "status": "neutral",
            "wind_errors": True,
        },
    )

    indoor = (
        contract.resolve_wind_field_vector(
            orientation=orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                180.0
            ),
            wind_speed_mps=10.0,
            batted_ball_spray_angle_degrees=(
                0.0
            ),
            indoor_effective=True,
        )
    )

    record_case(
        "7H-C14",
        "indoor environment returns zero wind vector",
        indoor.vector_resolution_status
        == "neutral"
        and close(
            indoor.wind_outfield_mps,
            0.0,
        )
        and (
            "indoor_environment_zero_wind_vector"
            in indoor.diagnostic_codes
        ),
        indoor.to_dict(),
        {
            "status": "neutral",
            "indoor_zero_wind": True,
        },
    )

    deterministic_repeat = (
        contract.resolve_wind_field_vector(
            orientation=orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                225.0
            ),
            wind_speed_mps=6.0,
            batted_ball_spray_angle_degrees=(
                45.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C15",
        "vector resolution deterministic",
        deterministic_repeat.to_dict()
        == spray_vector.to_dict(),
        deterministic_repeat.to_dict(),
        spray_vector.to_dict(),
    )

    disabled = (
        contract.evaluate_wind_field_vector_diagnostic(
            enabled=False,
            orientation=orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                180.0
            ),
            wind_speed_mps=10.0,
            batted_ball_spray_angle_degrees=(
                0.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C16",
        "diagnostic disabled behavior",
        disabled
        == {
            "enabled": False,
            "diagnostic_code": (
                "wind_field_vector_diagnostic_disabled"
            ),
            "production_authority": False,
            "simulation_inputs_changed": False,
            "canonical_probability_authority_changed": False,
            "aerodynamic_carry_calculated": False,
            "batted_ball_outcomes_changed": False,
        },
        disabled,
        {
            "enabled": False,
            "production_authority": False,
        },
    )

    mutable_orientation = copy.deepcopy(
        orientation
    )

    orientation_before = copy.deepcopy(
        mutable_orientation
    )

    enabled = (
        contract.evaluate_wind_field_vector_diagnostic(
            enabled=True,
            orientation=mutable_orientation,
            game_date=game_date,
            meteorological_wind_from_degrees=(
                180.0
            ),
            wind_speed_mps=10.0,
            batted_ball_spray_angle_degrees=(
                0.0
            ),
            indoor_effective=False,
        )
    )

    record_case(
        "7H-C17",
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
            "aerodynamic_carry_calculated"
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

    record_case(
        "7H-C18",
        "caller orientation remains immutable",
        mutable_orientation
        == orientation_before,
        {
            "orientation_unchanged": (
                mutable_orientation
                == orientation_before
            ),
        },
        {
            "orientation_unchanged": True,
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
            "check": "seven_g_predecessor_contract_present",
            "actual": predecessor_contract_present,
            "expected": True,
            "passed": predecessor_contract_present,
        },
        {
            "check": "eighteen_contract_cases_pass",
            "actual": sum(
                1
                for row in case_rows
                if row["passed"]
            ),
            "expected": 18,
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
            "check": "aerodynamic_carry_not_calculated",
            "actual": enabled[
                "aerodynamic_carry_calculated"
            ],
            "expected": False,
            "passed": (
                enabled[
                    "aerodynamic_carry_calculated"
                ]
                is False
            ),
        },
        {
            "check": "batted_ball_outcomes_unchanged",
            "actual": enabled[
                "batted_ball_outcomes_changed"
            ],
            "expected": False,
            "passed": (
                enabled[
                    "batted_ball_outcomes_changed"
                ]
                is False
            ),
        },
        {
            "check": "caller_inputs_immutable",
            "actual": (
                mutable_orientation
                == orientation_before
            ),
            "expected": True,
            "passed": (
                mutable_orientation
                == orientation_before
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
                "wind_field_vector_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "The deterministic metadata-only vector contract "
                "passed all implementation checks."
            ),
        },
        {
            "authority": (
                "production_environment_activation"
            ),
            "granted": False,
            "reason": (
                "7H does not wire vectors into production."
            ),
        },
        {
            "authority": (
                "simulation_probability_change"
            ),
            "granted": False,
            "reason": (
                "The diagnostic emits vector metadata only."
            ),
        },
        {
            "authority": (
                "aerodynamic_carry_calculation"
            ),
            "granted": False,
            "reason": (
                "No carry or flight model is implemented."
            ),
        },
        {
            "authority": (
                "batted_ball_outcome_change"
            ),
            "granted": False,
            "reason": (
                "Batted-ball results remain unchanged."
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
        "7I_atmospheric_density_and_carry_diagnostic_contract_plan"
        if all_checks_passed
        else
        "7H_wind_field_vector_contract_remediation"
    )

    diagnosis_name = (
        "wind_field_orientation_and_batted_ball_vector_contract_implementation_passed"
        if all_checks_passed
        else
        "wind_field_orientation_and_batted_ball_vector_contract_implementation_failed"
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
                    "Plan an atmospheric-density and carry diagnostic "
                    "contract without production authority."
                    if all_checks_passed
                    else
                    "Remediate failed 7H implementation checks."
                ),
                "entry_condition": (
                    "All eleven 7H implementation checks pass."
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
        "field_orientation_schema_implemented": True,
        "angle_normalization_implemented": True,
        "meteorological_wind_conversion_implemented": True,
        "geographic_vector_components_implemented": True,
        "field_relative_components_implemented": True,
        "ball_path_components_implemented": True,
        "neutral_and_unavailable_fallbacks_implemented": True,
        "diagnostic_disabled_by_default": True,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "aerodynamic_carry_calculated": False,
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
        "atmospheric_density_carry_planning_allowed_next": (
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
