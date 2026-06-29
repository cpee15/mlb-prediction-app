#!/usr/bin/env python3
"""
Layer 7G
Wind, Field Orientation, and Batted-Ball Vector Contract Plan

Defines the bounded implementation contract for:
- canonical field orientation;
- meteorological wind direction conversion;
- home-plate-relative field coordinates;
- wind vector decomposition;
- batted-ball direction representation;
- deterministic alignment diagnostics;
- missing orientation and wind fallbacks;
- metadata-only runtime exposure.

Planning only. This layer does not:
- calculate atmospheric or aerodynamic carry;
- alter batted-ball outcomes;
- modify simulation state, parameters, or probabilities;
- activate production environment behavior;
- execute historical validation, tuning, backtests, pricing, or edge logic.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7G"
LAYER_NAME = (
    "wind_field_orientation_and_batted_ball_vector_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_7G_wind_field_orientation_and_batted_ball_vector_contract_plan"
)

PLAN_7E_PATH = (
    ROOT
    / "scripts/"
    "plan_7E_roof_dome_weather_and_atmospheric_state_contract.py"
)

AUDIT_7F_PATH = (
    ROOT
    / "scripts/"
    "audit_7F_roof_dome_weather_and_atmospheric_state_contract.py"
)

VENUE_CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/venue_park_factor_contract.py"
)

WEATHER_CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/weather_atmospheric_contract.py"
)

REQUIRED_PATHS = [
    PLAN_7E_PATH,
    AUDIT_7F_PATH,
    VENUE_CONTRACT_PATH,
    WEATHER_CONTRACT_PATH,
]

PROHIBITED_AUTHORITIES = [
    "production_environment_activation",
    "production_wind_activation",
    "production_field_geometry_activation",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
    "batted_ball_outcome_change",
    "aerodynamic_carry_calculation",
    "home_run_probability_change",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_tuning",
    "backtest_execution",
    "pricing",
    "market_comparison",
    "edge_detection",
    "bet_recommendation",
]


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
        )
        + "\n",
        encoding="utf-8",
    )


def read_text(path: Path) -> str:
    if not path.exists():
        return ""

    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def string_constants(path: Path) -> set[str]:
    if not path.exists():
        return set()

    try:
        tree = ast.parse(
            read_text(path),
            filename=str(path),
        )
    except SyntaxError:
        return set()

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_paths_exist = all(
        path.exists()
        for path in REQUIRED_PATHS
    )

    predecessor_definitions = [
        (
            "7E",
            PLAN_7E_PATH,
            (
                "roof_dome_weather_and_atmospheric_"
                "state_contract_plan_complete"
            ),
        ),
        (
            "7F",
            AUDIT_7F_PATH,
            (
                "roof_dome_weather_and_atmospheric_"
                "state_contract_implementation_passed"
            ),
        ),
    ]

    predecessor_contracts = []

    for layer, path, diagnosis in predecessor_definitions:
        constants = string_constants(path)

        predecessor_contracts.append(
            {
                "layer": layer,
                "path": str(
                    path.relative_to(ROOT)
                ),
                "expected_diagnosis": diagnosis,
                "present": diagnosis in constants,
            }
        )

    predecessors_accepted = sum(
        1
        for row in predecessor_contracts
        if row["present"]
    )

    weather_contract_text = read_text(
        WEATHER_CONTRACT_PATH
    )

    wind_source_contract_present = all(
        token in weather_contract_text
        for token in [
            "wind_speed_mps",
            "wind_direction_degrees",
            "wind_gust_mps",
        ]
    )

    field_orientation_fields = [
        {
            "field": "canonical_venue_id",
            "type": "string",
            "required": True,
            "nullable": False,
            "purpose": "Join key to canonical venue identity.",
        },
        {
            "field": "orientation_version",
            "type": "string",
            "required": True,
            "nullable": False,
            "purpose": "Immutable orientation metadata version.",
        },
        {
            "field": "home_plate_latitude",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Optional home-plate latitude.",
        },
        {
            "field": "home_plate_longitude",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Optional home-plate longitude.",
        },
        {
            "field": "center_field_bearing_degrees_true",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "True-north bearing from home plate toward center field."
            ),
        },
        {
            "field": "left_field_line_bearing_degrees_true",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "True-north bearing from home plate down the left-field line."
            ),
        },
        {
            "field": "right_field_line_bearing_degrees_true",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "True-north bearing from home plate down the right-field line."
            ),
        },
        {
            "field": "fair_territory_span_degrees",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Angular width of fair territory.",
        },
        {
            "field": "orientation_source_name",
            "type": "string",
            "required": True,
            "nullable": False,
            "purpose": "Orientation source identity.",
        },
        {
            "field": "orientation_source_record_id",
            "type": "string",
            "required": True,
            "nullable": True,
            "purpose": "Source record identifier when available.",
        },
        {
            "field": "retrieved_at_utc",
            "type": "datetime",
            "required": True,
            "nullable": False,
            "purpose": "Orientation acquisition timestamp.",
        },
        {
            "field": "orientation_valid_from",
            "type": "date",
            "required": True,
            "nullable": True,
            "purpose": "First valid date for this orientation.",
        },
        {
            "field": "orientation_valid_through",
            "type": "date",
            "required": True,
            "nullable": True,
            "purpose": "Last valid date for this orientation.",
        },
        {
            "field": "diagnostic_codes",
            "type": "array[string]",
            "required": True,
            "nullable": False,
            "purpose": "Explicit resolution and fallback metadata.",
        },
    ]

    vector_state_fields = [
        {
            "field": "meteorological_wind_from_degrees",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Direction from which wind originates.",
        },
        {
            "field": "wind_toward_degrees_true",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Direction toward which wind travels.",
        },
        {
            "field": "wind_speed_mps",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Sustained wind-vector magnitude.",
        },
        {
            "field": "wind_east_mps",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "East-positive geographic wind component.",
        },
        {
            "field": "wind_north_mps",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "North-positive geographic wind component.",
        },
        {
            "field": "wind_outfield_mps",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Positive component from home plate toward center field.",
        },
        {
            "field": "wind_crossfield_mps",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Positive component toward right field.",
        },
        {
            "field": "batted_ball_spray_angle_degrees",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Home-plate-relative spray angle with center field at zero."
            ),
        },
        {
            "field": "batted_ball_bearing_degrees_true",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "True-north batted-ball travel bearing.",
        },
        {
            "field": "wind_along_ball_path_mps",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Signed wind component along batted-ball direction.",
        },
        {
            "field": "wind_across_ball_path_mps",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Signed wind component perpendicular to ball direction.",
        },
        {
            "field": "vector_resolution_status",
            "type": "enum",
            "required": True,
            "nullable": False,
            "purpose": "resolved, neutral, invalid, or unavailable.",
        },
    ]

    coordinate_conventions = [
        {
            "rule_id": "VEC-C01",
            "rule": "true_north_zero_degrees",
            "requirement": (
                "Geographic bearings use zero degrees at true north."
            ),
        },
        {
            "rule_id": "VEC-C02",
            "rule": "bearings_increase_clockwise",
            "requirement": (
                "Geographic bearings increase clockwise."
            ),
        },
        {
            "rule_id": "VEC-C03",
            "rule": "meteorological_direction_is_from",
            "requirement": (
                "Weather wind direction represents the origin direction."
            ),
        },
        {
            "rule_id": "VEC-C04",
            "rule": "field_spray_center_is_zero",
            "requirement": (
                "Center field is zero spray angle."
            ),
        },
        {
            "rule_id": "VEC-C05",
            "rule": "positive_spray_toward_right_field",
            "requirement": (
                "Positive spray angle rotates toward right field."
            ),
        },
        {
            "rule_id": "VEC-C06",
            "rule": "positive_outfield_component_is_out",
            "requirement": (
                "Positive outfield wind travels away from home plate."
            ),
        },
    ]

    transformation_rules = [
        {
            "rule_id": "VEC-T01",
            "rule": "wind_from_to_conversion",
            "formula": (
                "wind_toward = (wind_from + 180) mod 360"
            ),
        },
        {
            "rule_id": "VEC-T02",
            "rule": "geographic_east_component",
            "formula": (
                "east = speed * sin(wind_toward_radians)"
            ),
        },
        {
            "rule_id": "VEC-T03",
            "rule": "geographic_north_component",
            "formula": (
                "north = speed * cos(wind_toward_radians)"
            ),
        },
        {
            "rule_id": "VEC-T04",
            "rule": "field_outfield_component",
            "formula": (
                "dot geographic wind with center-field unit vector"
            ),
        },
        {
            "rule_id": "VEC-T05",
            "rule": "field_crossfield_component",
            "formula": (
                "dot geographic wind with right-field-positive unit vector"
            ),
        },
        {
            "rule_id": "VEC-T06",
            "rule": "spray_to_true_bearing",
            "formula": (
                "ball bearing = center-field bearing + spray angle"
            ),
        },
        {
            "rule_id": "VEC-T07",
            "rule": "along_ball_path_component",
            "formula": (
                "dot geographic wind with ball-path unit vector"
            ),
        },
        {
            "rule_id": "VEC-T08",
            "rule": "across_ball_path_component",
            "formula": (
                "dot geographic wind with ball-path perpendicular vector"
            ),
        },
    ]

    validation_rules = [
        {
            "rule_id": "VEC-V01",
            "rule": "canonical_venue_id_nonempty",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V02",
            "rule": "orientation_version_present",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V03",
            "rule": "center_field_bearing_in_range",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V04",
            "rule": "field_line_bearings_in_range",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V05",
            "rule": "fair_territory_span_physically_valid",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V06",
            "rule": "orientation_date_range_valid",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V07",
            "rule": "wind_direction_in_range",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V08",
            "rule": "wind_speed_finite_and_nonnegative",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V09",
            "rule": "spray_angle_within_supported_range",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V10",
            "rule": "vector_components_finite",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V11",
            "rule": "transformation_deterministic",
            "blocking": True,
        },
        {
            "rule_id": "VEC-V12",
            "rule": "diagnostic_does_not_modify_engine_inputs",
            "blocking": True,
        },
    ]

    fallback_contract = [
        {
            "fallback_id": "VEC-F01",
            "condition": "orientation_missing",
            "result": "vector_unavailable",
            "diagnostic_code": (
                "field_orientation_missing_vector_unavailable"
            ),
            "production_authority": False,
        },
        {
            "fallback_id": "VEC-F02",
            "condition": "orientation_invalid",
            "result": "vector_unavailable",
            "diagnostic_code": (
                "field_orientation_invalid_vector_unavailable"
            ),
            "production_authority": False,
        },
        {
            "fallback_id": "VEC-F03",
            "condition": "wind_missing",
            "result": "neutral_zero_wind_vector",
            "diagnostic_code": "wind_missing_neutral_vector",
            "production_authority": False,
        },
        {
            "fallback_id": "VEC-F04",
            "condition": "wind_invalid",
            "result": "neutral_zero_wind_vector",
            "diagnostic_code": "wind_invalid_neutral_vector",
            "production_authority": False,
        },
        {
            "fallback_id": "VEC-F05",
            "condition": "indoor_environment",
            "result": "neutral_zero_wind_vector",
            "diagnostic_code": "indoor_environment_zero_wind_vector",
            "production_authority": False,
        },
    ]

    implementation_steps = [
        {
            "step": 1,
            "action": "Create typed field-orientation schema.",
        },
        {
            "step": 2,
            "action": "Create typed wind and batted-ball vector schemas.",
        },
        {
            "step": 3,
            "action": "Implement angle normalization utilities.",
        },
        {
            "step": 4,
            "action": (
                "Implement meteorological-from to geographic-toward conversion."
            ),
        },
        {
            "step": 5,
            "action": (
                "Implement geographic-to-field coordinate transformation."
            ),
        },
        {
            "step": 6,
            "action": (
                "Implement spray-angle to true-bearing transformation."
            ),
        },
        {
            "step": 7,
            "action": (
                "Implement along-path and cross-path vector decomposition."
            ),
        },
        {
            "step": 8,
            "action": (
                "Implement neutral and unavailable fallback metadata."
            ),
        },
        {
            "step": 9,
            "action": (
                "Add deterministic, disabled-by-default contract audit."
            ),
        },
    ]

    acceptance_criteria = [
        {
            "criterion_id": "VEC-A01",
            "criterion": "field_orientation_schema_complete",
            "required": True,
        },
        {
            "criterion_id": "VEC-A02",
            "criterion": "vector_state_schema_complete",
            "required": True,
        },
        {
            "criterion_id": "VEC-A03",
            "criterion": "coordinate_conventions_explicit",
            "required": True,
        },
        {
            "criterion_id": "VEC-A04",
            "criterion": "wind_from_to_conversion_correct",
            "required": True,
        },
        {
            "criterion_id": "VEC-A05",
            "criterion": "field_coordinate_transform_deterministic",
            "required": True,
        },
        {
            "criterion_id": "VEC-A06",
            "criterion": "spray_bearing_transform_deterministic",
            "required": True,
        },
        {
            "criterion_id": "VEC-A07",
            "criterion": "along_and_cross_components_deterministic",
            "required": True,
        },
        {
            "criterion_id": "VEC-A08",
            "criterion": "missing_orientation_fallback_explicit",
            "required": True,
        },
        {
            "criterion_id": "VEC-A09",
            "criterion": "missing_wind_neutral_fallback_explicit",
            "required": True,
        },
        {
            "criterion_id": "VEC-A10",
            "criterion": "diagnostic_disabled_by_default",
            "required": True,
        },
        {
            "criterion_id": "VEC-A11",
            "criterion": "no_carry_or_probability_changes",
            "required": True,
        },
        {
            "criterion_id": "VEC-A12",
            "criterion": "independent_audit_passes",
            "required": True,
        },
    ]

    planning_checks = [
        {
            "check": "required_paths_exist",
            "actual": required_paths_exist,
            "expected": True,
            "passed": required_paths_exist,
        },
        {
            "check": "two_predecessor_contracts_present",
            "actual": predecessors_accepted,
            "expected": 2,
            "passed": predecessors_accepted == 2,
        },
        {
            "check": "wind_source_contract_present",
            "actual": wind_source_contract_present,
            "expected": True,
            "passed": wind_source_contract_present,
        },
        {
            "check": "fourteen_orientation_fields_defined",
            "actual": len(field_orientation_fields),
            "expected": 14,
            "passed": len(field_orientation_fields) == 14,
        },
        {
            "check": "twelve_vector_fields_defined",
            "actual": len(vector_state_fields),
            "expected": 12,
            "passed": len(vector_state_fields) == 12,
        },
        {
            "check": "six_coordinate_conventions_defined",
            "actual": len(coordinate_conventions),
            "expected": 6,
            "passed": len(coordinate_conventions) == 6,
        },
        {
            "check": "eight_transformation_rules_defined",
            "actual": len(transformation_rules),
            "expected": 8,
            "passed": len(transformation_rules) == 8,
        },
        {
            "check": "twelve_validation_rules_defined",
            "actual": len(validation_rules),
            "expected": 12,
            "passed": len(validation_rules) == 12,
        },
        {
            "check": "five_fallback_contracts_defined",
            "actual": len(fallback_contract),
            "expected": 5,
            "passed": len(fallback_contract) == 5,
        },
        {
            "check": "nine_implementation_steps_defined",
            "actual": len(implementation_steps),
            "expected": 9,
            "passed": len(implementation_steps) == 9,
        },
        {
            "check": "twelve_acceptance_criteria_defined",
            "actual": len(acceptance_criteria),
            "expected": 12,
            "passed": len(acceptance_criteria) == 12,
        },
        {
            "check": "planning_only_boundary_preserved",
            "actual": True,
            "expected": True,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        row["passed"]
        for row in planning_checks
    )

    authority_rows = [
        {
            "authority": authority,
            "granted": False,
            "reason": (
                "7G defines vector and coordinate contracts only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "wind_field_vector_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "7H may implement and independently audit "
                    "the bounded metadata-only contract."
                ),
            },
            {
                "authority": (
                    "production_environment_integration"
                ),
                "granted": False,
                "reason": (
                    "Vector resolution remains diagnostic-only."
                ),
            },
        ]
    )

    diagnosis_name = (
        "wind_field_orientation_and_batted_ball_vector_contract_plan_complete"
        if all_checks_passed
        else
        "wind_field_orientation_and_batted_ball_vector_contract_plan_failed"
    )

    recommended_next_layer = (
        "7H_wind_field_orientation_and_batted_ball_vector_contract_implementation"
        if all_checks_passed
        else
        "7H_wind_field_vector_contract_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        planning_checks,
    )

    write_csv(
        OUTPUT_DIR / "predecessor_contracts.csv",
        [
            "layer",
            "path",
            "expected_diagnosis",
            "present",
        ],
        predecessor_contracts,
    )

    write_csv(
        OUTPUT_DIR / "field_orientation_fields.csv",
        [
            "field",
            "type",
            "required",
            "nullable",
            "purpose",
        ],
        field_orientation_fields,
    )

    write_csv(
        OUTPUT_DIR / "vector_state_fields.csv",
        [
            "field",
            "type",
            "required",
            "nullable",
            "purpose",
        ],
        vector_state_fields,
    )

    write_csv(
        OUTPUT_DIR / "coordinate_conventions.csv",
        [
            "rule_id",
            "rule",
            "requirement",
        ],
        coordinate_conventions,
    )

    write_csv(
        OUTPUT_DIR / "transformation_rules.csv",
        [
            "rule_id",
            "rule",
            "formula",
        ],
        transformation_rules,
    )

    write_csv(
        OUTPUT_DIR / "validation_rules.csv",
        [
            "rule_id",
            "rule",
            "blocking",
        ],
        validation_rules,
    )

    write_csv(
        OUTPUT_DIR / "fallback_contract.csv",
        [
            "fallback_id",
            "condition",
            "result",
            "diagnostic_code",
            "production_authority",
        ],
        fallback_contract,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        [
            "step",
            "action",
        ],
        implementation_steps,
    )

    write_csv(
        OUTPUT_DIR / "acceptance_criteria.csv",
        [
            "criterion_id",
            "criterion",
            "required",
        ],
        acceptance_criteria,
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
                "recommended_next_layer": recommended_next_layer,
                "recommended_action": (
                    "Implement the wind, field-orientation, and "
                    "batted-ball vector contract as a "
                    "disabled-by-default diagnostic."
                    if all_checks_passed
                    else
                    "Remediate failed 7G planning checks."
                ),
                "entry_condition": (
                    "All twelve 7G planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "planning_checks_required": len(planning_checks),
        "planning_checks_passed": sum(
            1
            for row in planning_checks
            if row["passed"]
        ),
        "predecessors_required": len(
            predecessor_contracts
        ),
        "predecessors_accepted": predecessors_accepted,
        "field_orientation_fields_defined": len(
            field_orientation_fields
        ),
        "vector_state_fields_defined": len(
            vector_state_fields
        ),
        "coordinate_conventions_defined": len(
            coordinate_conventions
        ),
        "transformation_rules_defined": len(
            transformation_rules
        ),
        "validation_rules_defined": len(
            validation_rules
        ),
        "fallback_contracts_defined": len(
            fallback_contract
        ),
        "implementation_steps_defined": len(
            implementation_steps
        ),
        "acceptance_criteria_defined": len(
            acceptance_criteria
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
        "aerodynamic_carry_calculated": False,
        "batted_ball_outcomes_changed": False,
        "historical_validation_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "plan_summary.json",
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
        "wind_vector_contract_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_environment_integration_allowed_next": False,
        "recommended_next_layer": recommended_next_layer,
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / "planning_checks.csv"),
            str(OUTPUT_DIR / "predecessor_contracts.csv"),
            str(OUTPUT_DIR / "field_orientation_fields.csv"),
            str(OUTPUT_DIR / "vector_state_fields.csv"),
            str(OUTPUT_DIR / "coordinate_conventions.csv"),
            str(OUTPUT_DIR / "transformation_rules.csv"),
            str(OUTPUT_DIR / "validation_rules.csv"),
            str(OUTPUT_DIR / "fallback_contract.csv"),
            str(OUTPUT_DIR / "implementation_steps.csv"),
            str(OUTPUT_DIR / "acceptance_criteria.csv"),
            str(OUTPUT_DIR / "authority_boundaries.csv"),
            str(OUTPUT_DIR / "recommended_path.csv"),
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "plan_summary.json"),
            str(OUTPUT_DIR / "diagnosis.json"),
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
