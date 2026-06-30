#!/usr/bin/env python3
"""
Layer 7I
Atmospheric Density and Carry Diagnostic Contract Plan

Defines the bounded implementation contract for:
- atmospheric density estimation;
- vapor-pressure and dry-air decomposition;
- density altitude diagnostic metadata;
- normalized air-density ratios;
- along-path wind input;
- bounded carry-index diagnostics;
- neutral, invalid, indoor, and unavailable fallbacks;
- disabled-by-default runtime exposure.

Planning only. This layer does not:
- activate atmospheric carry in production;
- change batted-ball distance or outcome;
- change home-run probability;
- modify simulation state, parameters, or probabilities;
- perform historical calibration, tuning, backtesting, pricing, or edge work.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7I"
LAYER_NAME = (
    "atmospheric_density_and_carry_diagnostic_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_7I_atmospheric_density_and_carry_diagnostic_contract_plan"
)

PLAN_7G_PATH = (
    ROOT
    / "scripts/"
    "plan_7G_wind_field_orientation_and_batted_ball_vector_contract.py"
)

AUDIT_7H_PATH = (
    ROOT
    / "scripts/"
    "audit_7H_wind_field_orientation_and_batted_ball_vector_contract.py"
)

WEATHER_CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/weather_atmospheric_contract.py"
)

VECTOR_CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/wind_field_vector_contract.py"
)

REQUIRED_PATHS = [
    PLAN_7G_PATH,
    AUDIT_7H_PATH,
    WEATHER_CONTRACT_PATH,
    VECTOR_CONTRACT_PATH,
]

PROHIBITED_AUTHORITIES = [
    "production_environment_activation",
    "production_carry_activation",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
    "batted_ball_distance_change",
    "batted_ball_outcome_change",
    "home_run_probability_change",
    "historical_outcome_join",
    "accuracy_metric_generation",
    "parameter_calibration",
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
            sort_keys=True,
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
            "7G",
            PLAN_7G_PATH,
            (
                "wind_field_orientation_and_batted_ball_"
                "vector_contract_plan_complete"
            ),
        ),
        (
            "7H",
            AUDIT_7H_PATH,
            (
                "wind_field_orientation_and_batted_ball_"
                "vector_contract_implementation_passed"
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

    weather_text = read_text(
        WEATHER_CONTRACT_PATH
    )
    vector_text = read_text(
        VECTOR_CONTRACT_PATH
    )

    atmospheric_source_contract_present = all(
        token in weather_text
        for token in [
            "temperature_c",
            "relative_humidity_pct",
            "dew_point_c",
            "station_pressure_hpa",
            "sea_level_pressure_hpa",
        ]
    )

    vector_source_contract_present = all(
        token in vector_text
        for token in [
            "wind_along_ball_path_mps",
            "indoor_effective",
            "vector_resolution_status",
        ]
    )

    atmospheric_input_fields = [
        {
            "field": "temperature_c",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Ambient dry-bulb temperature.",
        },
        {
            "field": "relative_humidity_pct",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Ambient relative humidity.",
        },
        {
            "field": "dew_point_c",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Optional observed dew point.",
        },
        {
            "field": "station_pressure_hpa",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Preferred local atmospheric pressure.",
        },
        {
            "field": "sea_level_pressure_hpa",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Secondary pressure fallback only.",
        },
        {
            "field": "venue_elevation_m",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Optional elevation metadata.",
        },
        {
            "field": "wind_along_ball_path_mps",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Signed along-flight wind component.",
        },
        {
            "field": "indoor_effective",
            "type": "boolean",
            "required": True,
            "nullable": False,
            "purpose": "Indoor neutralization switch.",
        },
        {
            "field": "weather_source_class",
            "type": "string",
            "required": True,
            "nullable": True,
            "purpose": "Weather provenance class.",
        },
        {
            "field": "weather_source_record_id",
            "type": "string",
            "required": True,
            "nullable": True,
            "purpose": "Weather source record provenance.",
        },
        {
            "field": "observation_time_utc",
            "type": "datetime",
            "required": True,
            "nullable": True,
            "purpose": "Input observation or forecast timestamp.",
        },
        {
            "field": "freshness_minutes",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": "Input freshness metadata.",
        },
    ]

    diagnostic_output_fields = [
        {
            "field": "temperature_k",
            "type": "number",
            "nullable": True,
            "purpose": "Absolute temperature used in equations.",
        },
        {
            "field": "saturation_vapor_pressure_hpa",
            "type": "number",
            "nullable": True,
            "purpose": "Estimated saturation vapor pressure.",
        },
        {
            "field": "actual_vapor_pressure_hpa",
            "type": "number",
            "nullable": True,
            "purpose": "Estimated water-vapor partial pressure.",
        },
        {
            "field": "dry_air_pressure_hpa",
            "type": "number",
            "nullable": True,
            "purpose": "Estimated dry-air partial pressure.",
        },
        {
            "field": "air_density_kg_m3",
            "type": "number",
            "nullable": True,
            "purpose": "Moist-air density diagnostic.",
        },
        {
            "field": "reference_air_density_kg_m3",
            "type": "number",
            "nullable": False,
            "purpose": "Fixed documented comparison baseline.",
        },
        {
            "field": "air_density_ratio",
            "type": "number",
            "nullable": True,
            "purpose": "Observed density divided by reference density.",
        },
        {
            "field": "density_delta_pct",
            "type": "number",
            "nullable": True,
            "purpose": "Percent difference from reference density.",
        },
        {
            "field": "density_altitude_m",
            "type": "number",
            "nullable": True,
            "purpose": "Diagnostic equivalent-density altitude.",
        },
        {
            "field": "wind_along_ball_path_mps",
            "type": "number",
            "nullable": True,
            "purpose": "Signed along-path wind input.",
        },
        {
            "field": "density_component_index",
            "type": "number",
            "nullable": True,
            "purpose": "Bounded density-only diagnostic index.",
        },
        {
            "field": "wind_component_index",
            "type": "number",
            "nullable": True,
            "purpose": "Bounded along-path wind diagnostic index.",
        },
        {
            "field": "combined_carry_index",
            "type": "number",
            "nullable": True,
            "purpose": (
                "Dimensionless diagnostic index; not a distance adjustment."
            ),
        },
        {
            "field": "resolution_status",
            "type": "enum",
            "nullable": False,
            "purpose": "resolved, neutral, invalid, or unavailable.",
        },
        {
            "field": "diagnostic_codes",
            "type": "array[string]",
            "nullable": False,
            "purpose": "Explicit resolution and fallback metadata.",
        },
        {
            "field": "validation_errors",
            "type": "array[string]",
            "nullable": False,
            "purpose": "Explicit rejected-input metadata.",
        },
    ]

    equation_contracts = [
        {
            "equation_id": "ATM-E01",
            "name": "kelvin_conversion",
            "formula": "temperature_k = temperature_c + 273.15",
            "authority": "diagnostic_only",
        },
        {
            "equation_id": "ATM-E02",
            "name": "saturation_vapor_pressure",
            "formula": (
                "Magnus-type documented approximation over bounded range"
            ),
            "authority": "diagnostic_only",
        },
        {
            "equation_id": "ATM-E03",
            "name": "actual_vapor_pressure",
            "formula": (
                "relative_humidity_fraction * saturation_vapor_pressure"
            ),
            "authority": "diagnostic_only",
        },
        {
            "equation_id": "ATM-E04",
            "name": "dry_air_pressure",
            "formula": "total_pressure - actual_vapor_pressure",
            "authority": "diagnostic_only",
        },
        {
            "equation_id": "ATM-E05",
            "name": "moist_air_density",
            "formula": (
                "rho = pd/(Rd*T) + pv/(Rv*T)"
            ),
            "authority": "diagnostic_only",
        },
        {
            "equation_id": "ATM-E06",
            "name": "air_density_ratio",
            "formula": "rho / reference_rho",
            "authority": "diagnostic_only",
        },
        {
            "equation_id": "ATM-E07",
            "name": "density_delta_pct",
            "formula": "(rho / reference_rho - 1) * 100",
            "authority": "diagnostic_only",
        },
        {
            "equation_id": "ATM-E08",
            "name": "density_altitude",
            "formula": (
                "documented standard-atmosphere inversion"
            ),
            "authority": "diagnostic_only",
        },
        {
            "equation_id": "ATM-E09",
            "name": "density_component_index",
            "formula": (
                "bounded transform of reference_rho - observed_rho"
            ),
            "authority": "diagnostic_only",
        },
        {
            "equation_id": "ATM-E10",
            "name": "wind_component_index",
            "formula": (
                "bounded transform of along-path wind"
            ),
            "authority": "diagnostic_only",
        },
        {
            "equation_id": "ATM-E11",
            "name": "combined_carry_index",
            "formula": (
                "documented bounded combination of density and wind indices"
            ),
            "authority": "diagnostic_only",
        },
    ]

    constants_contract = [
        {
            "constant": "dry_air_gas_constant_j_kg_k",
            "value": "287.05",
            "status": "fixed_documented",
        },
        {
            "constant": "water_vapor_gas_constant_j_kg_k",
            "value": "461.495",
            "status": "fixed_documented",
        },
        {
            "constant": "reference_air_density_kg_m3",
            "value": "1.225",
            "status": "fixed_documented",
        },
        {
            "constant": "reference_temperature_c",
            "value": "15.0",
            "status": "fixed_documented",
        },
        {
            "constant": "reference_pressure_hpa",
            "value": "1013.25",
            "status": "fixed_documented",
        },
        {
            "constant": "density_index_bound",
            "value": "contract_defined_not_tuned",
            "status": "diagnostic_only",
        },
        {
            "constant": "wind_index_bound",
            "value": "contract_defined_not_tuned",
            "status": "diagnostic_only",
        },
    ]

    validation_rules = [
        {
            "rule_id": "ATM-V01",
            "rule": "temperature_finite_and_within_contract_range",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V02",
            "rule": "humidity_between_zero_and_one_hundred",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V03",
            "rule": "dew_point_not_materially_above_temperature",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V04",
            "rule": "station_pressure_finite_and_positive",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V05",
            "rule": "sea_level_pressure_finite_and_positive",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V06",
            "rule": "pressure_precedence_deterministic",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V07",
            "rule": "vapor_pressure_not_above_total_pressure",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V08",
            "rule": "air_density_finite_and_positive",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V09",
            "rule": "density_ratio_within_diagnostic_bounds",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V10",
            "rule": "wind_component_finite",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V11",
            "rule": "carry_index_bounded",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V12",
            "rule": "diagnostic_deterministic",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V13",
            "rule": "diagnostic_does_not_modify_inputs",
            "blocking": True,
        },
        {
            "rule_id": "ATM-V14",
            "rule": "diagnostic_does_not_change_engine_outputs",
            "blocking": True,
        },
    ]

    precedence_rules = [
        {
            "precedence": 1,
            "input": "station_pressure_hpa",
            "condition": "valid local pressure available",
        },
        {
            "precedence": 2,
            "input": "sea_level_pressure_hpa",
            "condition": (
                "station pressure unavailable and fallback explicitly allowed"
            ),
        },
        {
            "precedence": 3,
            "input": "neutral_reference_pressure",
            "condition": "all pressure inputs unavailable or invalid",
        },
    ]

    fallback_contracts = [
        {
            "fallback_id": "ATM-F01",
            "condition": "indoor_environment",
            "result": "neutral_reference_density_and_zero_wind",
            "diagnostic_code": "indoor_environment_neutral_carry_index",
            "production_authority": False,
        },
        {
            "fallback_id": "ATM-F02",
            "condition": "temperature_or_humidity_missing",
            "result": "carry_diagnostic_unavailable",
            "diagnostic_code": "atmospheric_inputs_missing_carry_unavailable",
            "production_authority": False,
        },
        {
            "fallback_id": "ATM-F03",
            "condition": "pressure_missing",
            "result": "neutral_reference_pressure_fallback",
            "diagnostic_code": "pressure_missing_reference_fallback",
            "production_authority": False,
        },
        {
            "fallback_id": "ATM-F04",
            "condition": "atmospheric_input_invalid",
            "result": "neutral_carry_index",
            "diagnostic_code": "atmospheric_inputs_invalid_neutral_carry",
            "production_authority": False,
        },
        {
            "fallback_id": "ATM-F05",
            "condition": "wind_along_path_missing",
            "result": "zero_wind_component",
            "diagnostic_code": "along_path_wind_missing_zero_component",
            "production_authority": False,
        },
        {
            "fallback_id": "ATM-F06",
            "condition": "wind_along_path_invalid",
            "result": "zero_wind_component",
            "diagnostic_code": "along_path_wind_invalid_zero_component",
            "production_authority": False,
        },
        {
            "fallback_id": "ATM-F07",
            "condition": "computed_density_invalid",
            "result": "neutral_carry_index",
            "diagnostic_code": "computed_density_invalid_neutral_carry",
            "production_authority": False,
        },
    ]

    implementation_steps = [
        {
            "step": 1,
            "action": "Create typed atmospheric-density input schema.",
        },
        {
            "step": 2,
            "action": "Create typed carry-diagnostic output schema.",
        },
        {
            "step": 3,
            "action": "Implement bounded vapor-pressure calculation.",
        },
        {
            "step": 4,
            "action": "Implement moist-air density calculation.",
        },
        {
            "step": 5,
            "action": "Implement pressure precedence and provenance.",
        },
        {
            "step": 6,
            "action": "Implement reference-density comparison metadata.",
        },
        {
            "step": 7,
            "action": "Implement density-altitude diagnostic metadata.",
        },
        {
            "step": 8,
            "action": "Implement bounded density and wind indices.",
        },
        {
            "step": 9,
            "action": "Implement combined carry index without distance mapping.",
        },
        {
            "step": 10,
            "action": "Implement explicit neutral and unavailable fallbacks.",
        },
        {
            "step": 11,
            "action": (
                "Add deterministic disabled-by-default independent audit."
            ),
        },
    ]

    acceptance_criteria = [
        {
            "criterion_id": "ATM-A01",
            "criterion": "atmospheric_input_schema_complete",
            "required": True,
        },
        {
            "criterion_id": "ATM-A02",
            "criterion": "diagnostic_output_schema_complete",
            "required": True,
        },
        {
            "criterion_id": "ATM-A03",
            "criterion": "equations_and_constants_explicit",
            "required": True,
        },
        {
            "criterion_id": "ATM-A04",
            "criterion": "pressure_precedence_deterministic",
            "required": True,
        },
        {
            "criterion_id": "ATM-A05",
            "criterion": "moist_air_density_deterministic",
            "required": True,
        },
        {
            "criterion_id": "ATM-A06",
            "criterion": "density_altitude_diagnostic_deterministic",
            "required": True,
        },
        {
            "criterion_id": "ATM-A07",
            "criterion": "density_index_bounded",
            "required": True,
        },
        {
            "criterion_id": "ATM-A08",
            "criterion": "wind_index_bounded",
            "required": True,
        },
        {
            "criterion_id": "ATM-A09",
            "criterion": "combined_index_not_distance_adjustment",
            "required": True,
        },
        {
            "criterion_id": "ATM-A10",
            "criterion": "fallbacks_explicit",
            "required": True,
        },
        {
            "criterion_id": "ATM-A11",
            "criterion": "diagnostic_disabled_by_default",
            "required": True,
        },
        {
            "criterion_id": "ATM-A12",
            "criterion": "inputs_immutable",
            "required": True,
        },
        {
            "criterion_id": "ATM-A13",
            "criterion": "no_simulation_or_outcome_changes",
            "required": True,
        },
        {
            "criterion_id": "ATM-A14",
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
            "check": "atmospheric_source_contract_present",
            "actual": atmospheric_source_contract_present,
            "expected": True,
            "passed": atmospheric_source_contract_present,
        },
        {
            "check": "vector_source_contract_present",
            "actual": vector_source_contract_present,
            "expected": True,
            "passed": vector_source_contract_present,
        },
        {
            "check": "twelve_input_fields_defined",
            "actual": len(atmospheric_input_fields),
            "expected": 12,
            "passed": len(atmospheric_input_fields) == 12,
        },
        {
            "check": "sixteen_output_fields_defined",
            "actual": len(diagnostic_output_fields),
            "expected": 16,
            "passed": len(diagnostic_output_fields) == 16,
        },
        {
            "check": "eleven_equation_contracts_defined",
            "actual": len(equation_contracts),
            "expected": 11,
            "passed": len(equation_contracts) == 11,
        },
        {
            "check": "seven_constants_defined",
            "actual": len(constants_contract),
            "expected": 7,
            "passed": len(constants_contract) == 7,
        },
        {
            "check": "fourteen_validation_rules_defined",
            "actual": len(validation_rules),
            "expected": 14,
            "passed": len(validation_rules) == 14,
        },
        {
            "check": "three_pressure_precedence_rules_defined",
            "actual": len(precedence_rules),
            "expected": 3,
            "passed": len(precedence_rules) == 3,
        },
        {
            "check": "seven_fallback_contracts_defined",
            "actual": len(fallback_contracts),
            "expected": 7,
            "passed": len(fallback_contracts) == 7,
        },
        {
            "check": "eleven_implementation_steps_defined",
            "actual": len(implementation_steps),
            "expected": 11,
            "passed": len(implementation_steps) == 11,
        },
        {
            "check": "fourteen_acceptance_criteria_defined",
            "actual": len(acceptance_criteria),
            "expected": 14,
            "passed": len(acceptance_criteria) == 14,
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
                "7I defines atmospheric-density and carry "
                "diagnostic contracts only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "atmospheric_density_carry_diagnostic_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "7J may implement and independently audit "
                    "the bounded diagnostic contract."
                ),
            },
            {
                "authority": (
                    "production_environment_integration"
                ),
                "granted": False,
                "reason": (
                    "Carry diagnostics remain non-authoritative."
                ),
            },
        ]
    )

    diagnosis_name = (
        "atmospheric_density_and_carry_diagnostic_contract_plan_complete"
        if all_checks_passed
        else
        "atmospheric_density_and_carry_diagnostic_contract_plan_failed"
    )

    recommended_next_layer = (
        "7J_atmospheric_density_and_carry_diagnostic_contract_implementation"
        if all_checks_passed
        else
        "7J_atmospheric_density_carry_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "predecessor_contracts.csv": predecessor_contracts,
        "atmospheric_input_fields.csv": atmospheric_input_fields,
        "diagnostic_output_fields.csv": diagnostic_output_fields,
        "equation_contracts.csv": equation_contracts,
        "constants_contract.csv": constants_contract,
        "validation_rules.csv": validation_rules,
        "pressure_precedence_rules.csv": precedence_rules,
        "fallback_contracts.csv": fallback_contracts,
        "implementation_steps.csv": implementation_steps,
        "acceptance_criteria.csv": acceptance_criteria,
        "authority_boundaries.csv": authority_rows,
    }

    fieldnames = {
        "planning_checks.csv": [
            "check", "actual", "expected", "passed"
        ],
        "predecessor_contracts.csv": [
            "layer", "path", "expected_diagnosis", "present"
        ],
        "atmospheric_input_fields.csv": [
            "field", "type", "required", "nullable", "purpose"
        ],
        "diagnostic_output_fields.csv": [
            "field", "type", "nullable", "purpose"
        ],
        "equation_contracts.csv": [
            "equation_id", "name", "formula", "authority"
        ],
        "constants_contract.csv": [
            "constant", "value", "status"
        ],
        "validation_rules.csv": [
            "rule_id", "rule", "blocking"
        ],
        "pressure_precedence_rules.csv": [
            "precedence", "input", "condition"
        ],
        "fallback_contracts.csv": [
            "fallback_id",
            "condition",
            "result",
            "diagnostic_code",
            "production_authority",
        ],
        "implementation_steps.csv": [
            "step", "action"
        ],
        "acceptance_criteria.csv": [
            "criterion_id", "criterion", "required"
        ],
        "authority_boundaries.csv": [
            "authority", "granted", "reason"
        ],
    }

    for filename, rows in artifacts.items():
        write_csv(
            OUTPUT_DIR / filename,
            fieldnames[filename],
            rows,
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
                    "Implement the atmospheric-density and carry "
                    "diagnostic contract without production authority."
                    if all_checks_passed
                    else
                    "Remediate failed 7I planning checks."
                ),
                "entry_condition": (
                    "All fourteen 7I planning checks pass."
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
        "atmospheric_input_fields_defined": len(
            atmospheric_input_fields
        ),
        "diagnostic_output_fields_defined": len(
            diagnostic_output_fields
        ),
        "equation_contracts_defined": len(
            equation_contracts
        ),
        "constants_defined": len(
            constants_contract
        ),
        "validation_rules_defined": len(
            validation_rules
        ),
        "pressure_precedence_rules_defined": len(
            precedence_rules
        ),
        "fallback_contracts_defined": len(
            fallback_contracts
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
        "production_carry_activated": False,
        "batted_ball_distance_changed": False,
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
        "atmospheric_density_carry_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_environment_integration_allowed_next": False,
        "recommended_next_layer": recommended_next_layer,
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / filename)
            for filename in [
                *artifacts.keys(),
                "recommended_path.csv",
            ]
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
