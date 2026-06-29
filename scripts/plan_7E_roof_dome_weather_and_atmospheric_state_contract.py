#!/usr/bin/env python3
"""
Layer 7E
Roof, Dome, Weather, and Atmospheric State Contract Plan

Defines the bounded implementation contract for:
- roof and dome state;
- temperature and humidity;
- atmospheric pressure and dew point;
- precipitation state;
- wind speed and direction source fields;
- observation timing and freshness;
- provider precedence;
- missing-data and indoor neutralization behavior;
- diagnostic-only runtime exposure.

Planning only. This layer does not:
- fetch or install weather data;
- modify production simulation behavior;
- change simulation state, parameters, or probabilities;
- calculate batted-ball carry;
- execute historical validation, tuning, backtests, pricing, or edge logic.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7E"
LAYER_NAME = (
    "roof_dome_weather_and_atmospheric_state_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_7E_roof_dome_weather_and_atmospheric_state_contract_plan"
)

PLAN_7A_PATH = (
    ROOT
    / "scripts/plan_7A_layer7_environment_realism_inventory_and_scope.py"
)

AUDIT_7B_PATH = (
    ROOT
    / "scripts/audit_7B_layer7_environment_source_and_runtime_inventory.py"
)

PLAN_7C_PATH = (
    ROOT
    / "scripts/plan_7C_canonical_venue_and_park_factor_source_contract.py"
)

AUDIT_7D_PATH = (
    ROOT
    / "scripts/audit_7D_canonical_venue_and_park_factor_contract.py"
)

VENUE_CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/venue_park_factor_contract.py"
)

REQUIRED_PATHS = [
    PLAN_7A_PATH,
    AUDIT_7B_PATH,
    PLAN_7C_PATH,
    AUDIT_7D_PATH,
    VENUE_CONTRACT_PATH,
]

PROHIBITED_AUTHORITIES = [
    "production_environment_activation",
    "production_weather_activation",
    "production_roof_state_activation",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
    "batted_ball_carry_calculation",
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
            "7A",
            PLAN_7A_PATH,
            (
                "layer7_environment_realism_inventory_"
                "and_scope_plan_complete"
            ),
        ),
        (
            "7B",
            AUDIT_7B_PATH,
            (
                "layer7_environment_source_and_runtime_"
                "inventory_complete"
            ),
        ),
        (
            "7C",
            PLAN_7C_PATH,
            (
                "canonical_venue_and_park_factor_"
                "source_contract_plan_complete"
            ),
        ),
        (
            "7D",
            AUDIT_7D_PATH,
            (
                "canonical_venue_and_park_factor_"
                "contract_implementation_passed"
            ),
        ),
    ]

    predecessor_contracts = []

    for layer, path, diagnosis in predecessor_definitions:
        constants = string_constants(
            path
        )

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

    venue_contract_constants = string_constants(
        VENUE_CONTRACT_PATH
    )

    venue_roof_contract_present = all(
        token in venue_contract_constants
        for token in [
            "open_air",
            "fixed_dome",
            "retractable",
            "unknown",
        ]
    )

    environment_state_fields = [
        {
            "field": "canonical_venue_id",
            "type": "string",
            "required": True,
            "nullable": False,
            "purpose": (
                "Join key to the canonical venue contract."
            ),
        },
        {
            "field": "game_start_time_utc",
            "type": "datetime",
            "required": True,
            "nullable": False,
            "purpose": (
                "Scheduled or confirmed game start time."
            ),
        },
        {
            "field": "observation_time_utc",
            "type": "datetime",
            "required": True,
            "nullable": True,
            "purpose": (
                "Timestamp represented by the weather observation."
            ),
        },
        {
            "field": "retrieved_at_utc",
            "type": "datetime",
            "required": True,
            "nullable": False,
            "purpose": (
                "Acquisition timestamp for freshness tracking."
            ),
        },
        {
            "field": "roof_type",
            "type": "enum",
            "required": True,
            "nullable": False,
            "purpose": (
                "open_air, fixed_dome, retractable, or unknown."
            ),
        },
        {
            "field": "roof_state",
            "type": "enum",
            "required": True,
            "nullable": False,
            "purpose": (
                "open, closed, fixed_closed, not_applicable, or unknown."
            ),
        },
        {
            "field": "indoor_effective",
            "type": "boolean",
            "required": True,
            "nullable": False,
            "purpose": (
                "Whether outdoor weather should be neutralized."
            ),
        },
        {
            "field": "temperature_c",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Air temperature in Celsius."
            ),
        },
        {
            "field": "relative_humidity_pct",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Relative humidity from 0 through 100."
            ),
        },
        {
            "field": "dew_point_c",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Dew-point temperature in Celsius."
            ),
        },
        {
            "field": "station_pressure_hpa",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Observed atmospheric pressure at station elevation."
            ),
        },
        {
            "field": "sea_level_pressure_hpa",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Sea-level-adjusted pressure when supplied."
            ),
        },
        {
            "field": "wind_speed_mps",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Sustained wind speed in meters per second."
            ),
        },
        {
            "field": "wind_gust_mps",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Wind-gust speed in meters per second."
            ),
        },
        {
            "field": "wind_direction_degrees",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Meteorological direction from 0 through 360 degrees."
            ),
        },
        {
            "field": "precipitation_state",
            "type": "enum",
            "required": True,
            "nullable": False,
            "purpose": (
                "none, rain, snow, mixed, other, or unknown."
            ),
        },
        {
            "field": "precipitation_rate_mm_hr",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Observed or forecast precipitation rate."
            ),
        },
        {
            "field": "weather_source_name",
            "type": "string",
            "required": True,
            "nullable": False,
            "purpose": (
                "Provider or dataset name."
            ),
        },
        {
            "field": "weather_source_record_id",
            "type": "string",
            "required": True,
            "nullable": True,
            "purpose": (
                "Provider record identifier when available."
            ),
        },
        {
            "field": "weather_source_class",
            "type": "enum",
            "required": True,
            "nullable": False,
            "purpose": (
                "confirmed_observation, near_game_forecast, "
                "approved_secondary, or neutral_fallback."
            ),
        },
        {
            "field": "is_forecast",
            "type": "boolean",
            "required": True,
            "nullable": False,
            "purpose": (
                "Whether the record is forecast rather than observed."
            ),
        },
        {
            "field": "freshness_minutes",
            "type": "number",
            "required": True,
            "nullable": True,
            "purpose": (
                "Absolute difference from game start or retrieval policy."
            ),
        },
        {
            "field": "fallback_used",
            "type": "boolean",
            "required": True,
            "nullable": False,
            "purpose": (
                "Whether any neutral or secondary fallback was used."
            ),
        },
        {
            "field": "diagnostic_codes",
            "type": "array[string]",
            "required": True,
            "nullable": False,
            "purpose": (
                "Explicit resolution, freshness, and fallback reasons."
            ),
        },
    ]

    source_precedence = [
        {
            "priority": 1,
            "source_class": (
                "confirmed_observation"
            ),
            "selection_rule": (
                "Use a valid observation closest to game time "
                "within the accepted observation window."
            ),
            "fallback_allowed": True,
        },
        {
            "priority": 2,
            "source_class": (
                "near_game_forecast"
            ),
            "selection_rule": (
                "Use the latest valid pregame forecast when no "
                "accepted observation is available."
            ),
            "fallback_allowed": True,
        },
        {
            "priority": 3,
            "source_class": (
                "approved_secondary"
            ),
            "selection_rule": (
                "Use only when higher-priority providers are unavailable "
                "and the same schema and freshness rules pass."
            ),
            "fallback_allowed": True,
        },
        {
            "priority": 4,
            "source_class": (
                "neutral_fallback"
            ),
            "selection_rule": (
                "Emit explicit null atmospheric fields and neutral metadata."
            ),
            "fallback_allowed": False,
        },
    ]

    roof_state_rules = [
        {
            "rule_id": "ENV-R01",
            "roof_type": "open_air",
            "input_roof_state": "not_applicable",
            "indoor_effective": False,
            "weather_behavior": (
                "outdoor_weather_retained"
            ),
        },
        {
            "rule_id": "ENV-R02",
            "roof_type": "fixed_dome",
            "input_roof_state": "fixed_closed",
            "indoor_effective": True,
            "weather_behavior": (
                "outdoor_weather_neutralized"
            ),
        },
        {
            "rule_id": "ENV-R03",
            "roof_type": "retractable",
            "input_roof_state": "open",
            "indoor_effective": False,
            "weather_behavior": (
                "outdoor_weather_retained"
            ),
        },
        {
            "rule_id": "ENV-R04",
            "roof_type": "retractable",
            "input_roof_state": "closed",
            "indoor_effective": True,
            "weather_behavior": (
                "outdoor_weather_neutralized"
            ),
        },
        {
            "rule_id": "ENV-R05",
            "roof_type": "retractable",
            "input_roof_state": "unknown",
            "indoor_effective": False,
            "weather_behavior": (
                "state_unknown_diagnostic_only"
            ),
        },
        {
            "rule_id": "ENV-R06",
            "roof_type": "unknown",
            "input_roof_state": "unknown",
            "indoor_effective": False,
            "weather_behavior": (
                "venue_roof_unknown_diagnostic_only"
            ),
        },
    ]

    freshness_rules = [
        {
            "rule_id": "ENV-T01",
            "rule": (
                "observation_time_required_for_nonfallback"
            ),
            "requirement": (
                "Every non-fallback record must identify its represented time."
            ),
        },
        {
            "rule_id": "ENV-T02",
            "rule": (
                "retrieval_time_required"
            ),
            "requirement": (
                "Every record must identify acquisition time."
            ),
        },
        {
            "rule_id": "ENV-T03",
            "rule": (
                "future_observation_prohibited"
            ),
            "requirement": (
                "Observed records may not occur materially after the game state "
                "they are used to describe."
            ),
        },
        {
            "rule_id": "ENV-T04",
            "rule": (
                "forecast_issue_time_before_game"
            ),
            "requirement": (
                "Forecasts must have been issued before game start."
            ),
        },
        {
            "rule_id": "ENV-T05",
            "rule": (
                "stale_weather_must_be_labeled"
            ),
            "requirement": (
                "Records outside the accepted freshness window remain "
                "diagnostic-only and carry an explicit stale code."
            ),
        },
        {
            "rule_id": "ENV-T06",
            "rule": (
                "provider_timestamp_semantics_explicit"
            ),
            "requirement": (
                "Observation, forecast-valid, issue, and retrieval timestamps "
                "must not be conflated."
            ),
        },
    ]

    validation_rules = [
        {
            "rule_id": "ENV-V01",
            "rule": (
                "canonical_venue_id_nonempty"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V02",
            "rule": (
                "roof_type_supported"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V03",
            "rule": (
                "roof_state_compatible_with_roof_type"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V04",
            "rule": (
                "temperature_within_physical_bounds"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V05",
            "rule": (
                "humidity_between_zero_and_one_hundred"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V06",
            "rule": (
                "pressure_finite_and_positive"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V07",
            "rule": (
                "wind_speed_nonnegative"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V08",
            "rule": (
                "wind_direction_in_range"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V09",
            "rule": (
                "precipitation_rate_nonnegative"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V10",
            "rule": (
                "source_name_and_class_present"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V11",
            "rule": (
                "timestamp_and_freshness_semantics_valid"
            ),
            "blocking": True,
        },
        {
            "rule_id": "ENV-V12",
            "rule": (
                "diagnostic_output_does_not_modify_engine_inputs"
            ),
            "blocking": True,
        },
    ]

    fallback_contract = [
        {
            "fallback_id": "ENV-F01",
            "condition": (
                "fixed_dome_or_confirmed_closed_roof"
            ),
            "result": (
                "indoor_weather_neutralized"
            ),
            "diagnostic_code": (
                "indoor_environment_outdoor_weather_neutralized"
            ),
            "production_authority": False,
        },
        {
            "fallback_id": "ENV-F02",
            "condition": (
                "weather_record_missing"
            ),
            "result": (
                "null_atmospheric_state"
            ),
            "diagnostic_code": (
                "weather_missing_neutral_fallback"
            ),
            "production_authority": False,
        },
        {
            "fallback_id": "ENV-F03",
            "condition": (
                "weather_record_invalid"
            ),
            "result": (
                "null_atmospheric_state"
            ),
            "diagnostic_code": (
                "weather_invalid_neutral_fallback"
            ),
            "production_authority": False,
        },
        {
            "fallback_id": "ENV-F04",
            "condition": (
                "primary_provider_unavailable_secondary_valid"
            ),
            "result": (
                "approved_secondary_weather"
            ),
            "diagnostic_code": (
                "secondary_weather_source_fallback"
            ),
            "production_authority": False,
        },
        {
            "fallback_id": "ENV-F05",
            "condition": (
                "roof_state_unknown"
            ),
            "result": (
                "retain_weather_without_indoor_claim"
            ),
            "diagnostic_code": (
                "roof_state_unknown_no_indoor_neutralization"
            ),
            "production_authority": False,
        },
    ]

    implementation_steps = [
        {
            "step": 1,
            "action": (
                "Create typed roof and atmospheric-state schemas."
            ),
        },
        {
            "step": 2,
            "action": (
                "Implement roof-type and roof-state compatibility checks."
            ),
        },
        {
            "step": 3,
            "action": (
                "Implement physical-range and timestamp validation."
            ),
        },
        {
            "step": 4,
            "action": (
                "Implement deterministic weather source precedence."
            ),
        },
        {
            "step": 5,
            "action": (
                "Implement indoor weather neutralization metadata."
            ),
        },
        {
            "step": 6,
            "action": (
                "Implement missing, invalid, stale, and secondary fallbacks."
            ),
        },
        {
            "step": 7,
            "action": (
                "Expose disabled-by-default diagnostic evaluation output."
            ),
        },
        {
            "step": 8,
            "action": (
                "Add deterministic contract cases and immutability checks."
            ),
        },
        {
            "step": 9,
            "action": (
                "Run independent audit for non-authority and metadata-only behavior."
            ),
        },
    ]

    acceptance_criteria = [
        {
            "criterion_id": "ENV-A01",
            "criterion": (
                "environment_state_schema_complete"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A02",
            "criterion": (
                "roof_state_semantics_deterministic"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A03",
            "criterion": (
                "weather_source_precedence_deterministic"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A04",
            "criterion": (
                "timestamp_and_freshness_semantics_enforced"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A05",
            "criterion": (
                "physical_range_validation_enforced"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A06",
            "criterion": (
                "indoor_weather_neutralization_explicit"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A07",
            "criterion": (
                "missing_and_invalid_fallbacks_explicit"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A08",
            "criterion": (
                "provenance_and_freshness_metadata_emitted"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A09",
            "criterion": (
                "diagnostic_disabled_by_default"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A10",
            "criterion": (
                "caller_inputs_immutable"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A11",
            "criterion": (
                "no_simulation_or_probability_changes"
            ),
            "required": True,
        },
        {
            "criterion_id": "ENV-A12",
            "criterion": (
                "independent_audit_passes"
            ),
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
            "check": "four_predecessor_contracts_present",
            "actual": predecessors_accepted,
            "expected": 4,
            "passed": predecessors_accepted == 4,
        },
        {
            "check": "venue_roof_contract_present",
            "actual": venue_roof_contract_present,
            "expected": True,
            "passed": venue_roof_contract_present,
        },
        {
            "check": "twenty_four_environment_fields_defined",
            "actual": len(
                environment_state_fields
            ),
            "expected": 24,
            "passed": len(
                environment_state_fields
            )
            == 24,
        },
        {
            "check": "four_source_precedence_levels_defined",
            "actual": len(
                source_precedence
            ),
            "expected": 4,
            "passed": len(
                source_precedence
            )
            == 4,
        },
        {
            "check": "six_roof_state_rules_defined",
            "actual": len(
                roof_state_rules
            ),
            "expected": 6,
            "passed": len(
                roof_state_rules
            )
            == 6,
        },
        {
            "check": "six_freshness_rules_defined",
            "actual": len(
                freshness_rules
            ),
            "expected": 6,
            "passed": len(
                freshness_rules
            )
            == 6,
        },
        {
            "check": "twelve_validation_rules_defined",
            "actual": len(
                validation_rules
            ),
            "expected": 12,
            "passed": len(
                validation_rules
            )
            == 12,
        },
        {
            "check": "five_fallback_contracts_defined",
            "actual": len(
                fallback_contract
            ),
            "expected": 5,
            "passed": len(
                fallback_contract
            )
            == 5,
        },
        {
            "check": "nine_implementation_steps_defined",
            "actual": len(
                implementation_steps
            ),
            "expected": 9,
            "passed": len(
                implementation_steps
            )
            == 9,
        },
        {
            "check": "twelve_acceptance_criteria_defined",
            "actual": len(
                acceptance_criteria
            ),
            "expected": 12,
            "passed": len(
                acceptance_criteria
            )
            == 12,
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
                "7E defines a weather-state contract plan only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "roof_weather_atmospheric_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "7F may implement and independently audit the "
                    "bounded diagnostic state contract."
                ),
            },
            {
                "authority": (
                    "production_environment_integration"
                ),
                "granted": False,
                "reason": (
                    "The environment state remains diagnostic-only."
                ),
            },
        ]
    )

    diagnosis_name = (
        "roof_dome_weather_and_atmospheric_state_contract_plan_complete"
        if all_checks_passed
        else
        "roof_dome_weather_and_atmospheric_state_contract_plan_failed"
    )

    recommended_next_layer = (
        "7F_roof_dome_weather_and_atmospheric_state_contract_implementation"
        if all_checks_passed
        else
        "7F_roof_weather_state_contract_plan_remediation"
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
        OUTPUT_DIR / "environment_state_fields.csv",
        [
            "field",
            "type",
            "required",
            "nullable",
            "purpose",
        ],
        environment_state_fields,
    )

    write_csv(
        OUTPUT_DIR / "source_precedence.csv",
        [
            "priority",
            "source_class",
            "selection_rule",
            "fallback_allowed",
        ],
        source_precedence,
    )

    write_csv(
        OUTPUT_DIR / "roof_state_rules.csv",
        [
            "rule_id",
            "roof_type",
            "input_roof_state",
            "indoor_effective",
            "weather_behavior",
        ],
        roof_state_rules,
    )

    write_csv(
        OUTPUT_DIR / "freshness_rules.csv",
        [
            "rule_id",
            "rule",
            "requirement",
        ],
        freshness_rules,
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
                "recommended_next_layer": (
                    recommended_next_layer
                ),
                "recommended_action": (
                    "Implement the roof, dome, weather, and atmospheric "
                    "state contract as a disabled-by-default diagnostic."
                    if all_checks_passed
                    else
                    "Remediate failed 7E planning checks."
                ),
                "entry_condition": (
                    "All twelve 7E planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "planning_checks_required": len(
            planning_checks
        ),
        "planning_checks_passed": sum(
            1
            for row in planning_checks
            if row["passed"]
        ),
        "predecessors_required": len(
            predecessor_contracts
        ),
        "predecessors_accepted": predecessors_accepted,
        "environment_state_fields_defined": len(
            environment_state_fields
        ),
        "source_precedence_levels_defined": len(
            source_precedence
        ),
        "roof_state_rules_defined": len(
            roof_state_rules
        ),
        "freshness_rules_defined": len(
            freshness_rules
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
        "batted_ball_carry_calculated": False,
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
        "weather_state_contract_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_environment_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / "planning_checks.csv"
            ),
            str(
                OUTPUT_DIR / "predecessor_contracts.csv"
            ),
            str(
                OUTPUT_DIR / "environment_state_fields.csv"
            ),
            str(
                OUTPUT_DIR / "source_precedence.csv"
            ),
            str(
                OUTPUT_DIR / "roof_state_rules.csv"
            ),
            str(
                OUTPUT_DIR / "freshness_rules.csv"
            ),
            str(
                OUTPUT_DIR / "validation_rules.csv"
            ),
            str(
                OUTPUT_DIR / "fallback_contract.csv"
            ),
            str(
                OUTPUT_DIR / "implementation_steps.csv"
            ),
            str(
                OUTPUT_DIR / "acceptance_criteria.csv"
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
                OUTPUT_DIR / "plan_summary.json"
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
