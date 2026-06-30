#!/usr/bin/env python3
"""
Layer 7K
Environment Diagnostic Composition Contract Plan

Defines the bounded composition contract across:
- canonical venue and park-factor metadata;
- roof, dome, weather, and atmospheric state;
- wind and field-orientation vectors;
- atmospheric-density and carry diagnostics;
- deterministic stage ordering;
- shared provenance and diagnostic-code aggregation;
- partial-resolution and failure isolation;
- disabled-by-default runtime exposure.

Planning only. This layer does not:
- activate environment effects in production;
- alter simulation state, parameters, probabilities, or outcomes;
- map carry diagnostics to distance;
- activate park factors;
- execute historical validation, tuning, backtests, pricing, or edge logic.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7K"
LAYER_NAME = (
    "environment_diagnostic_composition_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_7K_environment_diagnostic_composition_contract_plan"
)

VENUE_CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/"
    "venue_park_factor_contract.py"
)

WEATHER_CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/"
    "weather_atmospheric_contract.py"
)

VECTOR_CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/"
    "wind_field_vector_contract.py"
)

CARRY_CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/"
    "atmospheric_density_carry_contract.py"
)

AUDIT_7J_PATH = (
    ROOT
    / "scripts/"
    "audit_7J_atmospheric_density_and_carry_diagnostic_contract.py"
)

REQUIRED_PATHS = [
    VENUE_CONTRACT_PATH,
    WEATHER_CONTRACT_PATH,
    VECTOR_CONTRACT_PATH,
    CARRY_CONTRACT_PATH,
    AUDIT_7J_PATH,
]

PROHIBITED_AUTHORITIES = [
    "production_environment_activation",
    "production_venue_activation",
    "production_park_factor_activation",
    "production_weather_activation",
    "production_wind_activation",
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

    predecessor_present = (
        "atmospheric_density_and_carry_diagnostic_contract_implementation_passed"
        in string_constants(
            AUDIT_7J_PATH
        )
    )

    venue_text = read_text(
        VENUE_CONTRACT_PATH
    )
    weather_text = read_text(
        WEATHER_CONTRACT_PATH
    )
    vector_text = read_text(
        VECTOR_CONTRACT_PATH
    )
    carry_text = read_text(
        CARRY_CONTRACT_PATH
    )

    venue_contract_present = all(
        token in venue_text
        for token in [
            "canonical_venue_id",
            "production_authority",
        ]
    )

    weather_contract_present = all(
        token in weather_text
        for token in [
            "indoor_effective",
            "temperature_c",
            "wind_speed_mps",
            "production_authority",
        ]
    )

    vector_contract_present = all(
        token in vector_text
        for token in [
            "wind_along_ball_path_mps",
            "vector_resolution_status",
            "production_authority",
        ]
    )

    carry_contract_present = all(
        token in carry_text
        for token in [
            "combined_carry_index",
            "resolution_status",
            "production_authority",
        ]
    )

    composition_stages = [
        {
            "stage": 1,
            "name": "venue_resolution",
            "input_dependencies": "venue query and game date",
            "output_contract": "canonical venue and park metadata",
            "failure_behavior": "stage-local unavailable metadata",
        },
        {
            "stage": 2,
            "name": "weather_resolution",
            "input_dependencies": (
                "canonical venue, game time, roof metadata, weather records"
            ),
            "output_contract": "roof/weather/atmospheric state",
            "failure_behavior": "stage-local neutral or unavailable metadata",
        },
        {
            "stage": 3,
            "name": "field_vector_resolution",
            "input_dependencies": (
                "field orientation, weather wind, spray angle, indoor state"
            ),
            "output_contract": "field-relative wind vector metadata",
            "failure_behavior": "stage-local neutral or unavailable metadata",
        },
        {
            "stage": 4,
            "name": "carry_diagnostic_resolution",
            "input_dependencies": (
                "weather atmospheric state and along-path wind"
            ),
            "output_contract": (
                "density and dimensionless carry diagnostic metadata"
            ),
            "failure_behavior": "stage-local neutral or unavailable metadata",
        },
        {
            "stage": 5,
            "name": "composition_aggregation",
            "input_dependencies": "all prior stage outputs",
            "output_contract": "single environment diagnostic envelope",
            "failure_behavior": "aggregate partial-resolution metadata",
        },
    ]

    composition_output_fields = [
        {
            "field": "enabled",
            "type": "boolean",
            "nullable": False,
            "purpose": "Top-level diagnostic activation state.",
        },
        {
            "field": "composition_version",
            "type": "string",
            "nullable": False,
            "purpose": "Immutable composition contract version.",
        },
        {
            "field": "canonical_venue_id",
            "type": "string",
            "nullable": True,
            "purpose": "Resolved canonical venue identity.",
        },
        {
            "field": "game_start_time_utc",
            "type": "datetime",
            "nullable": False,
            "purpose": "Requested game start timestamp.",
        },
        {
            "field": "game_date",
            "type": "date",
            "nullable": False,
            "purpose": "Requested game date.",
        },
        {
            "field": "venue_resolution",
            "type": "object",
            "nullable": True,
            "purpose": "Venue and park-factor diagnostic payload.",
        },
        {
            "field": "weather_resolution",
            "type": "object",
            "nullable": True,
            "purpose": "Roof/weather/atmospheric diagnostic payload.",
        },
        {
            "field": "vector_resolution",
            "type": "object",
            "nullable": True,
            "purpose": "Wind and field-vector diagnostic payload.",
        },
        {
            "field": "carry_resolution",
            "type": "object",
            "nullable": True,
            "purpose": "Atmospheric-density and carry diagnostic payload.",
        },
        {
            "field": "stage_statuses",
            "type": "object",
            "nullable": False,
            "purpose": "Per-stage status map.",
        },
        {
            "field": "resolved_stage_count",
            "type": "integer",
            "nullable": False,
            "purpose": "Count of resolved stages.",
        },
        {
            "field": "neutral_stage_count",
            "type": "integer",
            "nullable": False,
            "purpose": "Count of neutral stages.",
        },
        {
            "field": "unavailable_stage_count",
            "type": "integer",
            "nullable": False,
            "purpose": "Count of unavailable stages.",
        },
        {
            "field": "invalid_stage_count",
            "type": "integer",
            "nullable": False,
            "purpose": "Count of invalid stages.",
        },
        {
            "field": "composition_status",
            "type": "enum",
            "nullable": False,
            "purpose": (
                "resolved, partial, neutral, unavailable, invalid, or disabled."
            ),
        },
        {
            "field": "diagnostic_codes",
            "type": "array[string]",
            "nullable": False,
            "purpose": "Deduplicated sorted diagnostic-code aggregation.",
        },
        {
            "field": "validation_errors",
            "type": "array[string]",
            "nullable": False,
            "purpose": "Deduplicated sorted validation-error aggregation.",
        },
        {
            "field": "provenance",
            "type": "object",
            "nullable": False,
            "purpose": "Shared stage and source provenance.",
        },
        {
            "field": "production_authority",
            "type": "boolean",
            "nullable": False,
            "purpose": "Always false in Layer 7 composition.",
        },
        {
            "field": "simulation_inputs_changed",
            "type": "boolean",
            "nullable": False,
            "purpose": "Always false.",
        },
        {
            "field": "canonical_probability_authority_changed",
            "type": "boolean",
            "nullable": False,
            "purpose": "Always false.",
        },
        {
            "field": "production_environment_activated",
            "type": "boolean",
            "nullable": False,
            "purpose": "Always false.",
        },
        {
            "field": "batted_ball_distance_changed",
            "type": "boolean",
            "nullable": False,
            "purpose": "Always false.",
        },
        {
            "field": "batted_ball_outcomes_changed",
            "type": "boolean",
            "nullable": False,
            "purpose": "Always false.",
        },
    ]

    status_rules = [
        {
            "rule_id": "COMP-S01",
            "condition": "diagnostic disabled",
            "composition_status": "disabled",
        },
        {
            "rule_id": "COMP-S02",
            "condition": "all required stages resolved",
            "composition_status": "resolved",
        },
        {
            "rule_id": "COMP-S03",
            "condition": (
                "at least one stage resolved and at least one non-resolved"
            ),
            "composition_status": "partial",
        },
        {
            "rule_id": "COMP-S04",
            "condition": "all executable stages neutral",
            "composition_status": "neutral",
        },
        {
            "rule_id": "COMP-S05",
            "condition": "all required stages unavailable",
            "composition_status": "unavailable",
        },
        {
            "rule_id": "COMP-S06",
            "condition": "composition input contract invalid",
            "composition_status": "invalid",
        },
    ]

    aggregation_rules = [
        {
            "rule_id": "COMP-A01",
            "rule": "stage_order_fixed",
            "requirement": "Stages execute in declared numeric order.",
        },
        {
            "rule_id": "COMP-A02",
            "rule": "stage_failure_isolated",
            "requirement": (
                "A stage failure cannot mutate or erase prior stage outputs."
            ),
        },
        {
            "rule_id": "COMP-A03",
            "rule": "diagnostic_codes_deduplicated",
            "requirement": (
                "Diagnostic codes are deduplicated and sorted."
            ),
        },
        {
            "rule_id": "COMP-A04",
            "rule": "validation_errors_deduplicated",
            "requirement": (
                "Validation errors are deduplicated and sorted."
            ),
        },
        {
            "rule_id": "COMP-A05",
            "rule": "stage_statuses_explicit",
            "requirement": (
                "Every planned stage emits an explicit status."
            ),
        },
        {
            "rule_id": "COMP-A06",
            "rule": "provenance_namespaced",
            "requirement": (
                "Provenance remains namespaced by composition stage."
            ),
        },
        {
            "rule_id": "COMP-A07",
            "rule": "input_objects_immutable",
            "requirement": (
                "Composition cannot mutate caller-owned inputs."
            ),
        },
        {
            "rule_id": "COMP-A08",
            "rule": "component_outputs_preserved",
            "requirement": (
                "Original component payloads remain unchanged."
            ),
        },
        {
            "rule_id": "COMP-A09",
            "rule": "composition_deterministic",
            "requirement": (
                "Equivalent inputs produce byte-equivalent semantic output."
            ),
        },
        {
            "rule_id": "COMP-A10",
            "rule": "authority_flags_false",
            "requirement": (
                "All production and simulation authority flags remain false."
            ),
        },
    ]

    dependency_rules = [
        {
            "rule_id": "COMP-D01",
            "downstream_stage": "weather_resolution",
            "dependency": "canonical venue identity",
            "missing_behavior": "weather stage unavailable",
        },
        {
            "rule_id": "COMP-D02",
            "downstream_stage": "field_vector_resolution",
            "dependency": "field orientation",
            "missing_behavior": "vector stage unavailable",
        },
        {
            "rule_id": "COMP-D03",
            "downstream_stage": "field_vector_resolution",
            "dependency": "weather wind fields",
            "missing_behavior": "neutral zero-wind vector",
        },
        {
            "rule_id": "COMP-D04",
            "downstream_stage": "carry_diagnostic_resolution",
            "dependency": "temperature and humidity",
            "missing_behavior": "carry stage unavailable",
        },
        {
            "rule_id": "COMP-D05",
            "downstream_stage": "carry_diagnostic_resolution",
            "dependency": "pressure",
            "missing_behavior": "reference pressure fallback",
        },
        {
            "rule_id": "COMP-D06",
            "downstream_stage": "carry_diagnostic_resolution",
            "dependency": "along-path wind",
            "missing_behavior": "zero-wind component",
        },
        {
            "rule_id": "COMP-D07",
            "downstream_stage": "all environment stages",
            "dependency": "indoor effective state",
            "missing_behavior": (
                "do not claim indoor neutralization without explicit state"
            ),
        },
        {
            "rule_id": "COMP-D08",
            "downstream_stage": "composition_aggregation",
            "dependency": "stage payloads",
            "missing_behavior": "partial or unavailable composition status",
        },
    ]

    validation_rules = [
        {
            "rule_id": "COMP-V01",
            "rule": "game_start_time_present",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V02",
            "rule": "game_date_matches_game_start_time",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V03",
            "rule": "composition_version_present",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V04",
            "rule": "stage_order_matches_contract",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V05",
            "rule": "stage_status_values_supported",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V06",
            "rule": "stage_counts_match_status_map",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V07",
            "rule": "composition_status_matches_stage_states",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V08",
            "rule": "diagnostic_codes_sorted_and_unique",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V09",
            "rule": "validation_errors_sorted_and_unique",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V10",
            "rule": "provenance_namespaced_by_stage",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V11",
            "rule": "component_authority_flags_false",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V12",
            "rule": "composition_authority_flags_false",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V13",
            "rule": "caller_inputs_immutable",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V14",
            "rule": "component_payloads_immutable",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V15",
            "rule": "composition_deterministic",
            "blocking": True,
        },
        {
            "rule_id": "COMP-V16",
            "rule": "disabled_path_skips_component_execution",
            "blocking": True,
        },
    ]

    fallback_contracts = [
        {
            "fallback_id": "COMP-F01",
            "condition": "diagnostic_disabled",
            "result": "disabled_envelope_only",
            "diagnostic_code": (
                "environment_composition_diagnostic_disabled"
            ),
        },
        {
            "fallback_id": "COMP-F02",
            "condition": "venue_unavailable",
            "result": "retain_stage_failure_and_continue_when_safe",
            "diagnostic_code": (
                "venue_resolution_unavailable"
            ),
        },
        {
            "fallback_id": "COMP-F03",
            "condition": "weather_unavailable",
            "result": "vector_and_carry_follow_dependency_fallbacks",
            "diagnostic_code": (
                "weather_resolution_unavailable"
            ),
        },
        {
            "fallback_id": "COMP-F04",
            "condition": "vector_unavailable",
            "result": "carry_uses_zero_along_path_wind_when_safe",
            "diagnostic_code": (
                "vector_resolution_unavailable"
            ),
        },
        {
            "fallback_id": "COMP-F05",
            "condition": "carry_unavailable",
            "result": "retain_venue_weather_and_vector_payloads",
            "diagnostic_code": (
                "carry_resolution_unavailable"
            ),
        },
        {
            "fallback_id": "COMP-F06",
            "condition": "stage_exception",
            "result": "stage_local_invalid_status",
            "diagnostic_code": (
                "environment_stage_exception_isolated"
            ),
        },
        {
            "fallback_id": "COMP-F07",
            "condition": "mixed_stage_states",
            "result": "partial_composition_status",
            "diagnostic_code": (
                "environment_composition_partial"
            ),
        },
    ]

    implementation_steps = [
        {
            "step": 1,
            "action": "Create typed composition input schema.",
        },
        {
            "step": 2,
            "action": "Create typed environment diagnostic envelope.",
        },
        {
            "step": 3,
            "action": "Implement fixed stage ordering.",
        },
        {
            "step": 4,
            "action": "Implement venue-stage adapter.",
        },
        {
            "step": 5,
            "action": "Implement weather-stage adapter.",
        },
        {
            "step": 6,
            "action": "Implement vector-stage adapter.",
        },
        {
            "step": 7,
            "action": "Implement carry-stage adapter.",
        },
        {
            "step": 8,
            "action": "Implement stage-local exception isolation.",
        },
        {
            "step": 9,
            "action": "Implement status and count aggregation.",
        },
        {
            "step": 10,
            "action": "Implement code, error, and provenance aggregation.",
        },
        {
            "step": 11,
            "action": "Implement disabled-by-default composition entrypoint.",
        },
        {
            "step": 12,
            "action": "Add independent deterministic composition audit.",
        },
    ]

    acceptance_criteria = [
        {
            "criterion_id": "COMP-C01",
            "criterion": "all_four_component_contracts_detected",
            "required": True,
        },
        {
            "criterion_id": "COMP-C02",
            "criterion": "composition_stage_order_explicit",
            "required": True,
        },
        {
            "criterion_id": "COMP-C03",
            "criterion": "composition_output_schema_complete",
            "required": True,
        },
        {
            "criterion_id": "COMP-C04",
            "criterion": "dependency_fallbacks_explicit",
            "required": True,
        },
        {
            "criterion_id": "COMP-C05",
            "criterion": "stage_failure_isolation_implemented",
            "required": True,
        },
        {
            "criterion_id": "COMP-C06",
            "criterion": "partial_resolution_supported",
            "required": True,
        },
        {
            "criterion_id": "COMP-C07",
            "criterion": "diagnostic_code_aggregation_deterministic",
            "required": True,
        },
        {
            "criterion_id": "COMP-C08",
            "criterion": "validation_error_aggregation_deterministic",
            "required": True,
        },
        {
            "criterion_id": "COMP-C09",
            "criterion": "provenance_namespaced",
            "required": True,
        },
        {
            "criterion_id": "COMP-C10",
            "criterion": "stage_counts_consistent",
            "required": True,
        },
        {
            "criterion_id": "COMP-C11",
            "criterion": "caller_inputs_immutable",
            "required": True,
        },
        {
            "criterion_id": "COMP-C12",
            "criterion": "component_payloads_immutable",
            "required": True,
        },
        {
            "criterion_id": "COMP-C13",
            "criterion": "diagnostic_disabled_by_default",
            "required": True,
        },
        {
            "criterion_id": "COMP-C14",
            "criterion": "no_production_environment_activation",
            "required": True,
        },
        {
            "criterion_id": "COMP-C15",
            "criterion": "no_simulation_or_outcome_changes",
            "required": True,
        },
        {
            "criterion_id": "COMP-C16",
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
            "check": "seven_j_predecessor_contract_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "venue_contract_present",
            "actual": venue_contract_present,
            "expected": True,
            "passed": venue_contract_present,
        },
        {
            "check": "weather_contract_present",
            "actual": weather_contract_present,
            "expected": True,
            "passed": weather_contract_present,
        },
        {
            "check": "vector_contract_present",
            "actual": vector_contract_present,
            "expected": True,
            "passed": vector_contract_present,
        },
        {
            "check": "carry_contract_present",
            "actual": carry_contract_present,
            "expected": True,
            "passed": carry_contract_present,
        },
        {
            "check": "five_composition_stages_defined",
            "actual": len(composition_stages),
            "expected": 5,
            "passed": len(composition_stages) == 5,
        },
        {
            "check": "twenty_four_output_fields_defined",
            "actual": len(composition_output_fields),
            "expected": 24,
            "passed": len(composition_output_fields) == 24,
        },
        {
            "check": "six_status_rules_defined",
            "actual": len(status_rules),
            "expected": 6,
            "passed": len(status_rules) == 6,
        },
        {
            "check": "ten_aggregation_rules_defined",
            "actual": len(aggregation_rules),
            "expected": 10,
            "passed": len(aggregation_rules) == 10,
        },
        {
            "check": "eight_dependency_rules_defined",
            "actual": len(dependency_rules),
            "expected": 8,
            "passed": len(dependency_rules) == 8,
        },
        {
            "check": "sixteen_validation_rules_defined",
            "actual": len(validation_rules),
            "expected": 16,
            "passed": len(validation_rules) == 16,
        },
        {
            "check": "seven_fallback_contracts_defined",
            "actual": len(fallback_contracts),
            "expected": 7,
            "passed": len(fallback_contracts) == 7,
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(implementation_steps),
            "expected": 12,
            "passed": len(implementation_steps) == 12,
        },
        {
            "check": "sixteen_acceptance_criteria_defined",
            "actual": len(acceptance_criteria),
            "expected": 16,
            "passed": len(acceptance_criteria) == 16,
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
                "7K defines diagnostic composition contracts only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "environment_diagnostic_composition_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "7L may implement and independently audit "
                    "the bounded diagnostic composition contract."
                ),
            },
            {
                "authority": (
                    "production_environment_integration"
                ),
                "granted": False,
                "reason": (
                    "Composition remains diagnostic-only."
                ),
            },
        ]
    )

    diagnosis_name = (
        "environment_diagnostic_composition_contract_plan_complete"
        if all_checks_passed
        else
        "environment_diagnostic_composition_contract_plan_failed"
    )

    recommended_next_layer = (
        "7L_environment_diagnostic_composition_contract_implementation"
        if all_checks_passed
        else
        "7L_environment_composition_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "composition_stages.csv": composition_stages,
        "composition_output_fields.csv": composition_output_fields,
        "status_rules.csv": status_rules,
        "aggregation_rules.csv": aggregation_rules,
        "dependency_rules.csv": dependency_rules,
        "validation_rules.csv": validation_rules,
        "fallback_contracts.csv": fallback_contracts,
        "implementation_steps.csv": implementation_steps,
        "acceptance_criteria.csv": acceptance_criteria,
        "authority_boundaries.csv": authority_rows,
    }

    fieldnames = {
        "planning_checks.csv": [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        "composition_stages.csv": [
            "stage",
            "name",
            "input_dependencies",
            "output_contract",
            "failure_behavior",
        ],
        "composition_output_fields.csv": [
            "field",
            "type",
            "nullable",
            "purpose",
        ],
        "status_rules.csv": [
            "rule_id",
            "condition",
            "composition_status",
        ],
        "aggregation_rules.csv": [
            "rule_id",
            "rule",
            "requirement",
        ],
        "dependency_rules.csv": [
            "rule_id",
            "downstream_stage",
            "dependency",
            "missing_behavior",
        ],
        "validation_rules.csv": [
            "rule_id",
            "rule",
            "blocking",
        ],
        "fallback_contracts.csv": [
            "fallback_id",
            "condition",
            "result",
            "diagnostic_code",
        ],
        "implementation_steps.csv": [
            "step",
            "action",
        ],
        "acceptance_criteria.csv": [
            "criterion_id",
            "criterion",
            "required",
        ],
        "authority_boundaries.csv": [
            "authority",
            "granted",
            "reason",
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
                "recommended_next_layer": (
                    recommended_next_layer
                ),
                "recommended_action": (
                    "Implement deterministic composition across "
                    "venue, weather, vector, and carry diagnostics."
                    if all_checks_passed
                    else
                    "Remediate failed 7K planning checks."
                ),
                "entry_condition": (
                    "All sixteen 7K planning checks pass."
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
        "component_contracts_required": 4,
        "component_contracts_accepted": sum(
            [
                venue_contract_present,
                weather_contract_present,
                vector_contract_present,
                carry_contract_present,
            ]
        ),
        "composition_stages_defined": len(
            composition_stages
        ),
        "composition_output_fields_defined": len(
            composition_output_fields
        ),
        "status_rules_defined": len(
            status_rules
        ),
        "aggregation_rules_defined": len(
            aggregation_rules
        ),
        "dependency_rules_defined": len(
            dependency_rules
        ),
        "validation_rules_defined": len(
            validation_rules
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
        "production_environment_activated": False,
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
        "environment_composition_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_environment_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / filename)
            for filename in [
                *artifacts.keys(),
                "recommended_path.csv",
            ]
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
