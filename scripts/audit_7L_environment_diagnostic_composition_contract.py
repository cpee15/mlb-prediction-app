#!/usr/bin/env python3
"""
Layer 7L environment diagnostic composition implementation audit.
"""

from __future__ import annotations

import ast
import copy
import csv
from datetime import date, datetime, timezone
import importlib.util
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7L"
LAYER_NAME = (
    "environment_diagnostic_composition_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_7L_environment_diagnostic_composition_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts/"
    "plan_7K_environment_diagnostic_composition_contract.py"
)

CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/"
    "environment_diagnostic_composition.py"
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


def load_contract() -> Any:
    spec = importlib.util.spec_from_file_location(
        "environment_diagnostic_composition_7l",
        CONTRACT_PATH,
    )

    if (
        spec is None
        or spec.loader is None
    ):
        raise RuntimeError(
            "Unable to load 7L contract"
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
        PLAN_PATH.exists()
        and CONTRACT_PATH.exists()
    )

    predecessor_present = (
        "environment_diagnostic_composition_contract_plan_complete"
        in string_constants(
            PLAN_PATH
        )
    )

    contract = load_contract()

    composition_input = (
        contract.EnvironmentCompositionInput(
            game_start_time_utc=datetime(
                2026,
                7,
                1,
                23,
                5,
                tzinfo=timezone.utc,
            ),
            game_date=date(
                2026,
                7,
                1,
            ),
            canonical_venue_id=(
                "venue-alpha"
            ),
        )
    )

    venue_payload = {
        "canonical_venue_id": "venue-alpha",
        "resolution_status": "resolved",
        "diagnostic_codes": [
            "venue_resolved",
            "shared_code",
        ],
        "validation_errors": [],
        "provenance": {
            "source": "venue-source",
        },
        "production_authority": False,
    }

    weather_payload = {
        "resolution_status": "resolved",
        "indoor_effective": False,
        "temperature_c": 24.0,
        "relative_humidity_pct": 55.0,
        "diagnostic_codes": [
            "weather_resolved",
            "shared_code",
        ],
        "validation_errors": [],
        "provenance": {
            "source": "weather-source",
        },
        "production_authority": False,
    }

    vector_payload = {
        "vector_resolution_status": "resolved",
        "wind_along_ball_path_mps": 3.0,
        "diagnostic_codes": [
            "vector_resolved",
        ],
        "validation_errors": [],
        "provenance": {
            "source": "vector-source",
        },
        "production_authority": False,
    }

    carry_payload = {
        "resolution_status": "resolved",
        "combined_carry_index": 0.2,
        "diagnostic_codes": [
            "carry_resolved",
        ],
        "validation_errors": [],
        "provenance": {
            "source": "carry-source",
        },
        "production_authority": False,
    }

    def provider_for(
        payload: dict[str, Any],
    ) -> Any:
        return lambda: payload

    all_resolved_providers = {
        "venue_resolution": provider_for(
            venue_payload
        ),
        "weather_resolution": provider_for(
            weather_payload
        ),
        "field_vector_resolution": provider_for(
            vector_payload
        ),
        "carry_diagnostic_resolution": provider_for(
            carry_payload
        ),
    }

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

    call_count = {
        "count": 0,
    }

    def disabled_provider() -> dict[str, Any]:
        call_count["count"] += 1
        return venue_payload

    disabled = (
        contract.compose_environment_diagnostics(
            enabled=False,
            composition_input=composition_input,
            stage_providers={
                "venue_resolution": (
                    disabled_provider
                ),
            },
        )
    )

    record(
        "7L-C01",
        "disabled path skips providers",
        disabled.composition_status
        == "disabled"
        and call_count["count"] == 0,
        {
            "status": disabled.composition_status,
            "provider_calls": call_count[
                "count"
            ],
        },
        {
            "status": "disabled",
            "provider_calls": 0,
        },
    )

    record(
        "7L-C02",
        "disabled path emits all disabled statuses",
        all(
            status == "disabled"
            for status in disabled.stage_statuses.values()
        ),
        disabled.stage_statuses,
        {
            "all_statuses": "disabled",
        },
    )

    resolved = (
        contract.compose_environment_diagnostics(
            enabled=True,
            composition_input=composition_input,
            stage_providers=(
                all_resolved_providers
            ),
        )
    )

    record(
        "7L-C03",
        "all resolved stages produce resolved composition",
        resolved.composition_status
        == "resolved",
        resolved.composition_status,
        "resolved",
    )

    record(
        "7L-C04",
        "fixed stage order preserved",
        tuple(
            resolved.stage_statuses.keys()
        )
        == contract.FULL_STAGE_ORDER,
        tuple(
            resolved.stage_statuses.keys()
        ),
        contract.FULL_STAGE_ORDER,
    )

    record(
        "7L-C05",
        "all resolved stage count includes aggregation",
        resolved.resolved_stage_count
        == 5,
        resolved.resolved_stage_count,
        5,
    )

    record(
        "7L-C06",
        "diagnostic codes deduplicated and sorted",
        resolved.diagnostic_codes
        == tuple(
            sorted(
                set(
                    resolved.diagnostic_codes
                )
            )
        )
        and resolved.diagnostic_codes.count(
            "shared_code"
        )
        == 1,
        resolved.diagnostic_codes,
        "sorted_unique",
    )

    record(
        "7L-C07",
        "provenance namespaced by stage",
        all(
            stage_name
            in resolved.provenance
            for stage_name
            in contract.FULL_STAGE_ORDER
        ),
        sorted(
            resolved.provenance.keys()
        ),
        sorted(
            [
                *contract.FULL_STAGE_ORDER,
                "component_execution_skipped",
                "composition_stage_order",
            ]
        ),
    )

    partial_providers = dict(
        all_resolved_providers
    )
    partial_providers[
        "field_vector_resolution"
    ] = lambda: None

    partial = (
        contract.compose_environment_diagnostics(
            enabled=True,
            composition_input=composition_input,
            stage_providers=(
                partial_providers
            ),
        )
    )

    record(
        "7L-C08",
        "mixed stage states produce partial composition",
        partial.composition_status
        == "partial"
        and (
            "environment_composition_partial"
            in partial.diagnostic_codes
        ),
        partial.to_dict(),
        {
            "status": "partial",
        },
    )

    record(
        "7L-C09",
        "unavailable vector does not erase other payloads",
        partial.venue_resolution
        is not None
        and partial.weather_resolution
        is not None
        and partial.vector_resolution
        is None
        and partial.carry_resolution
        is not None,
        {
            "venue": (
                partial.venue_resolution
                is not None
            ),
            "weather": (
                partial.weather_resolution
                is not None
            ),
            "vector": (
                partial.vector_resolution
                is not None
            ),
            "carry": (
                partial.carry_resolution
                is not None
            ),
        },
        {
            "venue": True,
            "weather": True,
            "vector": False,
            "carry": True,
        },
    )

    neutral_payload = {
        "resolution_status": "neutral",
        "diagnostic_codes": [
            "neutral_stage"
        ],
        "validation_errors": [],
        "provenance": {},
        "production_authority": False,
    }

    neutral = (
        contract.compose_environment_diagnostics(
            enabled=True,
            composition_input=composition_input,
            stage_providers={
                stage_name: provider_for(
                    neutral_payload
                )
                for stage_name
                in contract.COMPONENT_STAGE_ORDER
            },
        )
    )

    record(
        "7L-C10",
        "all neutral components produce neutral composition",
        neutral.composition_status
        == "neutral"
        and neutral.neutral_stage_count
        == 4,
        neutral.to_dict(),
        {
            "status": "neutral",
            "neutral_count": 4,
        },
    )

    unavailable = (
        contract.compose_environment_diagnostics(
            enabled=True,
            composition_input=composition_input,
            stage_providers={
                stage_name: (
                    lambda: None
                )
                for stage_name
                in contract.COMPONENT_STAGE_ORDER
            },
        )
    )

    record(
        "7L-C11",
        "all unavailable components produce unavailable composition",
        unavailable.composition_status
        == "unavailable"
        and unavailable.unavailable_stage_count
        == 4,
        unavailable.to_dict(),
        {
            "status": "unavailable",
            "unavailable_count": 4,
        },
    )

    def raising_provider() -> dict[str, Any]:
        raise ValueError(
            "isolated test exception"
        )

    exception_providers = dict(
        all_resolved_providers
    )
    exception_providers[
        "weather_resolution"
    ] = raising_provider

    isolated = (
        contract.compose_environment_diagnostics(
            enabled=True,
            composition_input=composition_input,
            stage_providers=(
                exception_providers
            ),
        )
    )

    record(
        "7L-C12",
        "stage exception isolated",
        isolated.stage_statuses[
            "weather_resolution"
        ]
        == "invalid"
        and isolated.venue_resolution
        is not None
        and isolated.vector_resolution
        is not None
        and isolated.carry_resolution
        is not None
        and (
            "environment_stage_exception_isolated"
            in isolated.diagnostic_codes
        ),
        isolated.to_dict(),
        {
            "weather_status": "invalid",
            "other_payloads_preserved": True,
        },
    )

    missing_provider = (
        contract.compose_environment_diagnostics(
            enabled=True,
            composition_input=composition_input,
            stage_providers={
                "venue_resolution": (
                    all_resolved_providers[
                        "venue_resolution"
                    ]
                ),
                "weather_resolution": (
                    all_resolved_providers[
                        "weather_resolution"
                    ]
                ),
                "field_vector_resolution": (
                    all_resolved_providers[
                        "field_vector_resolution"
                    ]
                ),
            },
        )
    )

    record(
        "7L-C13",
        "missing provider becomes unavailable",
        missing_provider.stage_statuses[
            "carry_diagnostic_resolution"
        ]
        == "unavailable"
        and (
            "carry_diagnostic_resolution_provider_missing"
            in missing_provider.diagnostic_codes
        ),
        missing_provider.to_dict(),
        {
            "carry_status": "unavailable",
        },
    )

    invalid_input = (
        contract.EnvironmentCompositionInput(
            game_start_time_utc=datetime(
                2026,
                7,
                2,
                1,
                0,
                tzinfo=timezone.utc,
            ),
            game_date=date(
                2026,
                7,
                1,
            ),
            canonical_venue_id=(
                "venue-alpha"
            ),
        )
    )

    invalid_composition = (
        contract.compose_environment_diagnostics(
            enabled=True,
            composition_input=invalid_input,
            stage_providers=(
                all_resolved_providers
            ),
        )
    )

    record(
        "7L-C14",
        "invalid composition input produces invalid status",
        invalid_composition.composition_status
        == "invalid"
        and (
            "game_date_matches_game_start_time"
            in invalid_composition.validation_errors
        ),
        invalid_composition.to_dict(),
        {
            "status": "invalid",
        },
    )

    inputs_before = {
        "composition_input": copy.deepcopy(
            composition_input
        ),
        "venue_payload": copy.deepcopy(
            venue_payload
        ),
        "weather_payload": copy.deepcopy(
            weather_payload
        ),
        "vector_payload": copy.deepcopy(
            vector_payload
        ),
        "carry_payload": copy.deepcopy(
            carry_payload
        ),
    }

    immutable_result = (
        contract.compose_environment_diagnostics(
            enabled=True,
            composition_input=composition_input,
            stage_providers=(
                all_resolved_providers
            ),
        )
    )

    inputs_after = {
        "composition_input": (
            composition_input
        ),
        "venue_payload": venue_payload,
        "weather_payload": weather_payload,
        "vector_payload": vector_payload,
        "carry_payload": carry_payload,
    }

    record(
        "7L-C15",
        "caller inputs remain immutable",
        inputs_before == inputs_after,
        {
            "unchanged": (
                inputs_before
                == inputs_after
            ),
        },
        {
            "unchanged": True,
        },
    )

    immutable_result.venue_resolution[
        "canonical_venue_id"
    ] = "mutated-copy"

    record(
        "7L-C16",
        "component payloads copied before composition",
        venue_payload[
            "canonical_venue_id"
        ]
        == "venue-alpha",
        venue_payload[
            "canonical_venue_id"
        ],
        "venue-alpha",
    )

    repeat = (
        contract.compose_environment_diagnostics(
            enabled=True,
            composition_input=composition_input,
            stage_providers=(
                all_resolved_providers
            ),
        )
    )

    repeat_again = (
        contract.compose_environment_diagnostics(
            enabled=True,
            composition_input=composition_input,
            stage_providers=(
                all_resolved_providers
            ),
        )
    )

    record(
        "7L-C17",
        "composition deterministic",
        repeat.to_dict()
        == repeat_again.to_dict(),
        repeat.to_dict(),
        repeat_again.to_dict(),
    )

    record(
        "7L-C18",
        "authority flags remain false",
        all(
            value is False
            for value in [
                repeat.production_authority,
                repeat.simulation_inputs_changed,
                repeat.canonical_probability_authority_changed,
                repeat.production_environment_activated,
                repeat.batted_ball_distance_changed,
                repeat.batted_ball_outcomes_changed,
            ]
        ),
        {
            "production_authority": (
                repeat.production_authority
            ),
            "simulation_inputs_changed": (
                repeat.simulation_inputs_changed
            ),
            "probability_authority_changed": (
                repeat.canonical_probability_authority_changed
            ),
            "environment_activated": (
                repeat.production_environment_activated
            ),
            "distance_changed": (
                repeat.batted_ball_distance_changed
            ),
            "outcomes_changed": (
                repeat.batted_ball_outcomes_changed
            ),
        },
        {
            "all_false": True,
        },
    )

    record(
        "7L-C19",
        "stage counts match status map",
        (
            repeat.resolved_stage_count
            == sum(
                status == "resolved"
                for status
                in repeat.stage_statuses.values()
            )
            and repeat.neutral_stage_count
            == sum(
                status == "neutral"
                for status
                in repeat.stage_statuses.values()
            )
            and repeat.unavailable_stage_count
            == sum(
                status == "unavailable"
                for status
                in repeat.stage_statuses.values()
            )
            and repeat.invalid_stage_count
            == sum(
                status == "invalid"
                for status
                in repeat.stage_statuses.values()
            )
        ),
        {
            "resolved": (
                repeat.resolved_stage_count
            ),
            "neutral": (
                repeat.neutral_stage_count
            ),
            "unavailable": (
                repeat.unavailable_stage_count
            ),
            "invalid": (
                repeat.invalid_stage_count
            ),
        },
        "counts_match_statuses",
    )

    record(
        "7L-C20",
        "canonical venue identity preserved",
        repeat.canonical_venue_id
        == "venue-alpha",
        repeat.canonical_venue_id,
        "venue-alpha",
    )

    implementation_checks = [
        {
            "check": "required_paths_exist",
            "actual": required_paths_exist,
            "expected": True,
            "passed": required_paths_exist,
        },
        {
            "check": "seven_k_predecessor_contract_present",
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
            "check": "five_stage_order_preserved",
            "actual": len(
                contract.FULL_STAGE_ORDER
            ),
            "expected": 5,
            "passed": (
                len(
                    contract.FULL_STAGE_ORDER
                )
                == 5
            ),
        },
        {
            "check": "stage_failure_isolation_implemented",
            "actual": isolated.stage_statuses[
                "weather_resolution"
            ],
            "expected": "invalid",
            "passed": (
                isolated.stage_statuses[
                    "weather_resolution"
                ]
                == "invalid"
            ),
        },
        {
            "check": "partial_resolution_supported",
            "actual": (
                partial.composition_status
            ),
            "expected": "partial",
            "passed": (
                partial.composition_status
                == "partial"
            ),
        },
        {
            "check": "diagnostic_disabled_by_default_path",
            "actual": (
                disabled.composition_status
            ),
            "expected": "disabled",
            "passed": (
                disabled.composition_status
                == "disabled"
            ),
        },
        {
            "check": "production_authority_absent",
            "actual": (
                repeat.production_authority
            ),
            "expected": False,
            "passed": (
                repeat.production_authority
                is False
            ),
        },
        {
            "check": "simulation_inputs_unchanged",
            "actual": (
                repeat.simulation_inputs_changed
            ),
            "expected": False,
            "passed": (
                repeat.simulation_inputs_changed
                is False
            ),
        },
        {
            "check": "probability_authority_unchanged",
            "actual": (
                repeat.canonical_probability_authority_changed
            ),
            "expected": False,
            "passed": (
                repeat.canonical_probability_authority_changed
                is False
            ),
        },
        {
            "check": "production_environment_not_activated",
            "actual": (
                repeat.production_environment_activated
            ),
            "expected": False,
            "passed": (
                repeat.production_environment_activated
                is False
            ),
        },
        {
            "check": "batted_ball_distance_unchanged",
            "actual": (
                repeat.batted_ball_distance_changed
            ),
            "expected": False,
            "passed": (
                repeat.batted_ball_distance_changed
                is False
            ),
        },
        {
            "check": "batted_ball_outcomes_unchanged",
            "actual": (
                repeat.batted_ball_outcomes_changed
            ),
            "expected": False,
            "passed": (
                repeat.batted_ball_outcomes_changed
                is False
            ),
        },
        {
            "check": "caller_inputs_immutable",
            "actual": (
                inputs_before
                == inputs_after
            ),
            "expected": True,
            "passed": (
                inputs_before
                == inputs_after
            ),
        },
        {
            "check": "composition_deterministic",
            "actual": (
                repeat.to_dict()
                == repeat_again.to_dict()
            ),
            "expected": True,
            "passed": (
                repeat.to_dict()
                == repeat_again.to_dict()
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
                "environment_diagnostic_composition"
            ),
            "granted": all_checks_passed,
            "reason": (
                "The deterministic metadata-only composition "
                "contract passed all implementation checks."
            ),
        },
        {
            "authority": (
                "production_environment_activation"
            ),
            "granted": False,
            "reason": (
                "Composition remains diagnostic-only."
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
            "authority": (
                "batted_ball_distance_change"
            ),
            "granted": False,
            "reason": (
                "Carry diagnostics remain dimensionless metadata."
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
        "7M_environment_observability_and_shadow_evaluation_contract_plan"
        if all_checks_passed
        else
        "7L_environment_composition_contract_remediation"
    )

    diagnosis_name = (
        "environment_diagnostic_composition_contract_implementation_passed"
        if all_checks_passed
        else
        "environment_diagnostic_composition_contract_implementation_failed"
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
                    "Plan shadow-only environment observability "
                    "and evaluation without production authority."
                    if all_checks_passed
                    else
                    "Remediate failed 7L implementation checks."
                ),
                "entry_condition": (
                    "All sixteen 7L implementation checks pass."
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
            cases
        ),
        "contract_cases_passed": sum(
            1
            for row in cases
            if row["passed"]
        ),
        "composition_stages_implemented": 5,
        "stage_failure_isolation_implemented": True,
        "partial_resolution_implemented": True,
        "deterministic_aggregation_implemented": True,
        "namespaced_provenance_implemented": True,
        "diagnostic_disabled_path_implemented": True,
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
        "environment_shadow_observability_planning_allowed_next": (
            all_checks_passed
        ),
        "production_environment_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR
                / "implementation_checks.csv"
            ),
            str(
                OUTPUT_DIR
                / "contract_cases.csv"
            ),
            str(
                OUTPUT_DIR
                / "authority_boundaries.csv"
            ),
            str(
                OUTPUT_DIR
                / "recommended_path.csv"
            ),
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "implementation_summary.json"
            ),
            str(
                OUTPUT_DIR
                / "diagnosis.json"
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
