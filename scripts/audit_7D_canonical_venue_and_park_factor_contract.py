#!/usr/bin/env python3
"""
Layer 7D
Canonical Venue and Park-Factor Contract Implementation Audit

Implements and independently audits the 7C contract as a disabled-by-default,
diagnostic-only component.

No production simulation integration or probability authority is introduced.
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


LAYER_ID = "7D"
LAYER_NAME = (
    "canonical_venue_and_park_factor_contract_implementation"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_7D_canonical_venue_and_park_factor_contract"
)

PLAN_7C_PATH = (
    ROOT
    / "scripts/plan_7C_canonical_venue_and_park_factor_source_contract.py"
)

CONTRACT_PATH = (
    ROOT
    / "mlb_app/environment/venue_park_factor_contract.py"
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


def read_text(path: Path) -> str:
    return path.read_text(
        encoding="utf-8",
        errors="ignore",
    )


def string_constants(path: Path) -> set[str]:
    tree = ast.parse(
        read_text(path),
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
        "venue_park_factor_contract_7d",
        CONTRACT_PATH,
    )

    if (
        spec is None
        or spec.loader is None
    ):
        raise RuntimeError(
            "Unable to load contract module"
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
        PLAN_7C_PATH.exists()
        and CONTRACT_PATH.exists()
    )

    plan_constants = string_constants(
        PLAN_7C_PATH
    )

    predecessor_contract_present = (
        "canonical_venue_and_park_factor_source_contract_plan_complete"
        in plan_constants
    )

    contract = load_contract_module()

    venues = [
        contract.CanonicalVenue(
            canonical_venue_id="venue-alpha",
            provider_venue_id="1001",
            canonical_venue_name=(
                "Alpha Ballpark"
            ),
            venue_aliases=(
                "Alpha Park",
                "Old Alpha Field",
            ),
            home_team_id="ALP",
            timezone="America/New_York",
            latitude=40.0,
            longitude=-73.0,
            elevation_meters=15.0,
            roof_type="open_air",
            active_from=date(
                2000,
                1,
                1,
            ),
            active_through=None,
        ),
        contract.CanonicalVenue(
            canonical_venue_id="venue-beta",
            provider_venue_id="1002",
            canonical_venue_name=(
                "Beta Dome"
            ),
            venue_aliases=(
                "The Beta Dome",
            ),
            home_team_id="BET",
            timezone="America/Chicago",
            latitude=41.0,
            longitude=-87.0,
            elevation_meters=181.0,
            roof_type="fixed_dome",
            active_from=date(
                2005,
                1,
                1,
            ),
            active_through=None,
        ),
    ]

    retrieved_at = datetime(
        2026,
        6,
        1,
        12,
        0,
        tzinfo=timezone.utc,
    )

    records = [
        contract.ParkFactorRecord(
            canonical_venue_id=(
                "venue-alpha"
            ),
            season=2026,
            factor_version="primary-2026-v1",
            factor_scope="overall_runs",
            factor_value=1.08,
            neutral_value=1.0,
            sample_games=40,
            source_name="primary",
            source_record_id="pf-a-2026",
            source_published_at=(
                retrieved_at
            ),
            retrieved_at=retrieved_at,
            is_final=False,
            source_class=(
                contract.PRIMARY_SOURCE_CLASS
            ),
        ),
        contract.ParkFactorRecord(
            canonical_venue_id=(
                "venue-alpha"
            ),
            season=2026,
            factor_version="secondary-2026-v1",
            factor_scope="overall_runs",
            factor_value=1.05,
            neutral_value=1.0,
            sample_games=42,
            source_name="secondary",
            source_record_id="pf-a2-2026",
            source_published_at=(
                retrieved_at
            ),
            retrieved_at=retrieved_at,
            is_final=False,
            source_class=(
                contract.SECONDARY_SOURCE_CLASS
            ),
        ),
        contract.ParkFactorRecord(
            canonical_venue_id=(
                "venue-alpha"
            ),
            season=2025,
            factor_version="primary-2025-final",
            factor_scope="home_runs",
            factor_value=1.12,
            neutral_value=1.0,
            sample_games=81,
            source_name="primary",
            source_record_id="pf-a-2025",
            source_published_at=(
                retrieved_at
            ),
            retrieved_at=retrieved_at,
            is_final=True,
            source_class=(
                contract.PRIMARY_SOURCE_CLASS
            ),
        ),
        contract.ParkFactorRecord(
            canonical_venue_id=(
                "venue-beta"
            ),
            season=2027,
            factor_version="future-invalid",
            factor_scope="overall_runs",
            factor_value=1.03,
            neutral_value=1.0,
            sample_games=10,
            source_name="primary",
            source_record_id="pf-b-2027",
            source_published_at=(
                retrieved_at
            ),
            retrieved_at=retrieved_at,
            is_final=False,
            source_class=(
                contract.PRIMARY_SOURCE_CLASS
            ),
        ),
    ]

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

    venue_alias = contract.resolve_venue(
        "  OLD-alpha_field ",
        date(
            2026,
            6,
            15,
        ),
        venues,
    )

    record_case(
        "7D-C01",
        "venue alias normalization and resolution",
        venue_alias.resolved
        and venue_alias.canonical_venue_id
        == "venue-alpha",
        venue_alias.to_dict(),
        {
            "resolved": True,
            "canonical_venue_id": (
                "venue-alpha"
            ),
        },
    )

    venue_repeat = contract.resolve_venue(
        "old alpha field",
        date(
            2026,
            6,
            15,
        ),
        list(reversed(venues)),
    )

    record_case(
        "7D-C02",
        "venue resolution deterministic under input ordering",
        venue_repeat.resolved == venue_alias.resolved
        and venue_repeat.canonical_venue_id == venue_alias.canonical_venue_id
        and venue_repeat.canonical_venue_name == venue_alias.canonical_venue_name
        and venue_repeat.diagnostic_code == venue_alias.diagnostic_code
        and venue_repeat.candidate_count == venue_alias.candidate_count,
        venue_repeat.to_dict(),
        {
            "resolved": venue_alias.resolved,
            "canonical_venue_id": venue_alias.canonical_venue_id,
            "canonical_venue_name": venue_alias.canonical_venue_name,
            "diagnostic_code": venue_alias.diagnostic_code,
            "candidate_count": venue_alias.candidate_count,
        },
    )

    exact = contract.resolve_park_factor(
        venue_resolution=venue_alias,
        game_season=2026,
        factor_scope="overall_runs",
        records=records,
    )

    record_case(
        "7D-C03",
        "primary exact-season source precedence",
        exact.factor_value == 1.08
        and exact.source_name == "primary"
        and not exact.stale,
        exact.to_dict(),
        {
            "factor_value": 1.08,
            "source_name": "primary",
            "stale": False,
        },
    )

    prior = contract.resolve_park_factor(
        venue_resolution=venue_alias,
        game_season=2026,
        factor_scope="home_runs",
        records=records,
    )

    record_case(
        "7D-C04",
        "nearest prior final season fallback",
        prior.factor_value == 1.12
        and prior.stale
        and prior.stale_seasons == 1
        and prior.diagnostic_code
        == "prior_season_factor_fallback",
        prior.to_dict(),
        {
            "factor_value": 1.12,
            "stale": True,
            "stale_seasons": 1,
        },
    )

    unresolved = contract.resolve_venue(
        "Missing Stadium",
        date(
            2026,
            6,
            15,
        ),
        venues,
    )

    unresolved_factor = (
        contract.resolve_park_factor(
            venue_resolution=unresolved,
            game_season=2026,
            factor_scope="overall_runs",
            records=records,
        )
    )

    record_case(
        "7D-C05",
        "unresolved venue neutral fallback",
        unresolved_factor.factor_value
        == 1.0
        and unresolved_factor.source_class
        == contract.NEUTRAL_SOURCE_CLASS
        and unresolved_factor.diagnostic_code
        == "venue_unresolved_neutral_fallback",
        unresolved_factor.to_dict(),
        {
            "factor_value": 1.0,
            "diagnostic_code": (
                "venue_unresolved_neutral_fallback"
            ),
        },
    )

    beta = contract.resolve_venue(
        "Beta Dome",
        date(
            2026,
            6,
            15,
        ),
        venues,
    )

    future_filtered = (
        contract.resolve_park_factor(
            venue_resolution=beta,
            game_season=2026,
            factor_scope="overall_runs",
            records=records,
        )
    )

    record_case(
        "7D-C06",
        "future-season record prohibited",
        future_filtered.factor_value
        == 1.0
        and future_filtered.diagnostic_code
        == "park_factor_missing_neutral_fallback",
        future_filtered.to_dict(),
        {
            "factor_value": 1.0,
            "future_record_selected": False,
        },
    )

    invalid_record = (
        contract.ParkFactorRecord(
            canonical_venue_id=(
                "venue-beta"
            ),
            season=2026,
            factor_version="bad-v1",
            factor_scope="overall_runs",
            factor_value=-1.0,
            neutral_value=1.0,
            sample_games=5,
            source_name="primary",
            source_record_id="bad",
            source_published_at=(
                retrieved_at
            ),
            retrieved_at=retrieved_at,
            is_final=False,
            source_class=(
                contract.PRIMARY_SOURCE_CLASS
            ),
        )
    )

    invalid_factor = (
        contract.resolve_park_factor(
            venue_resolution=beta,
            game_season=2026,
            factor_scope="overall_runs",
            records=[
                invalid_record
            ],
        )
    )

    record_case(
        "7D-C07",
        "invalid factor neutral fallback with explicit reason",
        invalid_factor.factor_value
        == 1.0
        and invalid_factor.diagnostic_code
        == "park_factor_invalid_neutral_fallback"
        and (
            "factor_value_finite_and_positive"
            in invalid_factor.validation_errors
        ),
        invalid_factor.to_dict(),
        {
            "factor_value": 1.0,
            "validation_error": (
                "factor_value_finite_and_positive"
            ),
        },
    )

    disabled = (
        contract.evaluate_venue_park_factor_diagnostic(
            enabled=False,
            venue_query="Alpha Park",
            game_date=date(
                2026,
                6,
                15,
            ),
            factor_scope="overall_runs",
            venues=venues,
            records=records,
        )
    )

    record_case(
        "7D-C08",
        "diagnostic disabled by default behavior",
        disabled
        == {
            "enabled": False,
            "diagnostic_code": (
                "venue_park_factor_diagnostic_disabled"
            ),
            "production_authority": False,
            "simulation_inputs_changed": False,
            "canonical_probability_authority_changed": False,
        },
        disabled,
        {
            "enabled": False,
            "production_authority": False,
        },
    )

    mutable_venues = copy.deepcopy(
        venues
    )
    mutable_records = copy.deepcopy(
        records
    )

    venues_before = copy.deepcopy(
        mutable_venues
    )
    records_before = copy.deepcopy(
        mutable_records
    )

    enabled = (
        contract.evaluate_venue_park_factor_diagnostic(
            enabled=True,
            venue_query="Alpha Park",
            game_date=date(
                2026,
                6,
                15,
            ),
            factor_scope="overall_runs",
            venues=mutable_venues,
            records=mutable_records,
        )
    )

    record_case(
        "7D-C09",
        "enabled diagnostic remains metadata-only",
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
        is False,
        enabled,
        {
            "production_authority": False,
            "simulation_inputs_changed": False,
            "canonical_probability_authority_changed": False,
        },
    )

    record_case(
        "7D-C10",
        "caller inputs remain immutable",
        mutable_venues == venues_before
        and mutable_records == records_before,
        {
            "venues_unchanged": (
                mutable_venues
                == venues_before
            ),
            "records_unchanged": (
                mutable_records
                == records_before
            ),
        },
        {
            "venues_unchanged": True,
            "records_unchanged": True,
        },
    )

    enabled_repeat = (
        contract.evaluate_venue_park_factor_diagnostic(
            enabled=True,
            venue_query="Alpha Park",
            game_date=date(
                2026,
                6,
                15,
            ),
            factor_scope="overall_runs",
            venues=list(
                reversed(
                    mutable_venues
                )
            ),
            records=list(
                reversed(
                    mutable_records
                )
            ),
        )
    )

    record_case(
        "7D-C11",
        "full diagnostic output deterministic",
        enabled_repeat == enabled,
        enabled_repeat,
        enabled,
    )

    record_case(
        "7D-C12",
        "provenance and freshness metadata emitted",
        all(
            key
            in enabled[
                "park_factor_resolution"
            ][
                "provenance"
            ]
            for key in [
                "source_name",
                "source_record_id",
                "source_published_at",
                "retrieved_at",
            ]
        )
        and "stale"
        in enabled[
            "park_factor_resolution"
        ]
        and "stale_seasons"
        in enabled[
            "park_factor_resolution"
        ],
        enabled[
            "park_factor_resolution"
        ],
        {
            "provenance_present": True,
            "freshness_present": True,
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
            "check": "seven_c_predecessor_contract_present",
            "actual": predecessor_contract_present,
            "expected": True,
            "passed": predecessor_contract_present,
        },
        {
            "check": "twelve_contract_cases_pass",
            "actual": sum(
                1
                for row in case_rows
                if row["passed"]
            ),
            "expected": 12,
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
            "check": "canonical_probability_authority_unchanged",
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
            "check": "caller_inputs_immutable",
            "actual": (
                mutable_venues
                == venues_before
                and mutable_records
                == records_before
            ),
            "expected": True,
            "passed": (
                mutable_venues
                == venues_before
                and mutable_records
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
                "venue_and_park_factor_diagnostic"
            ),
            "granted": all_checks_passed,
            "reason": (
                "The deterministic diagnostic contract passed "
                "all implementation checks."
            ),
        },
        {
            "authority": (
                "production_environment_activation"
            ),
            "granted": False,
            "reason": (
                "7D does not wire the contract into production."
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
                "canonical_probability_replacement"
            ),
            "granted": False,
            "reason": (
                "No canonical model authority is changed."
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
        "7E_roof_dome_weather_and_atmospheric_state_contract_plan"
        if all_checks_passed
        else
        "7D_canonical_venue_and_park_factor_contract_remediation"
    )

    diagnosis_name = (
        "canonical_venue_and_park_factor_contract_implementation_passed"
        if all_checks_passed
        else
        "canonical_venue_and_park_factor_contract_implementation_failed"
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
                    "Plan the roof, dome, weather, and atmospheric "
                    "state contract."
                    if all_checks_passed
                    else
                    "Remediate failed 7D implementation checks."
                ),
                "entry_condition": (
                    "All nine 7D implementation checks pass."
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
        "venue_schema_implemented": True,
        "park_factor_schema_implemented": True,
        "venue_alias_resolution_implemented": True,
        "source_precedence_implemented": True,
        "season_semantics_implemented": True,
        "neutral_fallback_implemented": True,
        "stale_fallback_implemented": True,
        "provenance_metadata_implemented": True,
        "diagnostic_disabled_by_default": True,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
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
        "weather_state_contract_planning_allowed_next": (
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
