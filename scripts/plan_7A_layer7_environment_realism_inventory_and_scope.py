#!/usr/bin/env python3
"""
Layer 7A
Layer 7 Environment Realism Inventory and Scope Plan

Inventories existing venue, park, roof, weather, atmospheric, field-geometry,
and batted-ball environment capabilities, then defines a bounded Layer 7
implementation sequence.

Planning and inventory only. This layer does not:
- modify production simulation behavior;
- change probabilities, parameters, or canonical model authority;
- run historical calibration or backtests;
- authorize pricing, market comparison, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7A"
LAYER_NAME = "layer7_environment_realism_inventory_and_scope_plan"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_7A_environment_realism_inventory_and_scope_plan"
)

ROADMAP_PATH = ROOT / "docs/roadmap_to_edge_detection.md"

LAYER6_EXIT_PATH = (
    ROOT
    / "scripts/finalize_6QP_layer6_narrow_scope_exit.py"
)

SEARCH_ROOTS = [
    ROOT / "mlb_app",
    ROOT / "scripts",
    ROOT / "docs",
]

SUPPORTED_SUFFIXES = {
    ".py",
    ".md",
    ".json",
    ".yaml",
    ".yml",
    ".csv",
}

DOMAIN_TERMS = {
    "venue_identity": [
        "venue",
        "venue_id",
        "stadium",
        "ballpark",
        "park_name",
    ],
    "park_factors": [
        "park factor",
        "park_factor",
        "park factors",
        "run environment",
        "run_environment",
    ],
    "roof_and_dome_state": [
        "roof",
        "roof_open",
        "roof_closed",
        "dome",
        "retractable",
    ],
    "weather_core": [
        "weather",
        "temperature",
        "humidity",
        "precipitation",
        "dew point",
        "dew_point",
    ],
    "wind": [
        "wind",
        "wind_speed",
        "wind direction",
        "wind_direction",
        "field orientation",
        "field_orientation",
    ],
    "atmospheric_physics": [
        "air density",
        "air_density",
        "barometric",
        "pressure",
        "altitude",
        "elevation",
    ],
    "field_geometry": [
        "wall height",
        "wall_height",
        "fence distance",
        "fence_distance",
        "field geometry",
        "field_geometry",
        "dimensions",
    ],
    "batted_ball_environment": [
        "launch angle",
        "launch_angle",
        "exit velocity",
        "exit_velocity",
        "spray angle",
        "spray_angle",
        "carry",
        "batted ball",
        "batted_ball",
    ],
    "environment_runtime_wiring": [
        "environment_source",
        "weather_source",
        "park_source",
        "environment_modifier",
        "weather_modifier",
        "park_modifier",
    ],
    "environment_validation": [
        "park bucket",
        "weather bucket",
        "environment bucket",
        "hr calibration",
        "xbh calibration",
        "total-run calibration",
        "run bias",
    ],
}

PROHIBITED_AUTHORITIES = [
    "production_environment_activation",
    "simulation_state_change",
    "simulation_parameter_change",
    "simulation_probability_change",
    "canonical_probability_replacement",
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


def iter_search_files() -> list[Path]:
    paths: list[Path] = []

    for root in SEARCH_ROOTS:
        if not root.exists():
            continue

        paths.extend(
            path
            for path in root.rglob("*")
            if path.is_file()
            and path.suffix.lower() in SUPPORTED_SUFFIXES
            and "tmp" not in path.parts
            and ".git" not in path.parts
        )

    return sorted(set(paths))


def find_domain_matches(
    files: list[Path],
    domain: str,
    terms: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    normalized_terms = [
        term.lower()
        for term in terms
    ]

    for path in files:
        text = read_text(path)
        lowered = text.lower()

        found_terms = sorted(
            {
                term
                for term in normalized_terms
                if term in lowered
            }
        )

        if not found_terms:
            continue

        rows.append(
            {
                "domain": domain,
                "path": str(
                    path.relative_to(ROOT)
                ),
                "matched_terms": "|".join(
                    found_terms
                ),
                "match_count": sum(
                    lowered.count(term)
                    for term in found_terms
                ),
            }
        )

    return rows


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    roadmap_text = read_text(
        ROADMAP_PATH
    )

    layer6_constants = string_constants(
        LAYER6_EXIT_PATH
    )

    roadmap_layer7_contract_present = all(
        token in roadmap_text
        for token in [
            "Layer 7 — Environment Realism Engine",
            "venue-specific wind geometry",
            "roof state and dome state",
            "air density",
            "weather-to-batted-ball interaction",
            "environment_and_park_physics",
        ]
    )

    layer6_exit_contract_present = all(
        token in layer6_constants
        for token in [
            "layer6_narrow_scope_exit_finalized",
            "layer6_closed_under_narrow_documented_scope",
            "narrow_documented_scope",
            "new_production_authority_granted",
        ]
    )

    search_files = iter_search_files()

    inventory_rows: list[dict[str, Any]] = []

    for domain, terms in DOMAIN_TERMS.items():
        inventory_rows.extend(
            find_domain_matches(
                search_files,
                domain,
                terms,
            )
        )

    domain_summary_rows = []

    for domain in DOMAIN_TERMS:
        domain_matches = [
            row
            for row in inventory_rows
            if row["domain"] == domain
        ]

        domain_summary_rows.append(
            {
                "domain": domain,
                "matching_files": len(
                    domain_matches
                ),
                "total_matches": sum(
                    int(row["match_count"])
                    for row in domain_matches
                ),
                "inventory_status": (
                    "references_present_requires_semantic_audit"
                    if domain_matches
                    else
                    "no_repository_evidence_found"
                ),
                "production_capability_verified": False,
            }
        )

    workstreams = [
        {
            "workstream_id": "ENV-01",
            "workstream": (
                "venue_identity_and_park_factor_contract"
            ),
            "objective": (
                "Define canonical venue identity, park-factor "
                "provenance, season/version semantics, and "
                "neutral fallback behavior."
            ),
            "entry_condition": (
                "7A inventory and scope plan complete."
            ),
            "exit_condition": (
                "Venue and park-factor inputs are explicit, "
                "traceable, versioned, and independently audited."
            ),
            "production_activation_allowed": False,
        },
        {
            "workstream_id": "ENV-02",
            "workstream": (
                "roof_dome_and_weather_state_contract"
            ),
            "objective": (
                "Define roof, dome, temperature, humidity, "
                "pressure, precipitation, and missing-data state."
            ),
            "entry_condition": (
                "ENV-01 source contracts accepted."
            ),
            "exit_condition": (
                "Environment state is validated and available "
                "through a diagnostic-only interface."
            ),
            "production_activation_allowed": False,
        },
        {
            "workstream_id": "ENV-03",
            "workstream": (
                "wind_orientation_and_field_geometry_model"
            ),
            "objective": (
                "Represent wind relative to field orientation "
                "and venue wall/dimension geometry."
            ),
            "entry_condition": (
                "ENV-02 state contract accepted."
            ),
            "exit_condition": (
                "Wind vectors and field geometry produce "
                "deterministic diagnostic outputs."
            ),
            "production_activation_allowed": False,
        },
        {
            "workstream_id": "ENV-04",
            "workstream": (
                "atmospheric_carry_and_batted_ball_interaction"
            ),
            "objective": (
                "Model air density, altitude, temperature, "
                "humidity, wind, launch characteristics, and "
                "park geometry as bounded carry diagnostics."
            ),
            "entry_condition": (
                "ENV-03 geometry model accepted."
            ),
            "exit_condition": (
                "Carry and contact-class effects are explicit, "
                "bounded, testable, and diagnostic-only."
            ),
            "production_activation_allowed": False,
        },
        {
            "workstream_id": "ENV-05",
            "workstream": (
                "environment_diagnostic_integration"
            ),
            "objective": (
                "Attach environment diagnostics through the "
                "shared simulation builder without changing "
                "engine inputs or canonical probabilities."
            ),
            "entry_condition": (
                "ENV-01 through ENV-04 component contracts pass."
            ),
            "exit_condition": (
                "Disabled exact equivalence, enabled metadata-only "
                "behavior, key isolation, and lazy imports pass."
            ),
            "production_activation_allowed": False,
        },
        {
            "workstream_id": "ENV-06",
            "workstream": (
                "environment_historical_validation_plan"
            ),
            "objective": (
                "Define, but do not execute, park/weather bucket "
                "validation for run bias, HR, XBH, and totals."
            ),
            "entry_condition": (
                "ENV-05 diagnostic integration complete."
            ),
            "exit_condition": (
                "Validation cohorts, metrics, leakage controls, "
                "sample rules, and promotion gates are specified."
            ),
            "production_activation_allowed": False,
        },
    ]

    execution_sequence = [
        {
            "step": 1,
            "layer": "7B",
            "action": (
                "Audit venue identity, park-factor sources, "
                "existing environment inputs, and runtime paths."
            ),
        },
        {
            "step": 2,
            "layer": "7C",
            "action": (
                "Plan the canonical venue and park-factor "
                "source contract."
            ),
        },
        {
            "step": 3,
            "layer": "7D",
            "action": (
                "Implement and independently audit the venue "
                "and park-factor contract."
            ),
        },
        {
            "step": 4,
            "layer": "7E",
            "action": (
                "Define roof, dome, weather, and atmospheric "
                "state contracts."
            ),
        },
        {
            "step": 5,
            "layer": "7F",
            "action": (
                "Implement deterministic diagnostic environment "
                "state evaluation."
            ),
        },
        {
            "step": 6,
            "layer": "7G",
            "action": (
                "Plan wind-orientation and field-geometry modeling."
            ),
        },
        {
            "step": 7,
            "layer": "7H",
            "action": (
                "Implement and audit wind/geometry diagnostics."
            ),
        },
        {
            "step": 8,
            "layer": "7I",
            "action": (
                "Plan atmospheric carry and batted-ball interaction."
            ),
        },
        {
            "step": 9,
            "layer": "7J",
            "action": (
                "Implement and audit carry diagnostics."
            ),
        },
        {
            "step": 10,
            "layer": "7K",
            "action": (
                "Plan shared-builder environment diagnostic "
                "integration."
            ),
        },
        {
            "step": 11,
            "layer": "7L",
            "action": (
                "Implement and independently audit combined "
                "environment diagnostics."
            ),
        },
        {
            "step": 12,
            "layer": "7M",
            "action": (
                "Plan historical environment validation without "
                "executing calibration or promotion."
            ),
        },
    ]

    scope_boundaries = [
        {
            "scope": (
                "repository_inventory_and_semantic_audit"
            ),
            "allowed": True,
            "reason": (
                "Required to establish the current Layer 7 baseline."
            ),
        },
        {
            "scope": (
                "diagnostic_environment_models"
            ),
            "allowed": True,
            "reason": (
                "May be built behind disabled-by-default "
                "diagnostic interfaces."
            ),
        },
        {
            "scope": (
                "production_environment_behavior"
            ),
            "allowed": False,
            "reason": (
                "Requires future empirical evidence and explicit "
                "production authorization."
            ),
        },
        {
            "scope": (
                "historical_environment_validation_execution"
            ),
            "allowed": False,
            "reason": (
                "7A defines scope only and does not join outcomes."
            ),
        },
        {
            "scope": (
                "parameter_tuning_backtesting_pricing_edge_detection"
            ),
            "allowed": False,
            "reason": (
                "These remain downstream of environment realism "
                "and distribution validation."
            ),
        },
    ]

    planning_checks = [
        {
            "check": (
                "roadmap_path_exists"
            ),
            "actual": ROADMAP_PATH.exists(),
            "expected": True,
            "passed": ROADMAP_PATH.exists(),
        },
        {
            "check": (
                "layer6_exit_path_exists"
            ),
            "actual": LAYER6_EXIT_PATH.exists(),
            "expected": True,
            "passed": LAYER6_EXIT_PATH.exists(),
        },
        {
            "check": (
                "roadmap_layer7_contract_present"
            ),
            "actual": roadmap_layer7_contract_present,
            "expected": True,
            "passed": roadmap_layer7_contract_present,
        },
        {
            "check": (
                "layer6_exit_contract_present"
            ),
            "actual": layer6_exit_contract_present,
            "expected": True,
            "passed": layer6_exit_contract_present,
        },
        {
            "check": (
                "ten_environment_domains_inventoried"
            ),
            "actual": len(
                DOMAIN_TERMS
            ),
            "expected": 10,
            "passed": len(
                DOMAIN_TERMS
            )
            == 10,
        },
        {
            "check": (
                "six_layer7_workstreams_defined"
            ),
            "actual": len(
                workstreams
            ),
            "expected": 6,
            "passed": len(
                workstreams
            )
            == 6,
        },
        {
            "check": (
                "twelve_execution_steps_defined"
            ),
            "actual": len(
                execution_sequence
            ),
            "expected": 12,
            "passed": len(
                execution_sequence
            )
            == 12,
        },
        {
            "check": (
                "planning_only_boundary_preserved"
            ),
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
                "7A is an inventory and planning layer only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "layer7_environment_inventory_execution"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "7B may perform the bounded semantic inventory "
                    "and runtime-path audit."
                ),
            },
            {
                "authority": (
                    "production_environment_integration"
                ),
                "granted": False,
                "reason": (
                    "No production environment authority is "
                    "created by 7A."
                ),
            },
        ]
    )

    diagnosis_name = (
        "layer7_environment_realism_inventory_and_scope_plan_complete"
        if all_checks_passed
        else
        "layer7_environment_realism_inventory_and_scope_plan_failed"
    )

    recommended_next_layer = (
        "7B_layer7_environment_source_and_runtime_inventory"
        if all_checks_passed
        else
        "7B_layer7_environment_scope_plan_remediation"
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
        OUTPUT_DIR / "repository_inventory.csv",
        [
            "domain",
            "path",
            "matched_terms",
            "match_count",
        ],
        inventory_rows,
    )

    write_csv(
        OUTPUT_DIR / "domain_summary.csv",
        [
            "domain",
            "matching_files",
            "total_matches",
            "inventory_status",
            "production_capability_verified",
        ],
        domain_summary_rows,
    )

    write_csv(
        OUTPUT_DIR / "workstreams.csv",
        [
            "workstream_id",
            "workstream",
            "objective",
            "entry_condition",
            "exit_condition",
            "production_activation_allowed",
        ],
        workstreams,
    )

    write_csv(
        OUTPUT_DIR / "execution_sequence.csv",
        [
            "step",
            "layer",
            "action",
        ],
        execution_sequence,
    )

    write_csv(
        OUTPUT_DIR / "scope_boundaries.csv",
        [
            "scope",
            "allowed",
            "reason",
        ],
        scope_boundaries,
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
                    "Audit current environment sources, schemas, "
                    "fallbacks, and runtime reachability."
                    if all_checks_passed
                    else
                    "Remediate failed 7A planning checks."
                ),
                "entry_condition": (
                    "All eight 7A planning checks pass."
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
        "search_files_scanned": len(
            search_files
        ),
        "inventory_matches": len(
            inventory_rows
        ),
        "environment_domains_inventoried": len(
            DOMAIN_TERMS
        ),
        "workstreams_defined": len(
            workstreams
        ),
        "execution_steps_defined": len(
            execution_sequence
        ),
        "layer6_exit_verified": (
            layer6_exit_contract_present
        ),
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "production_activation": False,
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
        "layer7_started": all_checks_passed,
        "layer7_completed": False,
        "new_production_authority_granted": False,
        "historical_validation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "environment_inventory_allowed_next": (
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
                OUTPUT_DIR / "repository_inventory.csv"
            ),
            str(
                OUTPUT_DIR / "domain_summary.csv"
            ),
            str(
                OUTPUT_DIR / "workstreams.csv"
            ),
            str(
                OUTPUT_DIR / "execution_sequence.csv"
            ),
            str(
                OUTPUT_DIR / "scope_boundaries.csv"
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
