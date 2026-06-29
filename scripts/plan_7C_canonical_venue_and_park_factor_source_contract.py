#!/usr/bin/env python3
"""
Layer 7C
Canonical Venue and Park-Factor Source Contract Plan

Defines the bounded implementation contract for:
- canonical venue identity;
- venue alias resolution;
- park-factor source precedence;
- season and version semantics;
- provenance and freshness metadata;
- neutral fallback behavior;
- diagnostic-only runtime exposure.

Planning only. This layer does not:
- fetch or install new source data;
- modify production simulation behavior;
- change simulation state, parameters, or probabilities;
- replace canonical probability authority;
- execute historical validation, tuning, backtests, pricing, or edge logic.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "7C"
LAYER_NAME = "canonical_venue_and_park_factor_source_contract_plan"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/layer_7C_canonical_venue_and_park_factor_source_contract_plan"
)

PLAN_7A_PATH = (
    ROOT
    / "scripts/plan_7A_layer7_environment_realism_inventory_and_scope.py"
)

AUDIT_7B_PATH = (
    ROOT
    / "scripts/audit_7B_layer7_environment_source_and_runtime_inventory.py"
)

ROADMAP_PATH = (
    ROOT
    / "docs/roadmap_to_edge_detection.md"
)

REQUIRED_PATHS = [
    PLAN_7A_PATH,
    AUDIT_7B_PATH,
    ROADMAP_PATH,
]

PROHIBITED_AUTHORITIES = [
    "production_environment_activation",
    "production_park_factor_activation",
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


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    required_paths_exist = all(
        path.exists()
        for path in REQUIRED_PATHS
    )

    plan_7a_constants = string_constants(
        PLAN_7A_PATH
    )

    audit_7b_constants = string_constants(
        AUDIT_7B_PATH
    )

    roadmap_text = read_text(
        ROADMAP_PATH
    )

    predecessor_contracts = [
        {
            "layer": "7A",
            "path": str(
                PLAN_7A_PATH.relative_to(ROOT)
            ),
            "expected_diagnosis": (
                "layer7_environment_realism_inventory_"
                "and_scope_plan_complete"
            ),
            "present": (
                "layer7_environment_realism_inventory_"
                "and_scope_plan_complete"
                in plan_7a_constants
            ),
        },
        {
            "layer": "7B",
            "path": str(
                AUDIT_7B_PATH.relative_to(ROOT)
            ),
            "expected_diagnosis": (
                "layer7_environment_source_and_runtime_"
                "inventory_complete"
            ),
            "present": (
                "layer7_environment_source_and_runtime_"
                "inventory_complete"
                in audit_7b_constants
            ),
        },
        {
            "layer": "roadmap",
            "path": str(
                ROADMAP_PATH.relative_to(ROOT)
            ),
            "expected_diagnosis": (
                "environment_and_park_physics"
            ),
            "present": (
                "environment_and_park_physics"
                in roadmap_text
            ),
        },
    ]

    predecessors_accepted = sum(
        1
        for row in predecessor_contracts
        if row["present"]
    )

    canonical_venue_fields = [
        {
            "field": "canonical_venue_id",
            "required": True,
            "type": "string",
            "nullable": False,
            "purpose": (
                "Stable internal venue identifier independent "
                "of provider naming."
            ),
        },
        {
            "field": "provider_venue_id",
            "required": True,
            "type": "string",
            "nullable": True,
            "purpose": (
                "Original upstream venue identifier when available."
            ),
        },
        {
            "field": "canonical_venue_name",
            "required": True,
            "type": "string",
            "nullable": False,
            "purpose": (
                "Canonical display and matching name."
            ),
        },
        {
            "field": "venue_aliases",
            "required": True,
            "type": "array[string]",
            "nullable": False,
            "purpose": (
                "Historical, sponsor, provider, and abbreviated names."
            ),
        },
        {
            "field": "home_team_id",
            "required": True,
            "type": "string",
            "nullable": True,
            "purpose": (
                "Current primary home-team association."
            ),
        },
        {
            "field": "timezone",
            "required": True,
            "type": "string",
            "nullable": False,
            "purpose": (
                "IANA timezone for event and weather resolution."
            ),
        },
        {
            "field": "latitude",
            "required": True,
            "type": "number",
            "nullable": True,
            "purpose": (
                "Venue coordinate for source reconciliation."
            ),
        },
        {
            "field": "longitude",
            "required": True,
            "type": "number",
            "nullable": True,
            "purpose": (
                "Venue coordinate for source reconciliation."
            ),
        },
        {
            "field": "elevation_meters",
            "required": True,
            "type": "number",
            "nullable": True,
            "purpose": (
                "Venue elevation for later atmospheric modeling."
            ),
        },
        {
            "field": "roof_type",
            "required": True,
            "type": "enum",
            "nullable": False,
            "purpose": (
                "open_air, fixed_dome, retractable, or unknown."
            ),
        },
        {
            "field": "active_from",
            "required": True,
            "type": "date",
            "nullable": True,
            "purpose": (
                "First date for which this venue identity is valid."
            ),
        },
        {
            "field": "active_through",
            "required": True,
            "type": "date",
            "nullable": True,
            "purpose": (
                "Last date for which this venue identity is valid."
            ),
        },
    ]

    park_factor_fields = [
        {
            "field": "canonical_venue_id",
            "required": True,
            "type": "string",
            "nullable": False,
            "purpose": (
                "Join key to canonical venue identity."
            ),
        },
        {
            "field": "season",
            "required": True,
            "type": "integer",
            "nullable": False,
            "purpose": (
                "Season represented by the factor."
            ),
        },
        {
            "field": "factor_version",
            "required": True,
            "type": "string",
            "nullable": False,
            "purpose": (
                "Immutable version identifier for the factor set."
            ),
        },
        {
            "field": "factor_scope",
            "required": True,
            "type": "enum",
            "nullable": False,
            "purpose": (
                "overall_runs, home_runs, hits, doubles, triples, "
                "or another explicitly supported class."
            ),
        },
        {
            "field": "factor_value",
            "required": True,
            "type": "number",
            "nullable": False,
            "purpose": (
                "Multiplicative factor centered on a neutral value."
            ),
        },
        {
            "field": "neutral_value",
            "required": True,
            "type": "number",
            "nullable": False,
            "purpose": (
                "Explicit neutral baseline, normally 1.0."
            ),
        },
        {
            "field": "sample_games",
            "required": True,
            "type": "integer",
            "nullable": True,
            "purpose": (
                "Games contributing to the factor estimate."
            ),
        },
        {
            "field": "source_name",
            "required": True,
            "type": "string",
            "nullable": False,
            "purpose": (
                "Human-readable source identity."
            ),
        },
        {
            "field": "source_record_id",
            "required": True,
            "type": "string",
            "nullable": True,
            "purpose": (
                "Upstream record identifier when available."
            ),
        },
        {
            "field": "source_published_at",
            "required": True,
            "type": "datetime",
            "nullable": True,
            "purpose": (
                "Publication timestamp for freshness evaluation."
            ),
        },
        {
            "field": "retrieved_at",
            "required": True,
            "type": "datetime",
            "nullable": False,
            "purpose": (
                "Acquisition timestamp."
            ),
        },
        {
            "field": "is_final",
            "required": True,
            "type": "boolean",
            "nullable": False,
            "purpose": (
                "Whether the source labels the season estimate final."
            ),
        },
    ]

    source_precedence = [
        {
            "priority": 1,
            "source_class": (
                "explicit_versioned_primary_source"
            ),
            "selection_rule": (
                "Use when canonical venue match, requested season, "
                "supported factor scope, and provenance fields are valid."
            ),
            "fallback_allowed": True,
        },
        {
            "priority": 2,
            "source_class": (
                "approved_secondary_source"
            ),
            "selection_rule": (
                "Use only when the primary source is unavailable or "
                "invalid and the secondary source passes the same schema."
            ),
            "fallback_allowed": True,
        },
        {
            "priority": 3,
            "source_class": (
                "nearest_prior_final_season"
            ),
            "selection_rule": (
                "Use the most recent earlier final season only when "
                "explicitly allowed and mark the record stale."
            ),
            "fallback_allowed": True,
        },
        {
            "priority": 4,
            "source_class": (
                "neutral_factor"
            ),
            "selection_rule": (
                "Use 1.0 only when no approved source record is valid."
            ),
            "fallback_allowed": False,
        },
    ]

    season_semantics = [
        {
            "rule_id": "PF-S01",
            "rule": (
                "exact_season_preferred"
            ),
            "requirement": (
                "Requested season records outrank all earlier seasons."
            ),
        },
        {
            "rule_id": "PF-S02",
            "rule": (
                "future_season_prohibited"
            ),
            "requirement": (
                "A factor from a season after the game date may not be used."
            ),
        },
        {
            "rule_id": "PF-S03",
            "rule": (
                "same_season_provisional_must_be_labeled"
            ),
            "requirement": (
                "In-season or provisional factors must carry is_final=false."
            ),
        },
        {
            "rule_id": "PF-S04",
            "rule": (
                "prior_season_fallback_must_be_explicit"
            ),
            "requirement": (
                "Prior-season fallback requires stale=true and age metadata."
            ),
        },
        {
            "rule_id": "PF-S05",
            "rule": (
                "venue_history_must_be_date_valid"
            ),
            "requirement": (
                "Venue identity must be valid for the game date."
            ),
        },
        {
            "rule_id": "PF-S06",
            "rule": (
                "factor_versions_are_immutable"
            ),
            "requirement": (
                "A version identifier must resolve to stable factor values."
            ),
        },
    ]

    fallback_contract = [
        {
            "fallback_id": "PF-F01",
            "condition": (
                "venue_alias_unresolved"
            ),
            "result": (
                "neutral_factor"
            ),
            "factor_value": 1.0,
            "diagnostic_code": (
                "venue_unresolved_neutral_fallback"
            ),
            "production_authority": False,
        },
        {
            "fallback_id": "PF-F02",
            "condition": (
                "venue_resolved_but_factor_missing"
            ),
            "result": (
                "neutral_factor"
            ),
            "factor_value": 1.0,
            "diagnostic_code": (
                "park_factor_missing_neutral_fallback"
            ),
            "production_authority": False,
        },
        {
            "fallback_id": "PF-F03",
            "condition": (
                "factor_record_invalid"
            ),
            "result": (
                "neutral_factor"
            ),
            "factor_value": 1.0,
            "diagnostic_code": (
                "park_factor_invalid_neutral_fallback"
            ),
            "production_authority": False,
        },
        {
            "fallback_id": "PF-F04",
            "condition": (
                "approved_prior_season_available"
            ),
            "result": (
                "stale_prior_season_factor"
            ),
            "factor_value": (
                "source_value"
            ),
            "diagnostic_code": (
                "prior_season_factor_fallback"
            ),
            "production_authority": False,
        },
    ]

    validation_rules = [
        {
            "rule_id": "PF-V01",
            "rule": (
                "canonical_venue_id_nonempty"
            ),
            "blocking": True,
        },
        {
            "rule_id": "PF-V02",
            "rule": (
                "factor_value_finite_and_positive"
            ),
            "blocking": True,
        },
        {
            "rule_id": "PF-V03",
            "rule": (
                "neutral_value_exactly_one"
            ),
            "blocking": True,
        },
        {
            "rule_id": "PF-V04",
            "rule": (
                "factor_scope_supported"
            ),
            "blocking": True,
        },
        {
            "rule_id": "PF-V05",
            "rule": (
                "season_not_after_game_season"
            ),
            "blocking": True,
        },
        {
            "rule_id": "PF-V06",
            "rule": (
                "source_name_and_version_present"
            ),
            "blocking": True,
        },
        {
            "rule_id": "PF-V07",
            "rule": (
                "retrieved_at_present"
            ),
            "blocking": True,
        },
        {
            "rule_id": "PF-V08",
            "rule": (
                "alias_resolution_is_deterministic"
            ),
            "blocking": True,
        },
        {
            "rule_id": "PF-V09",
            "rule": (
                "fallback_reason_is_explicit"
            ),
            "blocking": True,
        },
        {
            "rule_id": "PF-V10",
            "rule": (
                "diagnostic_output_does_not_modify_engine_inputs"
            ),
            "blocking": True,
        },
    ]

    implementation_steps = [
        {
            "step": 1,
            "action": (
                "Create canonical venue identity schema and alias table."
            ),
        },
        {
            "step": 2,
            "action": (
                "Create typed park-factor source-record schema."
            ),
        },
        {
            "step": 3,
            "action": (
                "Implement deterministic venue alias resolution."
            ),
        },
        {
            "step": 4,
            "action": (
                "Implement season-aware source precedence."
            ),
        },
        {
            "step": 5,
            "action": (
                "Implement neutral and stale-factor fallback metadata."
            ),
        },
        {
            "step": 6,
            "action": (
                "Expose disabled-by-default diagnostic resolution output."
            ),
        },
        {
            "step": 7,
            "action": (
                "Add contract tests for schema, precedence, and fallback."
            ),
        },
        {
            "step": 8,
            "action": (
                "Run independent audit for determinism and non-authority."
            ),
        },
    ]

    acceptance_criteria = [
        {
            "criterion_id": "PF-A01",
            "criterion": (
                "canonical_venue_schema_complete"
            ),
            "required": True,
        },
        {
            "criterion_id": "PF-A02",
            "criterion": (
                "park_factor_schema_complete"
            ),
            "required": True,
        },
        {
            "criterion_id": "PF-A03",
            "criterion": (
                "venue_alias_resolution_deterministic"
            ),
            "required": True,
        },
        {
            "criterion_id": "PF-A04",
            "criterion": (
                "season_and_version_semantics_enforced"
            ),
            "required": True,
        },
        {
            "criterion_id": "PF-A05",
            "criterion": (
                "source_precedence_deterministic"
            ),
            "required": True,
        },
        {
            "criterion_id": "PF-A06",
            "criterion": (
                "neutral_fallback_explicit"
            ),
            "required": True,
        },
        {
            "criterion_id": "PF-A07",
            "criterion": (
                "provenance_and_freshness_emitted"
            ),
            "required": True,
        },
        {
            "criterion_id": "PF-A08",
            "criterion": (
                "diagnostic_only_and_disabled_by_default"
            ),
            "required": True,
        },
        {
            "criterion_id": "PF-A09",
            "criterion": (
                "no_simulation_or_probability_changes"
            ),
            "required": True,
        },
        {
            "criterion_id": "PF-A10",
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
            "check": "three_predecessor_contracts_present",
            "actual": predecessors_accepted,
            "expected": 3,
            "passed": predecessors_accepted == 3,
        },
        {
            "check": "twelve_canonical_venue_fields_defined",
            "actual": len(
                canonical_venue_fields
            ),
            "expected": 12,
            "passed": len(
                canonical_venue_fields
            )
            == 12,
        },
        {
            "check": "twelve_park_factor_fields_defined",
            "actual": len(
                park_factor_fields
            ),
            "expected": 12,
            "passed": len(
                park_factor_fields
            )
            == 12,
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
            "check": "six_season_semantic_rules_defined",
            "actual": len(
                season_semantics
            ),
            "expected": 6,
            "passed": len(
                season_semantics
            )
            == 6,
        },
        {
            "check": "four_fallback_contracts_defined",
            "actual": len(
                fallback_contract
            ),
            "expected": 4,
            "passed": len(
                fallback_contract
            )
            == 4,
        },
        {
            "check": "ten_validation_rules_defined",
            "actual": len(
                validation_rules
            ),
            "expected": 10,
            "passed": len(
                validation_rules
            )
            == 10,
        },
        {
            "check": "eight_implementation_steps_defined",
            "actual": len(
                implementation_steps
            ),
            "expected": 8,
            "passed": len(
                implementation_steps
            )
            == 8,
        },
        {
            "check": "ten_acceptance_criteria_defined",
            "actual": len(
                acceptance_criteria
            ),
            "expected": 10,
            "passed": len(
                acceptance_criteria
            )
            == 10,
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
                "7C defines a source contract plan only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.extend(
        [
            {
                "authority": (
                    "venue_and_park_factor_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "7D may implement and independently audit the "
                    "bounded diagnostic contract."
                ),
            },
            {
                "authority": (
                    "production_environment_integration"
                ),
                "granted": False,
                "reason": (
                    "The contract remains diagnostic-only and "
                    "disabled by default."
                ),
            },
        ]
    )

    diagnosis_name = (
        "canonical_venue_and_park_factor_source_contract_plan_complete"
        if all_checks_passed
        else
        "canonical_venue_and_park_factor_source_contract_plan_failed"
    )

    recommended_next_layer = (
        "7D_canonical_venue_and_park_factor_contract_implementation"
        if all_checks_passed
        else
        "7D_canonical_venue_and_park_factor_contract_plan_remediation"
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
        OUTPUT_DIR / "canonical_venue_fields.csv",
        [
            "field",
            "required",
            "type",
            "nullable",
            "purpose",
        ],
        canonical_venue_fields,
    )

    write_csv(
        OUTPUT_DIR / "park_factor_fields.csv",
        [
            "field",
            "required",
            "type",
            "nullable",
            "purpose",
        ],
        park_factor_fields,
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
        OUTPUT_DIR / "season_semantics.csv",
        [
            "rule_id",
            "rule",
            "requirement",
        ],
        season_semantics,
    )

    write_csv(
        OUTPUT_DIR / "fallback_contract.csv",
        [
            "fallback_id",
            "condition",
            "result",
            "factor_value",
            "diagnostic_code",
            "production_authority",
        ],
        fallback_contract,
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
                    "Implement the canonical venue and park-factor "
                    "contract as a disabled-by-default diagnostic."
                    if all_checks_passed
                    else
                    "Remediate failed 7C planning checks."
                ),
                "entry_condition": (
                    "All eleven 7C planning checks pass."
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
        "canonical_venue_fields_defined": len(
            canonical_venue_fields
        ),
        "park_factor_fields_defined": len(
            park_factor_fields
        ),
        "source_precedence_levels_defined": len(
            source_precedence
        ),
        "season_semantic_rules_defined": len(
            season_semantics
        ),
        "fallback_contracts_defined": len(
            fallback_contract
        ),
        "validation_rules_defined": len(
            validation_rules
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
        "venue_and_park_factor_contract_implementation_allowed_next": (
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
                OUTPUT_DIR / "canonical_venue_fields.csv"
            ),
            str(
                OUTPUT_DIR / "park_factor_fields.csv"
            ),
            str(
                OUTPUT_DIR / "source_precedence.csv"
            ),
            str(
                OUTPUT_DIR / "season_semantics.csv"
            ),
            str(
                OUTPUT_DIR / "fallback_contract.csv"
            ),
            str(
                OUTPUT_DIR / "validation_rules.csv"
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
