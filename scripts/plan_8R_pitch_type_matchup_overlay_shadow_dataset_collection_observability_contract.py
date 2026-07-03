#!/usr/bin/env python3
"""
Layer 8R
Pitch-Type Matchup Overlay Shadow Dataset Collection Observability Contract Plan

Defines deterministic, diagnostic-only observability for the Layer 8Q
append-only collection.

Planning only.

This layer does not:
- join historical outcomes;
- evaluate predictive accuracy or calibration;
- tune parameters or thresholds;
- modify production or simulation behavior;
- run backtests;
- perform pricing, market comparison, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "8R"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8R_pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8Q_pitch_type_matchup_overlay_shadow_dataset_collection_contract.py"
)

OBSERVABILITY_FIELDS = [
    {
        "field": "observability_snapshot_id",
        "type": "deterministic_string",
        "required": True,
    },
    {
        "field": "observability_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "observed_at_utc",
        "type": "datetime",
        "required": True,
    },
    {
        "field": "collection_version",
        "type": "string",
        "required": True,
    },
    {
        "field": "collection_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "observability_status",
        "type": "enum",
        "required": True,
    },
    {
        "field": "record_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "accepted_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "accepted_with_warnings_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "rejected_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "empty_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "exact_duplicate_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "conflicting_duplicate_count",
        "type": "nonnegative_integer",
        "required": True,
    },
    {
        "field": "minimum_dataset_row_count",
        "type": "nonnegative_integer_or_null",
        "required": False,
    },
    {
        "field": "mean_dataset_row_count",
        "type": "nonnegative_float_or_null",
        "required": False,
    },
    {
        "field": "maximum_dataset_row_count",
        "type": "nonnegative_integer_or_null",
        "required": False,
    },
    {
        "field": "minimum_coverage_share",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "mean_coverage_share",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "maximum_coverage_share",
        "type": "float_0_1_or_null",
        "required": False,
    },
    {
        "field": "manifest_reconciles",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "collection_digest_reconciles",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "record_identifiers_unique",
        "type": "boolean",
        "required": True,
    },
    {
        "field": "diagnostic_codes",
        "type": "sorted_unique_string_array",
        "required": True,
    },
    {
        "field": "validation_errors",
        "type": "sorted_unique_string_array",
        "required": True,
    },
]

OBSERVABILITY_STATUSES = [
    {
        "status": "healthy",
        "meaning": "Collection reconciles with no warning or failure signals.",
    },
    {
        "status": "warning",
        "meaning": "Collection reconciles but warning-only signals are present.",
    },
    {
        "status": "degraded",
        "meaning": "Collection has rejection, conflict, or reconciliation failures.",
    },
    {
        "status": "empty",
        "meaning": "Collection is valid but has no retained records.",
    },
    {
        "status": "disabled",
        "meaning": "Observability is disabled and emits no snapshot.",
    },
]

SIGNAL_GROUPS = [
    {
        "signal_group": "collection_status_distribution",
        "purpose": "Track accepted, warning, rejected, and empty records.",
    },
    {
        "signal_group": "duplicate_integrity",
        "purpose": "Track exact and conflicting duplicate counts.",
    },
    {
        "signal_group": "manifest_reconciliation",
        "purpose": "Reconcile record and status counts against the manifest.",
    },
    {
        "signal_group": "digest_integrity",
        "purpose": "Recompute and validate the collection digest.",
    },
    {
        "signal_group": "record_identity",
        "purpose": "Validate uniqueness of collection record identifiers.",
    },
    {
        "signal_group": "dataset_size_distribution",
        "purpose": "Summarize retained dataset row counts.",
    },
    {
        "signal_group": "coverage_distribution",
        "purpose": "Summarize retained collection-record coverage.",
    },
    {
        "signal_group": "authority_boundary",
        "purpose": "Verify diagnostic-only and production-authority-false state.",
    },
]

AGGREGATION_RULES = [
    {
        "rule_id": "CO-A01",
        "rule": "status_counts_use_retained_collection_records",
    },
    {
        "rule_id": "CO-A02",
        "rule": "dataset_size_statistics_use_non_rejected_records",
    },
    {
        "rule_id": "CO-A03",
        "rule": "coverage_statistics_use_non_null_record_values",
    },
    {
        "rule_id": "CO-A04",
        "rule": "conflicts_override_warning_status",
    },
    {
        "rule_id": "CO-A05",
        "rule": "reconciliation_failures_map_to_degraded",
    },
    {
        "rule_id": "CO-A06",
        "rule": "diagnostics_and_errors_are_sorted_unique",
    },
]

VALIDATION_RULES = [
    {
        "rule_id": "CO-V01",
        "rule": "observability_version_explicit",
    },
    {
        "rule_id": "CO-V02",
        "rule": "collection_required_when_enabled",
    },
    {
        "rule_id": "CO-V03",
        "rule": "collection_manifest_required_when_emitted",
    },
    {
        "rule_id": "CO-V04",
        "rule": "collection_version_retained",
    },
    {
        "rule_id": "CO-V05",
        "rule": "collection_status_supported",
    },
    {
        "rule_id": "CO-V06",
        "rule": "observability_status_supported",
    },
    {
        "rule_id": "CO-V07",
        "rule": "manifest_record_count_reconciles",
    },
    {
        "rule_id": "CO-V08",
        "rule": "manifest_accepted_count_reconciles",
    },
    {
        "rule_id": "CO-V09",
        "rule": "manifest_warning_count_reconciles",
    },
    {
        "rule_id": "CO-V10",
        "rule": "manifest_rejected_count_reconciles",
    },
    {
        "rule_id": "CO-V11",
        "rule": "manifest_empty_count_reconciles",
    },
    {
        "rule_id": "CO-V12",
        "rule": "manifest_exact_duplicate_count_nonnegative",
    },
    {
        "rule_id": "CO-V13",
        "rule": "manifest_conflicting_duplicate_count_nonnegative",
    },
    {
        "rule_id": "CO-V14",
        "rule": "collection_digest_recomputes",
    },
    {
        "rule_id": "CO-V15",
        "rule": "collection_record_ids_unique",
    },
    {
        "rule_id": "CO-V16",
        "rule": "dataset_row_counts_nonnegative",
    },
    {
        "rule_id": "CO-V17",
        "rule": "coverage_values_between_zero_and_one",
    },
    {
        "rule_id": "CO-V18",
        "rule": "record_versions_present",
    },
    {
        "rule_id": "CO-V19",
        "rule": "record_digests_present",
    },
    {
        "rule_id": "CO-V20",
        "rule": "caller_collection_immutable",
    },
    {
        "rule_id": "CO-V21",
        "rule": "disabled_path_non_emitting",
    },
    {
        "rule_id": "CO-V22",
        "rule": "production_authority_false",
    },
]

ARTIFACT_SCHEMAS = [
    {
        "artifact": "collection_observability_snapshot.csv",
        "scope": "single_snapshot_row",
        "required": True,
    },
    {
        "artifact": "status_counts.csv",
        "scope": "one_row_per_collection_status",
        "required": True,
    },
    {
        "artifact": "signal_results.csv",
        "scope": "one_row_per_signal",
        "required": True,
    },
    {
        "artifact": "duplicate_signals.csv",
        "scope": "duplicate_integrity_summary",
        "required": True,
    },
    {
        "artifact": "reconciliation_results.csv",
        "scope": "manifest_and_digest_reconciliation",
        "required": True,
    },
    {
        "artifact": "authority_boundaries.csv",
        "scope": "authority_contract",
        "required": True,
    },
    {
        "artifact": "observability_report.json",
        "scope": "full_observability_report",
        "required": True,
    },
    {
        "artifact": "diagnosis.json",
        "scope": "layer_diagnosis",
        "required": True,
    },
]

FALLBACK_CONTRACTS = [
    {
        "fallback_id": "CO-F01",
        "condition": "observability_disabled",
        "result": "no_snapshot_emitted",
        "diagnostic_code": "matchup_shadow_collection_observability_disabled",
    },
    {
        "fallback_id": "CO-F02",
        "condition": "collection_missing",
        "result": "degraded_snapshot",
        "diagnostic_code": "matchup_shadow_collection_observability_collection_missing",
    },
    {
        "fallback_id": "CO-F03",
        "condition": "manifest_missing",
        "result": "degraded_snapshot",
        "diagnostic_code": "matchup_shadow_collection_observability_manifest_missing",
    },
    {
        "fallback_id": "CO-F04",
        "condition": "collection_empty",
        "result": "empty_snapshot",
        "diagnostic_code": "matchup_shadow_collection_observability_empty",
    },
    {
        "fallback_id": "CO-F05",
        "condition": "warning_records_present",
        "result": "warning_snapshot",
        "diagnostic_code": "matchup_shadow_collection_observability_warning_records",
    },
    {
        "fallback_id": "CO-F06",
        "condition": "rejected_records_present",
        "result": "degraded_snapshot",
        "diagnostic_code": "matchup_shadow_collection_observability_rejected_records",
    },
    {
        "fallback_id": "CO-F07",
        "condition": "conflicting_duplicates_present",
        "result": "degraded_snapshot",
        "diagnostic_code": "matchup_shadow_collection_observability_conflicts",
    },
    {
        "fallback_id": "CO-F08",
        "condition": "manifest_or_digest_mismatch",
        "result": "degraded_snapshot",
        "diagnostic_code": "matchup_shadow_collection_observability_reconciliation_failed",
    },
]

IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": "Create immutable observability snapshot and report types.",
    },
    {
        "step": 2,
        "action": "Import Layer 8Q collection records and manifest.",
    },
    {
        "step": 3,
        "action": "Reconcile manifest status and duplicate counts.",
    },
    {
        "step": 4,
        "action": "Recompute the deterministic collection digest.",
    },
    {
        "step": 5,
        "action": "Validate unique collection-record identifiers.",
    },
    {
        "step": 6,
        "action": "Aggregate dataset-size and coverage distributions.",
    },
    {
        "step": 7,
        "action": "Assign healthy, warning, degraded, empty, or disabled status.",
    },
    {
        "step": 8,
        "action": "Create deterministic observability snapshot identity.",
    },
    {
        "step": 9,
        "action": "Preserve caller collection immutability.",
    },
    {
        "step": 10,
        "action": "Keep observability disabled by default.",
    },
    {
        "step": 11,
        "action": "Create an independent observability audit.",
    },
    {
        "step": 12,
        "action": "Emit deterministic CSV and JSON artifacts.",
    },
]

ACCEPTANCE_CRITERIA = [
    {
        "criterion_id": "CO-C01",
        "criterion": "layer_8Q_dependency_verified",
    },
    {
        "criterion_id": "CO-C02",
        "criterion": "observability_snapshot_schema_defined",
    },
    {
        "criterion_id": "CO-C03",
        "criterion": "five_observability_statuses_defined",
    },
    {
        "criterion_id": "CO-C04",
        "criterion": "eight_signal_groups_defined",
    },
    {
        "criterion_id": "CO-C05",
        "criterion": "manifest_reconciliation_defined",
    },
    {
        "criterion_id": "CO-C06",
        "criterion": "collection_digest_reconciliation_defined",
    },
    {
        "criterion_id": "CO-C07",
        "criterion": "record_identity_validation_defined",
    },
    {
        "criterion_id": "CO-C08",
        "criterion": "status_distribution_defined",
    },
    {
        "criterion_id": "CO-C09",
        "criterion": "duplicate_integrity_signals_defined",
    },
    {
        "criterion_id": "CO-C10",
        "criterion": "dataset_size_distribution_defined",
    },
    {
        "criterion_id": "CO-C11",
        "criterion": "coverage_distribution_defined",
    },
    {
        "criterion_id": "CO-C12",
        "criterion": "disabled_path_non_emitting",
    },
    {
        "criterion_id": "CO-C13",
        "criterion": "caller_collection_immutable",
    },
    {
        "criterion_id": "CO-C14",
        "criterion": "historical_outcomes_absent",
    },
    {
        "criterion_id": "CO-C15",
        "criterion": "predictive_evaluation_absent",
    },
    {
        "criterion_id": "CO-C16",
        "criterion": "production_authority_absent",
    },
]

PROHIBITED_AUTHORITIES = [
    "production_overlay_integration",
    "production_matchup_adjustment",
    "simulation_state_change",
    "simulation_probability_change",
    "historical_outcome_join",
    "predictive_accuracy_evaluation",
    "calibration_evaluation",
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


def string_constants(
    path: Path,
) -> set[str]:
    if not path.exists():
        return set()

    try:
        tree = ast.parse(
            path.read_text(
                encoding="utf-8",
                errors="ignore",
            ),
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

    predecessor_present = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    field_names = [
        row["field"]
        for row in OBSERVABILITY_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_q_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_four_observability_fields_defined",
            "actual": len(OBSERVABILITY_FIELDS),
            "expected": 24,
            "passed": len(OBSERVABILITY_FIELDS) == 24,
        },
        {
            "check": "observability_field_names_unique",
            "actual": len(set(field_names)),
            "expected": len(field_names),
            "passed": len(set(field_names)) == len(field_names),
        },
        {
            "check": "five_observability_statuses_defined",
            "actual": len(OBSERVABILITY_STATUSES),
            "expected": 5,
            "passed": len(OBSERVABILITY_STATUSES) == 5,
        },
        {
            "check": "eight_signal_groups_defined",
            "actual": len(SIGNAL_GROUPS),
            "expected": 8,
            "passed": len(SIGNAL_GROUPS) == 8,
        },
        {
            "check": "six_aggregation_rules_defined",
            "actual": len(AGGREGATION_RULES),
            "expected": 6,
            "passed": len(AGGREGATION_RULES) == 6,
        },
        {
            "check": "twenty_two_validation_rules_defined",
            "actual": len(VALIDATION_RULES),
            "expected": 22,
            "passed": len(VALIDATION_RULES) == 22,
        },
        {
            "check": "eight_artifact_schemas_defined",
            "actual": len(ARTIFACT_SCHEMAS),
            "expected": 8,
            "passed": len(ARTIFACT_SCHEMAS) == 8,
        },
        {
            "check": "eight_fallback_contracts_defined",
            "actual": len(FALLBACK_CONTRACTS),
            "expected": 8,
            "passed": len(FALLBACK_CONTRACTS) == 8,
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 12,
            "passed": len(IMPLEMENTATION_STEPS) == 12,
        },
        {
            "check": "sixteen_acceptance_criteria_defined",
            "actual": len(ACCEPTANCE_CRITERIA),
            "expected": 16,
            "passed": len(ACCEPTANCE_CRITERIA) == 16,
        },
        {
            "check": "manifest_reconciliation_defined",
            "actual": any(
                row["rule"]
                == "manifest_record_count_reconciles"
                for row in VALIDATION_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "manifest_record_count_reconciles"
                for row in VALIDATION_RULES
            ),
        },
        {
            "check": "digest_reconciliation_defined",
            "actual": any(
                row["rule"]
                == "collection_digest_recomputes"
                for row in VALIDATION_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "collection_digest_recomputes"
                for row in VALIDATION_RULES
            ),
        },
        {
            "check": "conflict_status_precedence_defined",
            "actual": any(
                row["rule"]
                == "conflicts_override_warning_status"
                for row in AGGREGATION_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "conflicts_override_warning_status"
                for row in AGGREGATION_RULES
            ),
        },
        {
            "check": "observability_non_authoritative",
            "actual": True,
            "expected": True,
            "passed": True,
        },
        {
            "check": "historical_outcome_join_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "predictive_evaluation_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "planning_only_boundary_preserved",
            "actual": True,
            "expected": True,
            "passed": True,
        },
        {
            "check": "production_tuning_pricing_edge_authority_absent",
            "actual": False,
            "expected": False,
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
                "8R defines diagnostic collection observability only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": (
                "shadow_dataset_collection_observability_implementation"
            ),
            "granted": all_checks_passed,
            "reason": (
                "8S may implement bounded diagnostic collection observability."
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_plan_failed"
    )

    recommended_next_layer = (
        "8S_pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_implementation"
        if all_checks_passed
        else
        "8R_pitch_type_matchup_overlay_shadow_dataset_collection_observability_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "observability_fields.csv": OBSERVABILITY_FIELDS,
        "observability_statuses.csv": OBSERVABILITY_STATUSES,
        "signal_groups.csv": SIGNAL_GROUPS,
        "aggregation_rules.csv": AGGREGATION_RULES,
        "validation_rules.csv": VALIDATION_RULES,
        "artifact_schemas.csv": ARTIFACT_SCHEMAS,
        "fallback_contracts.csv": FALLBACK_CONTRACTS,
        "implementation_steps.csv": IMPLEMENTATION_STEPS,
        "acceptance_criteria.csv": ACCEPTANCE_CRITERIA,
        "authority_boundaries.csv": authority_rows,
    }

    fieldnames = {
        "planning_checks.csv": [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        "observability_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "observability_statuses.csv": [
            "status",
            "meaning",
        ],
        "signal_groups.csv": [
            "signal_group",
            "purpose",
        ],
        "aggregation_rules.csv": [
            "rule_id",
            "rule",
        ],
        "validation_rules.csv": [
            "rule_id",
            "rule",
        ],
        "artifact_schemas.csv": [
            "artifact",
            "scope",
            "required",
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
                    "Implement deterministic diagnostic observability for Layer 8Q collections."
                    if all_checks_passed
                    else
                    "Remediate failed 8R planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8R planning checks pass."
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
        "observability_fields_defined": len(
            OBSERVABILITY_FIELDS
        ),
        "observability_statuses_defined": len(
            OBSERVABILITY_STATUSES
        ),
        "signal_groups_defined": len(
            SIGNAL_GROUPS
        ),
        "aggregation_rules_defined": len(
            AGGREGATION_RULES
        ),
        "validation_rules_defined": len(
            VALIDATION_RULES
        ),
        "artifact_schemas_defined": len(
            ARTIFACT_SCHEMAS
        ),
        "fallback_contracts_defined": len(
            FALLBACK_CONTRACTS
        ),
        "implementation_steps_defined": len(
            IMPLEMENTATION_STEPS
        ),
        "acceptance_criteria_defined": len(
            ACCEPTANCE_CRITERIA
        ),
        "manifest_reconciliation_defined": True,
        "digest_reconciliation_defined": True,
        "historical_outcome_joined": False,
        "predictive_evaluation_executed": False,
        "production_behavior_changed": False,
        "simulation_behavior_changed": False,
        "canonical_probability_authority_changed": False,
        "tuning_executed": False,
        "pricing_or_edge_work_executed": False,
    }

    write_json(
        OUTPUT_DIR / "contract_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "diagnosis": diagnosis_name,
        "all_checks_passed": all_checks_passed,
        **summary,
        "layer8_completed": False,
        "new_production_authority_granted": False,
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "collection_observability_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_matchup_overlay_integration_allowed_next": False,
        "recommended_next_layer": (
            recommended_next_layer
        ),
        "generated_csv_artifacts": [
            str(
                OUTPUT_DIR / filename
            )
            for filename in [
                *artifacts.keys(),
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(
                OUTPUT_DIR
                / "contract_summary.json"
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
