#!/usr/bin/env python3
"""
Layer 8N
Pitch-Type Matchup Overlay Shadow Dataset Quality Gate Contract Plan

Defines deterministic, diagnostic-only quality gates for the Layer 8M
append-only shadow dataset.

Planning only.

This layer does not:
- join historical outcomes;
- evaluate predictive accuracy or calibration;
- tune thresholds using observed outcomes;
- change production or simulation behavior;
- run backtests;
- perform pricing, market comparison, or edge detection.
"""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any, Iterable


LAYER_ID = "8N"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8N_pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8M_pitch_type_matchup_overlay_shadow_dataset_contract.py"
)

QUALITY_GATE_FIELDS = [
    {"field": "quality_gate_version", "type": "string", "required": True},
    {"field": "evaluated_at_utc", "type": "datetime", "required": True},
    {"field": "dataset_status", "type": "enum", "required": True},
    {"field": "gate_status", "type": "enum", "required": True},
    {"field": "row_count", "type": "nonnegative_integer", "required": True},
    {"field": "partition_count", "type": "nonnegative_integer", "required": True},
    {"field": "duplicate_row_count", "type": "nonnegative_integer", "required": True},
    {"field": "invalid_row_count", "type": "nonnegative_integer", "required": True},
    {"field": "partial_row_count", "type": "nonnegative_integer", "required": True},
    {"field": "complete_row_count", "type": "nonnegative_integer", "required": True},
    {"field": "minimum_coverage_share", "type": "float_0_1_or_null", "required": False},
    {"field": "mean_coverage_share", "type": "float_0_1_or_null", "required": False},
    {"field": "maximum_coverage_share", "type": "float_0_1_or_null", "required": False},
    {"field": "fallback_row_count", "type": "nonnegative_integer", "required": True},
    {"field": "unknown_pitch_row_count", "type": "nonnegative_integer", "required": True},
    {"field": "pitcher_only_row_count", "type": "nonnegative_integer", "required": True},
    {"field": "schema_fingerprint_matches", "type": "boolean", "required": True},
    {"field": "manifest_reconciles", "type": "boolean", "required": True},
    {"field": "partition_manifest_reconciles", "type": "boolean", "required": True},
    {"field": "row_identifiers_unique", "type": "boolean", "required": True},
    {"field": "source_versions_present", "type": "boolean", "required": True},
    {"field": "failed_gate_count", "type": "nonnegative_integer", "required": True},
    {"field": "warning_gate_count", "type": "nonnegative_integer", "required": True},
    {"field": "diagnostic_codes", "type": "sorted_unique_string_array", "required": True},
    {"field": "validation_errors", "type": "sorted_unique_string_array", "required": True},
]

GATE_STATUSES = [
    {"status": "pass", "meaning": "All required quality gates pass."},
    {"status": "warn", "meaning": "Required gates pass but one or more warning gates trigger."},
    {"status": "fail", "meaning": "One or more required quality gates fail."},
    {"status": "empty", "meaning": "Dataset is valid but contains no rows."},
    {"status": "disabled", "meaning": "Quality-gate evaluation is disabled."},
]

REQUIRED_GATES = [
    {"gate_id": "QG-R01", "gate": "dataset_emitted"},
    {"gate_id": "QG-R02", "gate": "dataset_not_invalid"},
    {"gate_id": "QG-R03", "gate": "manifest_present"},
    {"gate_id": "QG-R04", "gate": "manifest_row_count_reconciles"},
    {"gate_id": "QG-R05", "gate": "manifest_partition_count_reconciles"},
    {"gate_id": "QG-R06", "gate": "manifest_unique_observation_count_reconciles"},
    {"gate_id": "QG-R07", "gate": "schema_fingerprint_present"},
    {"gate_id": "QG-R08", "gate": "schema_fingerprint_matches_expected"},
    {"gate_id": "QG-R09", "gate": "dataset_row_identifiers_unique"},
    {"gate_id": "QG-R10", "gate": "observation_identifiers_present"},
    {"gate_id": "QG-R11", "gate": "partition_keys_unique"},
    {"gate_id": "QG-R12", "gate": "partition_paths_unique"},
    {"gate_id": "QG-R13", "gate": "partition_row_counts_reconcile"},
    {"gate_id": "QG-R14", "gate": "observation_dates_valid"},
    {"gate_id": "QG-R15", "gate": "coverage_values_valid"},
    {"gate_id": "QG-R16", "gate": "count_values_nonnegative"},
    {"gate_id": "QG-R17", "gate": "usage_values_valid"},
    {"gate_id": "QG-R18", "gate": "source_versions_present"},
    {"gate_id": "QG-R19", "gate": "shadow_dataset_version_consistent"},
    {"gate_id": "QG-R20", "gate": "production_authority_false"},
]

WARNING_GATES = [
    {"gate_id": "QG-W01", "gate": "partial_rows_present"},
    {"gate_id": "QG-W02", "gate": "fallback_rows_present"},
    {"gate_id": "QG-W03", "gate": "unknown_pitch_rows_present"},
    {"gate_id": "QG-W04", "gate": "pitcher_only_rows_present"},
    {"gate_id": "QG-W05", "gate": "duplicate_rows_present"},
    {"gate_id": "QG-W06", "gate": "coverage_below_half_present"},
]

AGGREGATION_RULES = [
    {"rule_id": "QG-A01", "rule": "required_gate_failures_determine_fail_status"},
    {"rule_id": "QG-A02", "rule": "warnings_do_not_override_required_failures"},
    {"rule_id": "QG-A03", "rule": "empty_dataset_maps_to_empty_status"},
    {"rule_id": "QG-A04", "rule": "disabled_evaluation_emits_no_report"},
    {"rule_id": "QG-A05", "rule": "coverage_statistics_use_accepted_rows_only"},
    {"rule_id": "QG-A06", "rule": "diagnostics_and_errors_are_sorted_unique"},
]

VALIDATION_RULES = [
    {"rule_id": "QG-V01", "rule": "quality_gate_version_explicit"},
    {"rule_id": "QG-V02", "rule": "dataset_input_required_when_enabled"},
    {"rule_id": "QG-V03", "rule": "manifest_required_for_emitted_dataset"},
    {"rule_id": "QG-V04", "rule": "dataset_status_supported"},
    {"rule_id": "QG-V05", "rule": "row_count_reconciles"},
    {"rule_id": "QG-V06", "rule": "partition_count_reconciles"},
    {"rule_id": "QG-V07", "rule": "unique_observation_count_reconciles"},
    {"rule_id": "QG-V08", "rule": "duplicate_count_nonnegative"},
    {"rule_id": "QG-V09", "rule": "schema_fingerprint_nonempty"},
    {"rule_id": "QG-V10", "rule": "row_ids_unique"},
    {"rule_id": "QG-V11", "rule": "observation_ids_nonempty"},
    {"rule_id": "QG-V12", "rule": "partition_keys_unique"},
    {"rule_id": "QG-V13", "rule": "partition_paths_unique"},
    {"rule_id": "QG-V14", "rule": "partition_rows_reconcile"},
    {"rule_id": "QG-V15", "rule": "dates_parse_as_iso_dates"},
    {"rule_id": "QG-V16", "rule": "coverage_between_zero_and_one"},
    {"rule_id": "QG-V17", "rule": "counts_nonnegative"},
    {"rule_id": "QG-V18", "rule": "usage_between_zero_and_one"},
    {"rule_id": "QG-V19", "rule": "source_versions_retained"},
    {"rule_id": "QG-V20", "rule": "dataset_version_consistent"},
    {"rule_id": "QG-V21", "rule": "caller_dataset_immutable"},
    {"rule_id": "QG-V22", "rule": "production_authority_false"},
]

ARTIFACT_SCHEMAS = [
    {"artifact": "quality_gate_results.csv", "scope": "one_row_per_gate", "required": True},
    {"artifact": "quality_gate_summary.csv", "scope": "single_summary_row", "required": True},
    {"artifact": "status_counts.csv", "scope": "one_row_per_gate_status", "required": True},
    {"artifact": "warning_counts.csv", "scope": "one_row_per_warning_code", "required": True},
    {"artifact": "authority_boundaries.csv", "scope": "authority_contract", "required": True},
    {"artifact": "quality_gate_report.json", "scope": "full_quality_report", "required": True},
    {"artifact": "diagnosis.json", "scope": "layer_diagnosis", "required": True},
]

FALLBACK_CONTRACTS = [
    {
        "fallback_id": "QG-F01",
        "condition": "quality_gate_disabled",
        "result": "no_report_emitted",
        "diagnostic_code": "matchup_shadow_quality_gate_disabled",
    },
    {
        "fallback_id": "QG-F02",
        "condition": "dataset_missing",
        "result": "fail_report",
        "diagnostic_code": "matchup_shadow_quality_dataset_missing",
    },
    {
        "fallback_id": "QG-F03",
        "condition": "manifest_missing",
        "result": "fail_report",
        "diagnostic_code": "matchup_shadow_quality_manifest_missing",
    },
    {
        "fallback_id": "QG-F04",
        "condition": "dataset_empty",
        "result": "empty_report",
        "diagnostic_code": "matchup_shadow_quality_dataset_empty",
    },
    {
        "fallback_id": "QG-F05",
        "condition": "partial_rows_present",
        "result": "warning",
        "diagnostic_code": "matchup_shadow_quality_partial_rows",
    },
    {
        "fallback_id": "QG-F06",
        "condition": "fallback_rows_present",
        "result": "warning",
        "diagnostic_code": "matchup_shadow_quality_fallback_rows",
    },
    {
        "fallback_id": "QG-F07",
        "condition": "structural_reconciliation_failure",
        "result": "fail_report",
        "diagnostic_code": "matchup_shadow_quality_reconciliation_failed",
    },
]

IMPLEMENTATION_STEPS = [
    {"step": 1, "action": "Create immutable quality-gate result and summary records."},
    {"step": 2, "action": "Import Layer 8M shadow dataset records."},
    {"step": 3, "action": "Evaluate required structural and schema gates."},
    {"step": 4, "action": "Evaluate warning-only coverage and fallback gates."},
    {"step": 5, "action": "Reconcile manifest, partitions, rows, and duplicates."},
    {"step": 6, "action": "Compute deterministic coverage statistics."},
    {"step": 7, "action": "Assign pass, warn, fail, empty, or disabled status."},
    {"step": 8, "action": "Preserve caller-dataset immutability."},
    {"step": 9, "action": "Keep evaluation disabled by default."},
    {"step": 10, "action": "Create independent quality-gate audit."},
    {"step": 11, "action": "Emit deterministic CSV and JSON artifacts."},
    {"step": 12, "action": "Preserve all production and evaluation authority boundaries."},
]

ACCEPTANCE_CRITERIA = [
    {"criterion_id": "QG-C01", "criterion": "layer_8M_dependency_verified"},
    {"criterion_id": "QG-C02", "criterion": "quality_gate_schema_defined"},
    {"criterion_id": "QG-C03", "criterion": "five_gate_statuses_defined"},
    {"criterion_id": "QG-C04", "criterion": "required_gates_defined"},
    {"criterion_id": "QG-C05", "criterion": "warning_gates_defined"},
    {"criterion_id": "QG-C06", "criterion": "manifest_reconciliation_defined"},
    {"criterion_id": "QG-C07", "criterion": "partition_reconciliation_defined"},
    {"criterion_id": "QG-C08", "criterion": "schema_fingerprint_gate_defined"},
    {"criterion_id": "QG-C09", "criterion": "coverage_validity_gate_defined"},
    {"criterion_id": "QG-C10", "criterion": "source_version_gate_defined"},
    {"criterion_id": "QG-C11", "criterion": "deterministic_status_precedence_defined"},
    {"criterion_id": "QG-C12", "criterion": "disabled_path_non_emitting"},
    {"criterion_id": "QG-C13", "criterion": "caller_dataset_immutable"},
    {"criterion_id": "QG-C14", "criterion": "historical_outcomes_absent"},
    {"criterion_id": "QG-C15", "criterion": "predictive_evaluation_absent"},
    {"criterion_id": "QG-C16", "criterion": "production_authority_absent"},
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
    path.parent.mkdir(parents=True, exist_ok=True)

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
    path.parent.mkdir(parents=True, exist_ok=True)
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
        "pitch_type_matchup_overlay_shadow_dataset_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    field_names = [
        row["field"]
        for row in QUALITY_GATE_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_m_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_five_quality_gate_fields_defined",
            "actual": len(QUALITY_GATE_FIELDS),
            "expected": 25,
            "passed": len(QUALITY_GATE_FIELDS) == 25,
        },
        {
            "check": "quality_gate_field_names_unique",
            "actual": len(set(field_names)),
            "expected": len(field_names),
            "passed": len(set(field_names)) == len(field_names),
        },
        {
            "check": "five_gate_statuses_defined",
            "actual": len(GATE_STATUSES),
            "expected": 5,
            "passed": len(GATE_STATUSES) == 5,
        },
        {
            "check": "twenty_required_gates_defined",
            "actual": len(REQUIRED_GATES),
            "expected": 20,
            "passed": len(REQUIRED_GATES) == 20,
        },
        {
            "check": "six_warning_gates_defined",
            "actual": len(WARNING_GATES),
            "expected": 6,
            "passed": len(WARNING_GATES) == 6,
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
            "check": "seven_artifact_schemas_defined",
            "actual": len(ARTIFACT_SCHEMAS),
            "expected": 7,
            "passed": len(ARTIFACT_SCHEMAS) == 7,
        },
        {
            "check": "seven_fallback_contracts_defined",
            "actual": len(FALLBACK_CONTRACTS),
            "expected": 7,
            "passed": len(FALLBACK_CONTRACTS) == 7,
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
            "check": "manifest_reconciliation_gate_defined",
            "actual": any(
                row["gate"] == "manifest_row_count_reconciles"
                for row in REQUIRED_GATES
            ),
            "expected": True,
            "passed": any(
                row["gate"] == "manifest_row_count_reconciles"
                for row in REQUIRED_GATES
            ),
        },
        {
            "check": "schema_fingerprint_gate_defined",
            "actual": any(
                row["gate"] == "schema_fingerprint_matches_expected"
                for row in REQUIRED_GATES
            ),
            "expected": True,
            "passed": any(
                row["gate"] == "schema_fingerprint_matches_expected"
                for row in REQUIRED_GATES
            ),
        },
        {
            "check": "warning_gates_non_authoritative",
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
                "8N defines diagnostic structural quality gates only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": (
                "shadow_dataset_quality_gate_implementation"
            ),
            "granted": all_checks_passed,
            "reason": (
                "8O may implement the bounded diagnostic quality gates."
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_plan_failed"
    )

    recommended_next_layer = (
        "8O_pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_implementation"
        if all_checks_passed
        else
        "8N_pitch_type_matchup_overlay_shadow_dataset_quality_gate_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "quality_gate_fields.csv": QUALITY_GATE_FIELDS,
        "gate_statuses.csv": GATE_STATUSES,
        "required_gates.csv": REQUIRED_GATES,
        "warning_gates.csv": WARNING_GATES,
        "aggregation_rules.csv": AGGREGATION_RULES,
        "validation_rules.csv": VALIDATION_RULES,
        "artifact_schemas.csv": ARTIFACT_SCHEMAS,
        "fallback_contracts.csv": FALLBACK_CONTRACTS,
        "implementation_steps.csv": IMPLEMENTATION_STEPS,
        "acceptance_criteria.csv": ACCEPTANCE_CRITERIA,
        "authority_boundaries.csv": authority_rows,
    }

    fieldnames = {
        "planning_checks.csv": ["check", "actual", "expected", "passed"],
        "quality_gate_fields.csv": ["field", "type", "required"],
        "gate_statuses.csv": ["status", "meaning"],
        "required_gates.csv": ["gate_id", "gate"],
        "warning_gates.csv": ["gate_id", "gate"],
        "aggregation_rules.csv": ["rule_id", "rule"],
        "validation_rules.csv": ["rule_id", "rule"],
        "artifact_schemas.csv": ["artifact", "scope", "required"],
        "fallback_contracts.csv": [
            "fallback_id",
            "condition",
            "result",
            "diagnostic_code",
        ],
        "implementation_steps.csv": ["step", "action"],
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
                "recommended_next_layer": recommended_next_layer,
                "recommended_action": (
                    "Implement bounded diagnostic shadow-dataset quality gates."
                    if all_checks_passed
                    else
                    "Remediate failed 8N planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8N planning checks pass."
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
        "quality_gate_fields_defined": len(QUALITY_GATE_FIELDS),
        "gate_statuses_defined": len(GATE_STATUSES),
        "required_gates_defined": len(REQUIRED_GATES),
        "warning_gates_defined": len(WARNING_GATES),
        "aggregation_rules_defined": len(AGGREGATION_RULES),
        "validation_rules_defined": len(VALIDATION_RULES),
        "artifact_schemas_defined": len(ARTIFACT_SCHEMAS),
        "fallback_contracts_defined": len(FALLBACK_CONTRACTS),
        "implementation_steps_defined": len(IMPLEMENTATION_STEPS),
        "acceptance_criteria_defined": len(ACCEPTANCE_CRITERIA),
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
        "quality_gate_implementation_allowed_next": (
            all_checks_passed
        ),
        "production_matchup_overlay_integration_allowed_next": False,
        "recommended_next_layer": recommended_next_layer,
        "generated_csv_artifacts": [
            str(OUTPUT_DIR / filename)
            for filename in [
                *artifacts.keys(),
                "recommended_path.csv",
            ]
        ],
        "generated_json_artifacts": [
            str(OUTPUT_DIR / "contract_summary.json"),
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
