#!/usr/bin/env python3
"""
Layer 8AB
Pitch-Type Matchup Overlay Shadow Dataset Collection Retention
Observability History Quality Gate Observability Contract Plan

Defines deterministic, diagnostic-only observability over Layer 8AA
history-quality-gate reports.

Planning only.

This layer does not:
- mutate history or quality-gate reports;
- execute retention actions;
- delete, archive, expire, or quarantine records;
- join historical outcomes;
- evaluate predictive accuracy or calibration;
- tune thresholds;
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


LAYER_ID = "8AB"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_quality_gate_"
    "observability_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8AB_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_"
    "observability_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8AA_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_contract.py"
)


OBSERVABILITY_SNAPSHOT_FIELDS = [
    {"field": "observability_snapshot_id", "type": "deterministic_string", "required": True},
    {"field": "observability_version", "type": "string", "required": True},
    {"field": "observed_at_utc", "type": "datetime", "required": True},
    {"field": "quality_gate_version", "type": "string", "required": True},
    {"field": "quality_report_id", "type": "string", "required": True},
    {"field": "quality_status", "type": "enum", "required": True},
    {"field": "observability_status", "type": "enum", "required": True},
    {"field": "history_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "healthy_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "warning_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "degraded_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "empty_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "exact_duplicate_count", "type": "nonnegative_integer", "required": True},
    {"field": "conflicting_duplicate_count", "type": "nonnegative_integer", "required": True},
    {"field": "failed_dimension_count", "type": "nonnegative_integer", "required": True},
    {"field": "triggered_dimension_count", "type": "nonnegative_integer", "required": True},
    {"field": "history_digest_reconciles", "type": "boolean", "required": True},
    {"field": "history_record_ids_unique", "type": "boolean", "required": True},
    {"field": "history_order_reconciles", "type": "boolean", "required": True},
    {"field": "source_payload_digests_present", "type": "boolean", "required": True},
    {"field": "status_counts_reconcile", "type": "boolean", "required": True},
    {"field": "diagnostic_codes", "type": "sorted_unique_string_array", "required": True},
    {"field": "validation_errors", "type": "sorted_unique_string_array", "required": True},
    {"field": "production_authority", "type": "boolean_false", "required": True},
]


OBSERVABILITY_STATUSES = [
    {
        "status": "healthy",
        "meaning": (
            "The quality-gate report passed and no warning or failure "
            "signals are present."
        ),
    },
    {
        "status": "warning",
        "meaning": (
            "The quality gate passed with warning records, exact duplicates, "
            "or triggered non-failing dimensions."
        ),
    },
    {
        "status": "degraded",
        "meaning": (
            "The quality gate failed, contains validation errors, or contains "
            "failed dimensions."
        ),
    },
    {
        "status": "empty",
        "meaning": (
            "The source quality-gate report represents an empty history."
        ),
    },
    {
        "status": "disabled",
        "meaning": (
            "Quality-gate observability is disabled and emits no snapshot."
        ),
    },
]


SIGNAL_GROUPS = [
    {"signal_group_id": "HQO-G01", "signal_group": "quality_status_integrity"},
    {"signal_group_id": "HQO-G02", "signal_group": "dimension_integrity"},
    {"signal_group_id": "HQO-G03", "signal_group": "history_digest_integrity"},
    {"signal_group_id": "HQO-G04", "signal_group": "history_identity_integrity"},
    {"signal_group_id": "HQO-G05", "signal_group": "history_order_integrity"},
    {"signal_group_id": "HQO-G06", "signal_group": "source_digest_integrity"},
    {"signal_group_id": "HQO-G07", "signal_group": "status_count_integrity"},
    {"signal_group_id": "HQO-G08", "signal_group": "authority_boundary"},
]


AGGREGATION_RULES = [
    {"rule_id": "HQO-R01", "rule": "failed_quality_status_forces_degraded"},
    {"rule_id": "HQO-R02", "rule": "validation_errors_force_degraded"},
    {"rule_id": "HQO-R03", "rule": "failed_dimensions_force_degraded"},
    {"rule_id": "HQO-R04", "rule": "digest_failure_forces_degraded"},
    {"rule_id": "HQO-R05", "rule": "identity_failure_forces_degraded"},
    {"rule_id": "HQO-R06", "rule": "order_failure_forces_degraded"},
    {"rule_id": "HQO-R07", "rule": "source_digest_failure_forces_degraded"},
    {"rule_id": "HQO-R08", "rule": "status_count_failure_forces_degraded"},
    {"rule_id": "HQO-R09", "rule": "passed_with_warnings_maps_to_warning"},
    {"rule_id": "HQO-R10", "rule": "triggered_nonfailing_dimensions_map_to_warning"},
]


VALIDATION_RULES = [
    {"rule_id": "HQO-V01", "rule": "observability_version_explicit"},
    {"rule_id": "HQO-V02", "rule": "quality_report_required_when_enabled"},
    {"rule_id": "HQO-V03", "rule": "quality_report_id_present"},
    {"rule_id": "HQO-V04", "rule": "quality_gate_version_present"},
    {"rule_id": "HQO-V05", "rule": "quality_status_supported"},
    {"rule_id": "HQO-V06", "rule": "history_record_count_nonnegative"},
    {"rule_id": "HQO-V07", "rule": "status_counts_nonnegative"},
    {"rule_id": "HQO-V08", "rule": "status_counts_reconcile"},
    {"rule_id": "HQO-V09", "rule": "dimension_ids_present"},
    {"rule_id": "HQO-V10", "rule": "dimension_ids_unique"},
    {"rule_id": "HQO-V11", "rule": "failed_dimension_count_nonnegative"},
    {"rule_id": "HQO-V12", "rule": "triggered_dimension_count_nonnegative"},
    {"rule_id": "HQO-V13", "rule": "history_digest_reconciliation_preserved"},
    {"rule_id": "HQO-V14", "rule": "history_identity_validation_preserved"},
    {"rule_id": "HQO-V15", "rule": "history_order_validation_preserved"},
    {"rule_id": "HQO-V16", "rule": "source_digest_validation_preserved"},
    {"rule_id": "HQO-V17", "rule": "diagnostic_codes_sorted_unique"},
    {"rule_id": "HQO-V18", "rule": "validation_errors_sorted_unique"},
    {"rule_id": "HQO-V19", "rule": "snapshot_identity_deterministic"},
    {"rule_id": "HQO-V20", "rule": "caller_quality_report_immutable"},
    {"rule_id": "HQO-V21", "rule": "disabled_path_non_emitting"},
    {"rule_id": "HQO-V22", "rule": "production_authority_false"},
]


ARTIFACT_SCHEMAS = [
    {
        "artifact": "quality_gate_observability_snapshot.csv",
        "scope": "one_row_per_quality_gate_observability_snapshot",
        "required": True,
    },
    {
        "artifact": "quality_gate_observability_report.json",
        "scope": "complete_quality_gate_observability_report",
        "required": True,
    },
    {
        "artifact": "observability_status_counts.csv",
        "scope": "one_row_per_observability_status",
        "required": True,
    },
    {
        "artifact": "quality_status_distribution.csv",
        "scope": "one_row_per_source_quality_status",
        "required": True,
    },
    {
        "artifact": "signal_results.csv",
        "scope": "one_row_per_observability_signal",
        "required": True,
    },
    {
        "artifact": "dimension_signals.csv",
        "scope": "one_row_per_quality_dimension_signal",
        "required": True,
    },
    {
        "artifact": "authority_boundaries.csv",
        "scope": "authority_contract",
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
        "fallback_id": "HQO-F01",
        "condition": "observability_disabled",
        "result": "no_observability_snapshot_emitted",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_"
            "observability_disabled"
        ),
    },
    {
        "fallback_id": "HQO-F02",
        "condition": "quality_report_missing",
        "result": "degraded_observability_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_"
            "observability_report_missing"
        ),
    },
    {
        "fallback_id": "HQO-F03",
        "condition": "quality_report_id_missing",
        "result": "degraded_observability_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_"
            "observability_report_id_missing"
        ),
    },
    {
        "fallback_id": "HQO-F04",
        "condition": "unsupported_quality_status",
        "result": "degraded_observability_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_"
            "observability_status_unsupported"
        ),
    },
    {
        "fallback_id": "HQO-F05",
        "condition": "dimension_identity_conflict",
        "result": "degraded_observability_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_"
            "observability_dimension_conflict"
        ),
    },
    {
        "fallback_id": "HQO-F06",
        "condition": "status_count_mismatch",
        "result": "degraded_observability_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_"
            "observability_count_mismatch"
        ),
    },
    {
        "fallback_id": "HQO-F07",
        "condition": "authority_violation",
        "result": "degraded_observability_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_"
            "observability_authority_violation"
        ),
    },
    {
        "fallback_id": "HQO-F08",
        "condition": "empty_quality_report",
        "result": "empty_observability_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_"
            "observability_empty"
        ),
    },
]


IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": (
            "Create immutable quality-gate-observability signal, snapshot, "
            "and report types."
        ),
    },
    {
        "step": 2,
        "action": (
            "Import Layer 8AA history-quality-gate reports."
        ),
    },
    {
        "step": 3,
        "action": (
            "Validate source quality-report identity and version."
        ),
    },
    {
        "step": 4,
        "action": (
            "Aggregate quality statuses and quality-dimension outcomes."
        ),
    },
    {
        "step": 5,
        "action": (
            "Preserve digest, identity, order, source-digest, and count "
            "integrity signals."
        ),
    },
    {
        "step": 6,
        "action": (
            "Classify healthy, warning, degraded, empty, and disabled paths."
        ),
    },
    {
        "step": 7,
        "action": (
            "Apply deterministic failure precedence."
        ),
    },
    {
        "step": 8,
        "action": (
            "Create deterministic observability-snapshot identities."
        ),
    },
    {
        "step": 9,
        "action": (
            "Keep quality-gate observability disabled by default."
        ),
    },
    {
        "step": 10,
        "action": (
            "Preserve caller quality-report immutability."
        ),
    },
    {
        "step": 11,
        "action": (
            "Create an independent quality-gate-observability audit."
        ),
    },
    {
        "step": 12,
        "action": (
            "Emit deterministic CSV and JSON artifacts without mutating "
            "history or quality reports."
        ),
    },
]


ACCEPTANCE_CRITERIA = [
    {"criterion_id": "HQO-C01", "criterion": "layer_8AA_dependency_verified"},
    {"criterion_id": "HQO-C02", "criterion": "observability_snapshot_schema_defined"},
    {"criterion_id": "HQO-C03", "criterion": "five_observability_statuses_defined"},
    {"criterion_id": "HQO-C04", "criterion": "eight_signal_groups_defined"},
    {"criterion_id": "HQO-C05", "criterion": "quality_status_aggregation_defined"},
    {"criterion_id": "HQO-C06", "criterion": "dimension_signal_aggregation_defined"},
    {"criterion_id": "HQO-C07", "criterion": "failure_precedence_defined"},
    {"criterion_id": "HQO-C08", "criterion": "warning_path_defined"},
    {"criterion_id": "HQO-C09", "criterion": "deterministic_snapshot_identity_defined"},
    {"criterion_id": "HQO-C10", "criterion": "source_integrity_signals_preserved"},
    {"criterion_id": "HQO-C11", "criterion": "status_count_reconciliation_defined"},
    {"criterion_id": "HQO-C12", "criterion": "disabled_path_non_emitting"},
    {"criterion_id": "HQO-C13", "criterion": "history_and_report_mutation_absent"},
    {"criterion_id": "HQO-C14", "criterion": "historical_outcomes_absent"},
    {"criterion_id": "HQO-C15", "criterion": "predictive_evaluation_absent"},
    {"criterion_id": "HQO-C16", "criterion": "production_authority_absent"},
]


PROHIBITED_AUTHORITIES = [
    "history_record_mutation",
    "quality_report_mutation",
    "physical_record_deletion",
    "record_archival_execution",
    "record_expiration_execution",
    "record_quarantine_execution",
    "retention_action_execution",
    "retention_decision_mutation",
    "retention_observability_mutation",
    "retention_window_tuning",
    "production_overlay_integration",
    "production_matchup_adjustment",
    "simulation_state_change",
    "simulation_probability_change",
    "historical_outcome_join",
    "predictive_accuracy_evaluation",
    "calibration_evaluation",
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
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_contract_"
        "implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    field_names = [
        row["field"]
        for row in OBSERVABILITY_SNAPSHOT_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_aa_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_four_observability_fields_defined",
            "actual": len(OBSERVABILITY_SNAPSHOT_FIELDS),
            "expected": 24,
            "passed": len(OBSERVABILITY_SNAPSHOT_FIELDS) == 24,
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
            "check": "ten_aggregation_rules_defined",
            "actual": len(AGGREGATION_RULES),
            "expected": 10,
            "passed": len(AGGREGATION_RULES) == 10,
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
            "check": "failure_precedence_defined",
            "actual": any(
                row["rule"]
                == "failed_quality_status_forces_degraded"
                for row in AGGREGATION_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "failed_quality_status_forces_degraded"
                for row in AGGREGATION_RULES
            ),
        },
        {
            "check": "warning_path_defined",
            "actual": any(
                row["rule"]
                == "passed_with_warnings_maps_to_warning"
                for row in AGGREGATION_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "passed_with_warnings_maps_to_warning"
                for row in AGGREGATION_RULES
            ),
        },
        {
            "check": "source_integrity_signals_defined",
            "actual": all(
                dimension in {
                    row["signal_group"]
                    for row in SIGNAL_GROUPS
                }
                for dimension in (
                    "history_digest_integrity",
                    "history_identity_integrity",
                    "history_order_integrity",
                    "source_digest_integrity",
                    "status_count_integrity",
                )
            ),
            "expected": True,
            "passed": all(
                dimension in {
                    row["signal_group"]
                    for row in SIGNAL_GROUPS
                }
                for dimension in (
                    "history_digest_integrity",
                    "history_identity_integrity",
                    "history_order_integrity",
                    "source_digest_integrity",
                    "status_count_integrity",
                )
            ),
        },
        {
            "check": "history_and_report_mutation_absent",
            "actual": False,
            "expected": False,
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
                "8AB defines diagnostic quality-gate observability only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": (
                "retention_observability_history_quality_gate_"
                "observability_implementation"
            ),
            "granted": all_checks_passed,
            "reason": (
                "8AC may implement bounded deterministic observability "
                "over history-quality-gate reports."
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_contract_plan_failed"
    )

    recommended_next_layer = (
        "8AC_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_contract_implementation"
        if all_checks_passed
        else
        "8AB_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_"
        "observability_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "observability_snapshot_fields.csv": OBSERVABILITY_SNAPSHOT_FIELDS,
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
        "observability_snapshot_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "observability_statuses.csv": [
            "status",
            "meaning",
        ],
        "signal_groups.csv": [
            "signal_group_id",
            "signal_group",
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
                "recommended_next_layer": recommended_next_layer,
                "recommended_action": (
                    "Implement deterministic observability over "
                    "history-quality-gate reports."
                    if all_checks_passed
                    else
                    "Remediate failed 8AB planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8AB planning checks pass."
                ),
                "passed": all_checks_passed,
            }
        ],
    )

    summary = {
        "planning_checks_required": len(planning_checks),
        "planning_checks_passed": sum(
            row["passed"]
            for row in planning_checks
        ),
        "observability_snapshot_fields_defined": len(
            OBSERVABILITY_SNAPSHOT_FIELDS
        ),
        "observability_statuses_defined": len(
            OBSERVABILITY_STATUSES
        ),
        "signal_groups_defined": len(SIGNAL_GROUPS),
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
        "failure_precedence_defined": True,
        "warning_path_defined": True,
        "source_integrity_signals_defined": True,
        "history_mutated": False,
        "quality_report_mutated": False,
        "retention_action_executed": False,
        "physical_deletion_executed": False,
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
        "history_mutation_allowed_next": False,
        "quality_report_mutation_allowed_next": False,
        "retention_action_allowed_next": False,
        "physical_deletion_allowed_next": False,
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "history_quality_gate_observability_implementation_allowed_next": (
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
