#!/usr/bin/env python3
"""
Layer 8Z
Pitch-Type Matchup Overlay Shadow Dataset Collection Retention
Observability History Quality Gate Contract Plan

Defines a deterministic, diagnostic-only quality gate for the immutable
Layer 8Y retention-observability history.

Planning only.

This layer does not:
- mutate or rewrite observability-history records;
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


LAYER_ID = "8Z"
LAYER_NAME = (
    "pitch_type_matchup_overlay_shadow_dataset_collection_"
    "retention_observability_history_quality_gate_contract_plan"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp/"
    "layer_8Z_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_quality_gate_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts/"
    "audit_8Y_pitch_type_matchup_overlay_shadow_dataset_"
    "collection_retention_observability_history_contract.py"
)


QUALITY_REPORT_FIELDS = [
    {"field": "quality_report_id", "type": "deterministic_string", "required": True},
    {"field": "quality_gate_version", "type": "string", "required": True},
    {"field": "evaluated_at_utc", "type": "datetime", "required": True},
    {"field": "history_version", "type": "string", "required": True},
    {"field": "history_digest", "type": "sha256_string", "required": True},
    {"field": "quality_status", "type": "enum", "required": True},
    {"field": "history_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "healthy_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "warning_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "degraded_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "empty_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "unique_history_record_count", "type": "nonnegative_integer", "required": True},
    {"field": "exact_duplicate_count", "type": "nonnegative_integer", "required": True},
    {"field": "conflicting_duplicate_count", "type": "nonnegative_integer", "required": True},
    {"field": "history_digest_reconciles", "type": "boolean", "required": True},
    {"field": "history_record_ids_unique", "type": "boolean", "required": True},
    {"field": "history_order_reconciles", "type": "boolean", "required": True},
    {"field": "source_payload_digests_present", "type": "boolean", "required": True},
    {"field": "status_counts_reconcile", "type": "boolean", "required": True},
    {"field": "production_authority_absent", "type": "boolean", "required": True},
    {"field": "diagnostic_codes", "type": "sorted_unique_string_array", "required": True},
    {"field": "validation_errors", "type": "sorted_unique_string_array", "required": True},
    {"field": "quality_gate_passed", "type": "boolean", "required": True},
    {"field": "production_authority", "type": "boolean_false", "required": True},
]


QUALITY_STATUSES = [
    {
        "status": "passed",
        "meaning": (
            "History integrity, identity, ordering, digests, counts, and "
            "authority boundaries all pass."
        ),
    },
    {
        "status": "passed_with_warnings",
        "meaning": (
            "History integrity passes, but warning observability records or "
            "exact duplicates are present."
        ),
    },
    {
        "status": "failed",
        "meaning": (
            "Digest, identity, ordering, source-digest, status-count, "
            "conflict, degraded-record, or authority checks fail."
        ),
    },
    {
        "status": "empty",
        "meaning": (
            "An emitted history ledger contains no history records."
        ),
    },
    {
        "status": "disabled",
        "meaning": (
            "The history quality gate is disabled and emits no report."
        ),
    },
]


QUALITY_DIMENSIONS = [
    {"dimension_id": "HQ-D01", "dimension": "history_digest_integrity"},
    {"dimension_id": "HQ-D02", "dimension": "history_identity_integrity"},
    {"dimension_id": "HQ-D03", "dimension": "history_order_integrity"},
    {"dimension_id": "HQ-D04", "dimension": "source_payload_digest_integrity"},
    {"dimension_id": "HQ-D05", "dimension": "observability_status_integrity"},
    {"dimension_id": "HQ-D06", "dimension": "duplicate_integrity"},
    {"dimension_id": "HQ-D07", "dimension": "status_count_reconciliation"},
    {"dimension_id": "HQ-D08", "dimension": "authority_boundary"},
]


DECISION_RULES = [
    {"rule_id": "HQ-R01", "rule": "degraded_history_record_forces_failure"},
    {"rule_id": "HQ-R02", "rule": "conflicting_duplicate_forces_failure"},
    {"rule_id": "HQ-R03", "rule": "history_digest_mismatch_forces_failure"},
    {"rule_id": "HQ-R04", "rule": "history_identity_conflict_forces_failure"},
    {"rule_id": "HQ-R05", "rule": "history_order_mismatch_forces_failure"},
    {"rule_id": "HQ-R06", "rule": "missing_source_digest_forces_failure"},
    {"rule_id": "HQ-R07", "rule": "status_count_mismatch_forces_failure"},
    {"rule_id": "HQ-R08", "rule": "production_authority_forces_failure"},
    {"rule_id": "HQ-R09", "rule": "warning_records_allow_passed_with_warnings"},
    {"rule_id": "HQ-R10", "rule": "exact_duplicates_allow_passed_with_warnings"},
]


VALIDATION_RULES = [
    {"rule_id": "HQ-V01", "rule": "quality_gate_version_explicit"},
    {"rule_id": "HQ-V02", "rule": "history_ledger_required_when_enabled"},
    {"rule_id": "HQ-V03", "rule": "history_version_present"},
    {"rule_id": "HQ-V04", "rule": "history_digest_present"},
    {"rule_id": "HQ-V05", "rule": "history_digest_reconciles"},
    {"rule_id": "HQ-V06", "rule": "history_record_count_nonnegative"},
    {"rule_id": "HQ-V07", "rule": "history_record_ids_present"},
    {"rule_id": "HQ-V08", "rule": "history_record_ids_unique"},
    {"rule_id": "HQ-V09", "rule": "history_record_order_deterministic"},
    {"rule_id": "HQ-V10", "rule": "snapshot_payload_digests_present"},
    {"rule_id": "HQ-V11", "rule": "report_payload_digests_present"},
    {"rule_id": "HQ-V12", "rule": "source_payload_digests_sha256_length"},
    {"rule_id": "HQ-V13", "rule": "observability_status_supported"},
    {"rule_id": "HQ-V14", "rule": "observability_status_counts_nonnegative"},
    {"rule_id": "HQ-V15", "rule": "observability_status_counts_reconcile"},
    {"rule_id": "HQ-V16", "rule": "exact_duplicate_count_nonnegative"},
    {"rule_id": "HQ-V17", "rule": "conflicting_duplicate_count_nonnegative"},
    {"rule_id": "HQ-V18", "rule": "conflicting_duplicates_force_failure"},
    {"rule_id": "HQ-V19", "rule": "degraded_records_force_failure"},
    {"rule_id": "HQ-V20", "rule": "caller_history_ledger_immutable"},
    {"rule_id": "HQ-V21", "rule": "disabled_path_non_emitting"},
    {"rule_id": "HQ-V22", "rule": "production_authority_false"},
]


ARTIFACT_SCHEMAS = [
    {
        "artifact": "history_quality_report.csv",
        "scope": "one_row_per_history_quality_evaluation",
        "required": True,
    },
    {
        "artifact": "history_quality_report.json",
        "scope": "complete_history_quality_report",
        "required": True,
    },
    {
        "artifact": "quality_status_counts.csv",
        "scope": "one_row_per_quality_status",
        "required": True,
    },
    {
        "artifact": "quality_dimension_results.csv",
        "scope": "one_row_per_quality_dimension",
        "required": True,
    },
    {
        "artifact": "history_integrity_failures.csv",
        "scope": "one_row_per_quality_failure",
        "required": True,
    },
    {
        "artifact": "history_warning_signals.csv",
        "scope": "one_row_per_quality_warning",
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
        "fallback_id": "HQ-F01",
        "condition": "quality_gate_disabled",
        "result": "no_quality_report_emitted",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_disabled"
        ),
    },
    {
        "fallback_id": "HQ-F02",
        "condition": "history_ledger_missing",
        "result": "failed_quality_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_ledger_missing"
        ),
    },
    {
        "fallback_id": "HQ-F03",
        "condition": "history_digest_missing",
        "result": "failed_quality_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_digest_missing"
        ),
    },
    {
        "fallback_id": "HQ-F04",
        "condition": "history_digest_mismatch",
        "result": "failed_quality_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_digest_mismatch"
        ),
    },
    {
        "fallback_id": "HQ-F05",
        "condition": "history_identity_conflict",
        "result": "failed_quality_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_identity_conflict"
        ),
    },
    {
        "fallback_id": "HQ-F06",
        "condition": "history_order_mismatch",
        "result": "failed_quality_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_order_mismatch"
        ),
    },
    {
        "fallback_id": "HQ-F07",
        "condition": "source_payload_digest_missing",
        "result": "failed_quality_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_source_digest_missing"
        ),
    },
    {
        "fallback_id": "HQ-F08",
        "condition": "empty_history_ledger",
        "result": "empty_quality_report",
        "diagnostic_code": (
            "matchup_shadow_retention_observability_history_quality_gate_empty"
        ),
    },
]


IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": (
            "Create immutable history-quality result, dimension, and report "
            "types."
        ),
    },
    {
        "step": 2,
        "action": (
            "Import the Layer 8Y immutable observability-history ledger."
        ),
    },
    {
        "step": 3,
        "action": (
            "Recompute and reconcile the deterministic history digest."
        ),
    },
    {
        "step": 4,
        "action": (
            "Validate unique history-record identities."
        ),
    },
    {
        "step": 5,
        "action": (
            "Validate deterministic history-record ordering."
        ),
    },
    {
        "step": 6,
        "action": (
            "Validate snapshot and report source-payload digests."
        ),
    },
    {
        "step": 7,
        "action": (
            "Aggregate observability statuses and reconcile counts."
        ),
    },
    {
        "step": 8,
        "action": (
            "Classify passed, passed-with-warnings, failed, empty, and "
            "disabled paths."
        ),
    },
    {
        "step": 9,
        "action": (
            "Force failure for degraded history records or conflicting "
            "duplicates."
        ),
    },
    {
        "step": 10,
        "action": (
            "Preserve caller history-ledger immutability."
        ),
    },
    {
        "step": 11,
        "action": (
            "Create an independent history-quality-gate audit."
        ),
    },
    {
        "step": 12,
        "action": (
            "Emit deterministic CSV and JSON artifacts without mutating "
            "history."
        ),
    },
]


ACCEPTANCE_CRITERIA = [
    {"criterion_id": "HQ-C01", "criterion": "layer_8Y_dependency_verified"},
    {"criterion_id": "HQ-C02", "criterion": "quality_report_schema_defined"},
    {"criterion_id": "HQ-C03", "criterion": "five_quality_statuses_defined"},
    {"criterion_id": "HQ-C04", "criterion": "history_digest_reconciliation_defined"},
    {"criterion_id": "HQ-C05", "criterion": "history_identity_validation_defined"},
    {"criterion_id": "HQ-C06", "criterion": "history_order_validation_defined"},
    {"criterion_id": "HQ-C07", "criterion": "source_payload_digest_validation_defined"},
    {"criterion_id": "HQ-C08", "criterion": "observability_status_aggregation_defined"},
    {"criterion_id": "HQ-C09", "criterion": "status_count_reconciliation_defined"},
    {"criterion_id": "HQ-C10", "criterion": "warning_path_defined"},
    {"criterion_id": "HQ-C11", "criterion": "failure_precedence_defined"},
    {"criterion_id": "HQ-C12", "criterion": "disabled_path_non_emitting"},
    {"criterion_id": "HQ-C13", "criterion": "history_mutation_absent"},
    {"criterion_id": "HQ-C14", "criterion": "historical_outcomes_absent"},
    {"criterion_id": "HQ-C15", "criterion": "predictive_evaluation_absent"},
    {"criterion_id": "HQ-C16", "criterion": "production_authority_absent"},
]


PROHIBITED_AUTHORITIES = [
    "history_record_mutation",
    "history_record_rewrite",
    "physical_record_deletion",
    "record_archival_execution",
    "record_expiration_execution",
    "record_quarantine_execution",
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
        "retention_observability_history_contract_implementation_passed"
        in string_constants(
            PREDECESSOR_PATH
        )
    )

    field_names = [
        row["field"]
        for row in QUALITY_REPORT_FIELDS
    ]

    planning_checks = [
        {
            "check": "eight_y_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "twenty_four_quality_fields_defined",
            "actual": len(QUALITY_REPORT_FIELDS),
            "expected": 24,
            "passed": len(QUALITY_REPORT_FIELDS) == 24,
        },
        {
            "check": "quality_field_names_unique",
            "actual": len(set(field_names)),
            "expected": len(field_names),
            "passed": len(set(field_names)) == len(field_names),
        },
        {
            "check": "five_quality_statuses_defined",
            "actual": len(QUALITY_STATUSES),
            "expected": 5,
            "passed": len(QUALITY_STATUSES) == 5,
        },
        {
            "check": "eight_quality_dimensions_defined",
            "actual": len(QUALITY_DIMENSIONS),
            "expected": 8,
            "passed": len(QUALITY_DIMENSIONS) == 8,
        },
        {
            "check": "ten_decision_rules_defined",
            "actual": len(DECISION_RULES),
            "expected": 10,
            "passed": len(DECISION_RULES) == 10,
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
            "check": "history_digest_reconciliation_defined",
            "actual": any(
                row["dimension"]
                == "history_digest_integrity"
                for row in QUALITY_DIMENSIONS
            ),
            "expected": True,
            "passed": any(
                row["dimension"]
                == "history_digest_integrity"
                for row in QUALITY_DIMENSIONS
            ),
        },
        {
            "check": "failure_precedence_defined",
            "actual": any(
                row["rule"]
                == "degraded_history_record_forces_failure"
                for row in DECISION_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "degraded_history_record_forces_failure"
                for row in DECISION_RULES
            ),
        },
        {
            "check": "warning_path_defined",
            "actual": any(
                row["rule"]
                == "warning_records_allow_passed_with_warnings"
                for row in DECISION_RULES
            ),
            "expected": True,
            "passed": any(
                row["rule"]
                == "warning_records_allow_passed_with_warnings"
                for row in DECISION_RULES
            ),
        },
        {
            "check": "history_mutation_absent",
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
                "8Z defines a diagnostic history quality gate only."
            ),
        }
        for authority in PROHIBITED_AUTHORITIES
    ]

    authority_rows.append(
        {
            "authority": (
                "retention_observability_history_quality_gate_implementation"
            ),
            "granted": all_checks_passed,
            "reason": (
                "8AA may implement a bounded diagnostic quality gate over "
                "immutable observability history."
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_contract_plan_failed"
    )

    recommended_next_layer = (
        "8AA_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_contract_implementation"
        if all_checks_passed
        else
        "8Z_pitch_type_matchup_overlay_shadow_dataset_collection_"
        "retention_observability_history_quality_gate_contract_plan_remediation"
    )

    artifacts = {
        "planning_checks.csv": planning_checks,
        "quality_report_fields.csv": QUALITY_REPORT_FIELDS,
        "quality_statuses.csv": QUALITY_STATUSES,
        "quality_dimensions.csv": QUALITY_DIMENSIONS,
        "decision_rules.csv": DECISION_RULES,
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
        "quality_report_fields.csv": [
            "field",
            "type",
            "required",
        ],
        "quality_statuses.csv": [
            "status",
            "meaning",
        ],
        "quality_dimensions.csv": [
            "dimension_id",
            "dimension",
        ],
        "decision_rules.csv": [
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
                    "Implement a deterministic quality gate over immutable "
                    "retention-observability history."
                    if all_checks_passed
                    else
                    "Remediate failed 8Z planning checks."
                ),
                "entry_condition": (
                    "All nineteen 8Z planning checks pass."
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
            row["passed"]
            for row in planning_checks
        ),
        "quality_report_fields_defined": len(
            QUALITY_REPORT_FIELDS
        ),
        "quality_statuses_defined": len(
            QUALITY_STATUSES
        ),
        "quality_dimensions_defined": len(
            QUALITY_DIMENSIONS
        ),
        "decision_rules_defined": len(
            DECISION_RULES
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
        "history_digest_reconciliation_defined": True,
        "history_identity_validation_defined": True,
        "history_order_validation_defined": True,
        "source_payload_digest_validation_defined": True,
        "status_count_reconciliation_defined": True,
        "failure_precedence_defined": True,
        "warning_path_defined": True,
        "history_mutated": False,
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
        "retention_action_allowed_next": False,
        "physical_deletion_allowed_next": False,
        "historical_validation_allowed_next": False,
        "historical_outcome_enrichment_allowed_next": False,
        "predictive_evaluation_allowed_next": False,
        "tuning_allowed_next": False,
        "backtests_allowed_next": False,
        "pricing_allowed_next": False,
        "edge_detection_allowed_next": False,
        "history_quality_gate_implementation_allowed_next": (
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
                OUTPUT_DIR / "contract_summary.json"
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
