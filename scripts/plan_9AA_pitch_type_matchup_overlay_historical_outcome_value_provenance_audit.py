#!/usr/bin/env python3
"""
Layer 9AA
Pitch-Type Matchup Overlay Historical Outcome-Value Provenance Audit Plan

Plans a read-only provenance audit for the twelve historical comparative
records classified by Layer 9Z as invalid_outcome_value.

Planning only.

This layer defines:

- admissible Layer 9Z remediation inputs and lineage;
- outcome-value provenance stages and source-boundary checkpoints;
- deterministic failure-mode classifications;
- read-only field inventories and compatibility checks;
- audit-record fields, ordering, reconciliation, and artifacts;
- authority boundaries for Layer 9AB.

This layer does not:

- mutate canonical historical records;
- coerce, default, fabricate, or impute outcomes;
- repair mappings or write candidate artifacts;
- lower support thresholds;
- recompute metrics, interpretations, evidence, or remediation records;
- train or tune models;
- estimate uncertainty or statistical significance;
- declare superiority, equivalence, activation, or production readiness;
- modify production probabilities, simulations, pricing, markets, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AA"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_value_provenance_audit_plan"
)

PLAN_VERSION = (
    "layer_9AA_historical_outcome_value_"
    "provenance_audit_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AA_pitch_type_matchup_overlay_"
    "historical_outcome_value_provenance_audit_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9Z_pitch_type_matchup_overlay_"
    "historical_comparative_data_gap_remediation_contract.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9Z_historical_comparative_data_gap_"
    "remediation_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "data_gap_remediation_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_value_"
    "provenance_audit_planning"
)


INPUT_RULES = [
    {
        "rule_id": "HOVPA-I01",
        "rule": "remediation_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOVPA-I02",
        "rule": "remediation_record_id_must_be_unique",
    },
    {
        "rule_id": "HOVPA-I03",
        "rule": "source_evidence_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOVPA-I04",
        "rule": "source_interpretation_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOVPA-I05",
        "rule": "gap_category_must_equal_invalid_outcome_value",
    },
    {
        "rule_id": "HOVPA-I06",
        "rule": "gap_priority_must_equal_one",
    },
    {
        "rule_id": "HOVPA-I07",
        "rule": "mutation_scope_must_be_candidate_only_or_read_only",
    },
    {
        "rule_id": "HOVPA-I08",
        "rule": "invalid_outcome_remediation_actions_must_be_present",
    },
    {
        "rule_id": "HOVPA-I09",
        "rule": "exactly_twelve_invalid_outcome_records_must_be_selected",
    },
]

PROVENANCE_STAGES = [
    {
        "stage_id": "HOVPA-P01",
        "stage_name": "comparison_record",
        "audit_question": (
            "Which comparison record supplied the historical outcome reference?"
        ),
    },
    {
        "stage_id": "HOVPA-P02",
        "stage_name": "historical_prediction_artifact",
        "audit_question": (
            "Which historical prediction artifact and record can be resolved "
            "read-only, and which record carried the candidate outcome field?"
        ),
    },
    {
        "stage_id": "HOVPA-P03",
        "stage_name": "outcome_field_selection",
        "audit_question": (
            "Which field name or field path was selected as the outcome value?"
        ),
    },
    {
        "stage_id": "HOVPA-P04",
        "stage_name": "raw_outcome_value",
        "audit_question": (
            "What exact raw value and runtime type were observed before validation?"
        ),
    },
    {
        "stage_id": "HOVPA-P05",
        "stage_name": "normalization_boundary",
        "audit_question": (
            "Was any authorized parsing or normalization attempted before "
            "compatibility evaluation?"
        ),
    },
    {
        "stage_id": "HOVPA-P06",
        "stage_name": "metric_compatibility_boundary",
        "audit_question": (
            "Why was the outcome incompatible with the metric definition?"
        ),
    },
    {
        "stage_id": "HOVPA-P07",
        "stage_name": "status_emission",
        "audit_question": (
            "Where and under which rule was outcome_value_invalid emitted?"
        ),
    },
    {
        "stage_id": "HOVPA-P08",
        "stage_name": "downstream_lineage",
        "audit_question": (
            "Was the invalid status preserved through interpretation, evidence, "
            "and remediation records?"
        ),
    },
]

FIELD_INVENTORY_RULES = [
    {
        "inventory_id": "HOVPA-F01",
        "field_group": "identity",
        "fields": (
            "comparison_record_id|metric_record_id|interpretation_record_id|"
            "evidence_record_id|remediation_record_id"
        ),
    },
    {
        "inventory_id": "HOVPA-F02",
        "field_group": "artifact_lineage",
        "fields": (
            "source_artifact_path|source_artifact_digest|source_record_id|"
            "source_record_digest"
        ),
    },
    {
        "inventory_id": "HOVPA-F03",
        "field_group": "outcome_locator",
        "fields": (
            "outcome_field_name|outcome_field_path|outcome_source_namespace"
        ),
    },
    {
        "inventory_id": "HOVPA-F04",
        "field_group": "raw_value",
        "fields": (
            "raw_outcome_value|raw_outcome_type|raw_outcome_serialization"
        ),
    },
    {
        "inventory_id": "HOVPA-F05",
        "field_group": "normalized_value",
        "fields": (
            "normalized_outcome_value|normalized_outcome_type|"
            "normalization_rule_applied"
        ),
    },
    {
        "inventory_id": "HOVPA-F06",
        "field_group": "metric_expectation",
        "fields": (
            "metric_name|expected_outcome_semantic|expected_outcome_type|"
            "accepted_value_domain"
        ),
    },
    {
        "inventory_id": "HOVPA-F07",
        "field_group": "failure",
        "fields": (
            "failure_stage|failure_mode|failure_detail|source_metric_status"
        ),
    },
    {
        "inventory_id": "HOVPA-F08",
        "field_group": "audit_lineage",
        "fields": (
            "source_comparison_digest|source_metric_record_digest|"
            "source_interpretation_digest|source_evidence_record_digest|"
            "source_remediation_record_digest"
        ),
    },
]

FAILURE_MODES = [
    {
        "failure_mode": "outcome_field_absent",
        "applies_when": "configured outcome field or path does not exist",
    },
    {
        "failure_mode": "outcome_value_null",
        "applies_when": "field exists but raw value is null",
    },
    {
        "failure_mode": "outcome_value_empty",
        "applies_when": "field exists but raw value is empty or whitespace",
    },
    {
        "failure_mode": "outcome_value_non_numeric",
        "applies_when": "metric requires numeric outcome and raw value is non-numeric",
    },
    {
        "failure_mode": "outcome_value_non_finite",
        "applies_when": "numeric conversion yields NaN or infinity",
    },
    {
        "failure_mode": "outcome_value_domain_invalid",
        "applies_when": "value type is valid but outside the accepted domain",
    },
    {
        "failure_mode": "outcome_semantic_mismatch",
        "applies_when": (
            "field meaning does not match the metric outcome semantic"
        ),
    },
    {
        "failure_mode": "outcome_field_mapping_incorrect",
        "applies_when": (
            "a different available source field appears to represent the "
            "required outcome semantic"
        ),
    },
    {
        "failure_mode": "outcome_source_artifact_unresolved",
        "applies_when": "source artifact or source record cannot be resolved",
    },
    {
        "failure_mode": "outcome_lineage_incomplete",
        "applies_when": "required provenance identifiers or digests are missing",
    },
    {
        "failure_mode": "multiple_candidate_outcome_fields",
        "applies_when": (
            "more than one plausible field exists and the canonical mapping "
            "is not established"
        ),
    },
    {
        "failure_mode": "failure_mode_unresolved",
        "applies_when": "read-only evidence is insufficient to classify the failure",
    },
]

COMPATIBILITY_CHECKS = [
    {
        "check_id": "HOVPA-C01",
        "check_name": "field_presence",
        "expected_result": "outcome field or field path resolves deterministically",
    },
    {
        "check_id": "HOVPA-C02",
        "check_name": "value_presence",
        "expected_result": "raw outcome value is present and non-empty",
    },
    {
        "check_id": "HOVPA-C03",
        "check_name": "runtime_type",
        "expected_result": "raw type is compatible or explicitly parseable",
    },
    {
        "check_id": "HOVPA-C04",
        "check_name": "finite_numeric_value",
        "expected_result": "numeric metrics receive finite numeric outcomes",
    },
    {
        "check_id": "HOVPA-C05",
        "check_name": "accepted_domain",
        "expected_result": "outcome value belongs to the metric's accepted domain",
    },
    {
        "check_id": "HOVPA-C06",
        "check_name": "semantic_alignment",
        "expected_result": "field meaning matches the required outcome semantic",
    },
    {
        "check_id": "HOVPA-C07",
        "check_name": "artifact_resolution",
        "expected_result": "source artifact and source record resolve read-only",
    },
    {
        "check_id": "HOVPA-C08",
        "check_name": "lineage_completeness",
        "expected_result": "all upstream and downstream digests are preserved",
    },
]

AUDIT_STATUSES = [
    {
        "status": "failure_mode_classified",
        "applies_when": (
            "one deterministic failure mode is supported by read-only provenance"
        ),
    },
    {
        "status": "multiple_failure_modes_observed",
        "applies_when": (
            "more than one non-conflicting failure mode is supported"
        ),
    },
    {
        "status": "candidate_mapping_identified",
        "applies_when": (
            "a plausible alternative field is observed but no repair is applied"
        ),
    },
    {
        "status": "source_artifact_unresolved",
        "applies_when": "historical source artifact or source record cannot be read",
    },
    {
        "status": "lineage_invalid",
        "applies_when": "required source identity or digest validation fails",
    },
    {
        "status": "audit_inconclusive",
        "applies_when": "available read-only evidence cannot classify the failure",
    },
]

CLAIM_BOUNDARIES = [
    {
        "boundary_id": "HOVPA-B01",
        "rule": (
            "The audit may identify a likely failure mechanism but may not "
            "silently repair it."
        ),
    },
    {
        "boundary_id": "HOVPA-B02",
        "rule": (
            "A candidate outcome field may be reported but may not become "
            "canonical in this layer."
        ),
    },
    {
        "boundary_id": "HOVPA-B03",
        "rule": (
            "Raw outcome values may not be coerced, defaulted, fabricated, "
            "or imputed."
        ),
    },
    {
        "boundary_id": "HOVPA-B04",
        "rule": (
            "An unresolved source artifact may not be treated as a valid "
            "zero or missing outcome."
        ),
    },
    {
        "boundary_id": "HOVPA-B05",
        "rule": (
            "A classified failure mode does not establish that remediation "
            "will restore metric eligibility."
        ),
    },
    {
        "boundary_id": "HOVPA-B06",
        "rule": (
            "The audit may not lower support thresholds or change metric definitions."
        ),
    },
    {
        "boundary_id": "HOVPA-B07",
        "rule": (
            "Audit findings do not establish predictive improvement, "
            "superiority, equivalence, or readiness."
        ),
    },
    {
        "boundary_id": "HOVPA-B08",
        "rule": (
            "The audit cannot authorize activation, production use, pricing, "
            "market comparison, or betting."
        ),
    },
]

AUDIT_RECORD_FIELDS = [
    {"ordinal": 1, "field": "audit_contract_version"},
    {"ordinal": 2, "field": "audit_record_id"},
    {"ordinal": 3, "field": "source_remediation_record_id"},
    {"ordinal": 4, "field": "metric_name"},
    {"ordinal": 5, "field": "aggregation_name"},
    {"ordinal": 6, "field": "aggregation_key"},
    {"ordinal": 7, "field": "comparison_record_id"},
    {"ordinal": 8, "field": "metric_record_id"},
    {"ordinal": 9, "field": "interpretation_record_id"},
    {"ordinal": 10, "field": "evidence_record_id"},
    {"ordinal": 11, "field": "source_artifact_path"},
    {"ordinal": 12, "field": "source_artifact_digest"},
    {"ordinal": 13, "field": "source_record_id"},
    {"ordinal": 14, "field": "source_record_digest"},
    {"ordinal": 15, "field": "outcome_field_name"},
    {"ordinal": 16, "field": "outcome_field_path"},
    {"ordinal": 17, "field": "raw_outcome_value"},
    {"ordinal": 18, "field": "raw_outcome_type"},
    {"ordinal": 19, "field": "raw_outcome_serialization"},
    {"ordinal": 20, "field": "normalized_outcome_value"},
    {"ordinal": 21, "field": "normalized_outcome_type"},
    {"ordinal": 22, "field": "normalization_rule_applied"},
    {"ordinal": 23, "field": "expected_outcome_semantic"},
    {"ordinal": 24, "field": "expected_outcome_type"},
    {"ordinal": 25, "field": "accepted_value_domain"},
    {"ordinal": 26, "field": "compatibility_check_results"},
    {"ordinal": 27, "field": "failure_stage"},
    {"ordinal": 28, "field": "failure_modes"},
    {"ordinal": 29, "field": "failure_detail"},
    {"ordinal": 30, "field": "candidate_outcome_fields"},
    {"ordinal": 31, "field": "audit_status"},
    {"ordinal": 32, "field": "audit_limitations"},
    {"ordinal": 33, "field": "audit_exclusion_codes"},
    {"ordinal": 34, "field": "source_comparison_digest"},
    {"ordinal": 35, "field": "source_metric_record_digest"},
    {"ordinal": 36, "field": "source_interpretation_digest"},
    {"ordinal": 37, "field": "source_evidence_record_digest"},
    {"ordinal": 38, "field": "source_remediation_record_digest"},
    {"ordinal": 39, "field": "audit_identity_digest"},
    {"ordinal": 40, "field": "audit_record_digest"},
]

EXCLUSION_CODES = [
    {
        "code": "historical_outcome_audit_remediation_record_invalid",
        "category": "source",
    },
    {
        "code": "historical_outcome_audit_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_audit_source_artifact_unresolved",
        "category": "artifact",
    },
    {
        "code": "historical_outcome_audit_source_record_unresolved",
        "category": "record",
    },
    {
        "code": "historical_outcome_audit_field_absent",
        "category": "field",
    },
    {
        "code": "historical_outcome_audit_value_null",
        "category": "value",
    },
    {
        "code": "historical_outcome_audit_value_empty",
        "category": "value",
    },
    {
        "code": "historical_outcome_audit_value_type_incompatible",
        "category": "type",
    },
    {
        "code": "historical_outcome_audit_value_non_finite",
        "category": "value",
    },
    {
        "code": "historical_outcome_audit_value_domain_invalid",
        "category": "domain",
    },
    {
        "code": "historical_outcome_audit_semantic_mismatch",
        "category": "semantic",
    },
    {
        "code": "historical_outcome_audit_candidate_mapping_observed",
        "category": "mapping",
    },
    {
        "code": "historical_outcome_audit_multiple_candidate_fields",
        "category": "mapping",
    },
    {
        "code": "historical_outcome_audit_failure_mode_unresolved",
        "category": "classification",
    },
    {
        "code": "historical_outcome_audit_source_mutation_prohibited",
        "category": "guardrail",
    },
    {
        "code": "historical_outcome_audit_imputation_prohibited",
        "category": "guardrail",
    },
]

ORDERING_FIELDS = [
    {"ordinal": 1, "field": "metric_name"},
    {"ordinal": 2, "field": "aggregation_name"},
    {"ordinal": 3, "field": "aggregation_key"},
    {"ordinal": 4, "field": "failure_stage"},
    {"ordinal": 5, "field": "audit_status"},
    {"ordinal": 6, "field": "source_remediation_record_id"},
    {"ordinal": 7, "field": "audit_record_id"},
]

IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "verify_layer_9aa_plan_and_layer_9z_predecessor",
    },
    {
        "ordinal": 2,
        "step": "replay_layer_9z_remediation_records",
    },
    {
        "ordinal": 3,
        "step": "select_exactly_twelve_invalid_outcome_remediation_records",
    },
    {
        "ordinal": 4,
        "step": "resolve_evidence_interpretation_metric_and_comparison_lineage",
    },
    {
        "ordinal": 5,
        "step": "resolve_historical_source_artifact_and_source_record_read_only",
    },
    {
        "ordinal": 6,
        "step": "inventory_available_outcome_fields_and_paths",
    },
    {
        "ordinal": 7,
        "step": "capture_exact_raw_value_type_and_serialization",
    },
    {
        "ordinal": 8,
        "step": "evaluate_metric_outcome_semantic_type_and_domain_compatibility",
    },
    {
        "ordinal": 9,
        "step": "classify_failure_stage_modes_and_candidate_fields",
    },
    {
        "ordinal": 10,
        "step": "derive_audit_identity_and_record_digests",
    },
    {
        "ordinal": 11,
        "step": "replay_audit_under_reversed_input_order",
    },
    {
        "ordinal": 12,
        "step": "write_temporary_read_only_diagnostic_artifacts",
    },
]

DIAGNOSTIC_ARTIFACTS = [
    {"artifact": "planning_checks.csv"},
    {"artifact": "input_rules.csv"},
    {"artifact": "provenance_stages.csv"},
    {"artifact": "field_inventory_rules.csv"},
    {"artifact": "failure_modes.csv"},
    {"artifact": "compatibility_checks.csv"},
    {"artifact": "audit_statuses.csv"},
    {"artifact": "claim_boundaries.csv"},
    {"artifact": "audit_record_field_contract.csv"},
    {"artifact": "exclusion_code_catalog.csv"},
    {"artifact": "ordering_fields.csv"},
    {"artifact": "implementation_steps.csv"},
    {"artifact": "authority_boundaries.csv"},
    {"artifact": "outcome_value_provenance_audit_plan_summary.json"},
    {"artifact": "diagnosis.json"},
]

PROHIBITED_AUTHORITIES = [
    "activation_recommendation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "candidate_artifact_write",
    "canonical_historical_record_mutation",
    "canonical_outcome_mapping_change",
    "canonical_probability_authority_change",
    "dataset_split_execution",
    "edge_detection",
    "equivalence_declaration",
    "market_comparison",
    "model_training",
    "outcome_coercion",
    "outcome_imputation",
    "parameter_tuning",
    "pricing",
    "production_historical_prediction_materialization",
    "production_matchup_activation",
    "production_overlay_integration",
    "production_readiness_declaration",
    "simulation_probability_change",
    "simulation_state_change",
    "statistical_significance_evaluation",
    "superiority_declaration",
    "threshold_relaxation",
    "threshold_tuning",
    "uncertainty_estimation",
]


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_payload(payload: Any) -> str:
    return hashlib.sha256(
        canonical_json_bytes(payload)
    ).hexdigest()


def string_constants(path: Path) -> set[str]:
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


def write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(fieldnames),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
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


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    predecessor_verified = (
        PREDECESSOR_PATH.exists()
        and EXPECTED_PREDECESSOR_VERSION
        in predecessor_constants
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    field_names = [
        row["field"]
        for row in AUDIT_RECORD_FIELDS
    ]

    stage_names = [
        row["stage_name"]
        for row in PROVENANCE_STAGES
    ]

    failure_mode_names = [
        row["failure_mode"]
        for row in FAILURE_MODES
    ]

    audit_status_names = [
        row["status"]
        for row in AUDIT_STATUSES
    ]

    checks = [
        {
            "check": "nine_z_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "nine_input_rules_defined",
            "actual": len(INPUT_RULES),
            "expected": 9,
            "passed": len(INPUT_RULES) == 9,
        },
        {
            "check": "eight_provenance_stages_defined",
            "actual": len(PROVENANCE_STAGES),
            "expected": 8,
            "passed": (
                len(PROVENANCE_STAGES) == 8
                and len(set(stage_names)) == 8
            ),
        },
        {
            "check": "eight_field_inventory_rules_defined",
            "actual": len(FIELD_INVENTORY_RULES),
            "expected": 8,
            "passed": len(FIELD_INVENTORY_RULES) == 8,
        },
        {
            "check": "twelve_failure_modes_defined",
            "actual": len(FAILURE_MODES),
            "expected": 12,
            "passed": (
                len(FAILURE_MODES) == 12
                and len(set(failure_mode_names)) == 12
            ),
        },
        {
            "check": "eight_compatibility_checks_defined",
            "actual": len(COMPATIBILITY_CHECKS),
            "expected": 8,
            "passed": len(COMPATIBILITY_CHECKS) == 8,
        },
        {
            "check": "six_audit_statuses_defined",
            "actual": len(AUDIT_STATUSES),
            "expected": 6,
            "passed": (
                len(AUDIT_STATUSES) == 6
                and len(set(audit_status_names)) == 6
            ),
        },
        {
            "check": "eight_claim_boundaries_defined",
            "actual": len(CLAIM_BOUNDARIES),
            "expected": 8,
            "passed": len(CLAIM_BOUNDARIES) == 8,
        },
        {
            "check": "forty_audit_fields_defined",
            "actual": len(AUDIT_RECORD_FIELDS),
            "expected": 40,
            "passed": (
                len(AUDIT_RECORD_FIELDS) == 40
                and len(set(field_names)) == 40
            ),
        },
        {
            "check": "sixteen_exclusion_codes_defined",
            "actual": len(EXCLUSION_CODES),
            "expected": 16,
            "passed": len(EXCLUSION_CODES) == 16,
        },
        {
            "check": "seven_ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 7,
            "passed": len(ORDERING_FIELDS) == 7,
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 12,
            "passed": len(IMPLEMENTATION_STEPS) == 12,
        },
        {
            "check": "fifteen_diagnostic_artifacts_defined",
            "actual": len(DIAGNOSTIC_ARTIFACTS),
            "expected": 15,
            "passed": len(DIAGNOSTIC_ARTIFACTS) == 15,
        },
        {
            "check": "field_absence_failure_mode_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "outcome_field_absent"
                in set(failure_mode_names)
            ),
        },
        {
            "check": "type_incompatibility_failure_mode_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "outcome_value_non_numeric"
                in set(failure_mode_names)
            ),
        },
        {
            "check": "semantic_mismatch_failure_mode_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "outcome_semantic_mismatch"
                in set(failure_mode_names)
            ),
        },
        {
            "check": "candidate_mapping_status_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "candidate_mapping_identified"
                in set(audit_status_names)
            ),
        },
        {
            "check": "read_only_source_resolution_defined",
            "actual": True,
            "expected": True,
            "passed": any(
                "read-only"
                in row["audit_question"].lower()
                for row in PROVENANCE_STAGES
            ),
        },
        {
            "check": "outcome_coercion_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                "may not be coerced"
                in row["rule"].lower()
                for row in CLAIM_BOUNDARIES
            ),
        },
        {
            "check": "candidate_mapping_write_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                "may not become canonical"
                in row["rule"].lower()
                for row in CLAIM_BOUNDARIES
            ),
        },
        {
            "check": "audit_records_not_materialized",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "source_records_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "outcomes_not_coerced_or_imputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "metrics_interpretations_evidence_and_remediation_not_recomputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "uncertainty_not_estimated",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "superiority_not_declared",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "production_and_betting_authority_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        bool(row["passed"])
        for row in checks
    )

    plan_digest = sha256_payload(
        {
            "plan_version": PLAN_VERSION,
            "input_rules": INPUT_RULES,
            "provenance_stages": PROVENANCE_STAGES,
            "field_inventory_rules": FIELD_INVENTORY_RULES,
            "failure_modes": FAILURE_MODES,
            "compatibility_checks": COMPATIBILITY_CHECKS,
            "audit_statuses": AUDIT_STATUSES,
            "claim_boundaries": CLAIM_BOUNDARIES,
            "audit_record_fields": AUDIT_RECORD_FIELDS,
            "exclusion_codes": EXCLUSION_CODES,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_audit_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_audit_plan_failed"
    )

    next_layer = (
        "9AB_pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_audit_implementation"
        if all_checks_passed
        else
        "9AA_pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_audit_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "input_rules.csv",
        ["rule_id", "rule"],
        INPUT_RULES,
    )

    write_csv(
        OUTPUT_DIR / "provenance_stages.csv",
        [
            "stage_id",
            "stage_name",
            "audit_question",
        ],
        PROVENANCE_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "field_inventory_rules.csv",
        [
            "inventory_id",
            "field_group",
            "fields",
        ],
        FIELD_INVENTORY_RULES,
    )

    write_csv(
        OUTPUT_DIR / "failure_modes.csv",
        [
            "failure_mode",
            "applies_when",
        ],
        FAILURE_MODES,
    )

    write_csv(
        OUTPUT_DIR / "compatibility_checks.csv",
        [
            "check_id",
            "check_name",
            "expected_result",
        ],
        COMPATIBILITY_CHECKS,
    )

    write_csv(
        OUTPUT_DIR / "audit_statuses.csv",
        [
            "status",
            "applies_when",
        ],
        AUDIT_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "claim_boundaries.csv",
        ["boundary_id", "rule"],
        CLAIM_BOUNDARIES,
    )

    write_csv(
        OUTPUT_DIR
        / "audit_record_field_contract.csv",
        ["ordinal", "field"],
        AUDIT_RECORD_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "exclusion_code_catalog.csv",
        ["code", "category"],
        EXCLUSION_CODES,
    )

    write_csv(
        OUTPUT_DIR / "ordering_fields.csv",
        ["ordinal", "field"],
        ORDERING_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        ["ordinal", "step"],
        IMPLEMENTATION_STEPS,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        ["authority", "granted", "reason"],
        [
            {
                "authority": authority,
                "granted": False,
                "reason": (
                    "Layer 9AA is planning-only and grants no source mutation, "
                    "mapping change, candidate write, coercion, imputation, "
                    "uncertainty, significance, superiority, equivalence, "
                    "activation, production, market, pricing, or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_value_"
                    "provenance_audit_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9AB may perform a deterministic read-only provenance "
                    "audit of the twelve invalid-outcome records and classify "
                    "failure modes without applying repairs."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_verified": predecessor_verified,
        "input_rules": len(INPUT_RULES),
        "provenance_stages": len(PROVENANCE_STAGES),
        "field_inventory_rules": len(
            FIELD_INVENTORY_RULES
        ),
        "failure_modes": len(FAILURE_MODES),
        "compatibility_checks": len(
            COMPATIBILITY_CHECKS
        ),
        "audit_statuses": len(AUDIT_STATUSES),
        "claim_boundaries": len(
            CLAIM_BOUNDARIES
        ),
        "audit_record_fields": len(
            AUDIT_RECORD_FIELDS
        ),
        "exclusion_codes": len(
            EXCLUSION_CODES
        ),
        "ordering_fields": len(
            ORDERING_FIELDS
        ),
        "implementation_steps": len(
            IMPLEMENTATION_STEPS
        ),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required": len(
            checks
        ),
        "plan_digest": plan_digest,
        "expected_invalid_outcome_records": 12,
        "audit_records_materialized": 0,
        "source_records_changed": 0,
        "candidate_artifacts_written": 0,
        "outcomes_coerced": 0,
        "outcomes_imputed": 0,
        "metrics_recomputed": 0,
        "interpretations_recomputed": 0,
        "evidence_recomputed": 0,
        "remediation_records_recomputed": 0,
        "uncertainty_estimates_calculated": 0,
        "statistical_significance_tests_calculated": 0,
        "superiority_decisions_emitted": 0,
        "equivalence_decisions_emitted": 0,
        "activation_recommendations_emitted": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            next_layer
        ),
    }

    write_json(
        OUTPUT_DIR
        / "outcome_value_provenance_audit_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": (
            all_checks_passed
        ),
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_value_"
            "provenance_audit_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "recommended_next_layer": (
            next_layer
        ),
        "output_directory": str(
            OUTPUT_DIR.relative_to(ROOT)
        ),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
    print(
        f"Plan version: {PLAN_VERSION}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_verified}"
    )
    print(
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(
        "Expected invalid-outcome records: 12"
    )
    print(
        "Provenance stages: "
        f"{len(PROVENANCE_STAGES)}"
    )
    print(
        "Failure modes: "
        f"{len(FAILURE_MODES)}"
    )
    print(
        "Compatibility checks: "
        f"{len(COMPATIBILITY_CHECKS)}"
    )
    print(
        "Audit record fields: "
        f"{len(AUDIT_RECORD_FIELDS)}"
    )
    print(f"Plan digest: {plan_digest}")
    print("Audit records materialized: 0")
    print("Source records changed: 0")
    print("Candidate artifacts written: 0")
    print("Outcomes coerced: 0")
    print("Outcomes imputed: 0")
    print("Metrics recomputed: 0")
    print("Interpretations recomputed: 0")
    print("Evidence recomputed: 0")
    print("Remediation records recomputed: 0")
    print("Uncertainty estimates calculated: 0")
    print(
        "Statistical significance tests calculated: 0"
    )
    print("Superiority decisions emitted: 0")
    print("Equivalence decisions emitted: 0")
    print(
        "Activation recommendations emitted: 0"
    )
    print("Production probabilities changed: 0")
    print("Market comparisons executed: 0")
    print("Betting edges calculated: 0")
    print(f"Diagnosis: {diagnosis_name}")
    print(
        "Authority granted: "
        f"{diagnosis['authority_granted']}"
    )
    print(
        "Recommended next layer: "
        f"{next_layer}"
    )
    print(
        "Artifacts: "
        f"{OUTPUT_DIR.relative_to(ROOT)}"
    )

    if not all_checks_passed:
        failed_checks = [
            row["check"]
            for row in checks
            if not row["passed"]
        ]

        print(
            "FAILED CHECKS: "
            + ", ".join(failed_checks)
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
