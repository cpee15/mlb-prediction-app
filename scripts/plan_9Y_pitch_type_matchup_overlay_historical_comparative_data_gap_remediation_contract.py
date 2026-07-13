#!/usr/bin/env python3
"""
Layer 9Y
Pitch-Type Matchup Overlay Historical Comparative Data-Gap Remediation
Contract Plan

Plans bounded remediation for the evidence gaps classified by Layer 9X.

Planning only.

This layer defines:

- admissible Layer 9X evidence inputs and lineage;
- remediation categories for support, outcome validity, coverage, and
  directional-evidence gaps;
- non-destructive remediation actions and sequencing;
- completion criteria and verification requirements;
- deterministic remediation-record fields, ordering, and artifacts;
- authority boundaries for Layer 9Z.

This layer does not:

- alter source historical records;
- impute outcomes or fabricate observations;
- lower minimum-support thresholds;
- recompute metrics, interpretations, or evidence classifications;
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


LAYER_ID = "9Y"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "data_gap_remediation_contract_plan"
)

PLAN_VERSION = (
    "layer_9Y_historical_comparative_data_gap_"
    "remediation_contract_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9Y_pitch_type_matchup_overlay_"
    "historical_comparative_data_gap_remediation_contract_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9X_pitch_type_matchup_overlay_"
    "historical_comparative_evidence_sufficiency_contract.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9X_historical_comparative_evidence_"
    "sufficiency_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "evidence_sufficiency_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_comparative_data_gap_"
    "remediation_contract_planning"
)


INPUT_RULES = [
    {
        "rule_id": "HCDGR-I01",
        "rule": "evidence_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HCDGR-I02",
        "rule": "evidence_record_id_must_be_unique",
    },
    {
        "rule_id": "HCDGR-I03",
        "rule": "source_interpretation_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HCDGR-I04",
        "rule": "assessment_scope_and_key_must_be_preserved",
    },
    {
        "rule_id": "HCDGR-I05",
        "rule": "evidence_status_must_be_recognized",
    },
    {
        "rule_id": "HCDGR-I06",
        "rule": "source_record_counts_must_be_nonnegative_and_reconciled",
    },
    {
        "rule_id": "HCDGR-I07",
        "rule": "record_scope_gap_counts_must_match_layer_9x_classifications",
    },
    {
        "rule_id": "HCDGR-I08",
        "rule": "aggregate_no_directional_evidence_must_not_be_treated_as_equivalence",
    },
    {
        "rule_id": "HCDGR-I09",
        "rule": "remediation_must_reference_observed_gap_codes_only",
    },
]

GAP_CATEGORIES = [
    {
        "category_id": "HCDGR-G01",
        "gap_category": "insufficient_metric_support",
        "source_condition": "evidence_status=insufficient_metric_support",
        "remediation_goal": (
            "increase valid paired historical support without lowering "
            "the metric minimum-support requirement"
        ),
    },
    {
        "category_id": "HCDGR-G02",
        "gap_category": "invalid_outcome_value",
        "source_condition": (
            "record lineage resolves to source metric status "
            "outcome_value_invalid"
        ),
        "remediation_goal": (
            "identify and repair outcome extraction, typing, mapping, "
            "or compatibility failures at the source boundary"
        ),
    },
    {
        "category_id": "HCDGR-G03",
        "gap_category": "coverage_only",
        "source_condition": "evidence_status=coverage_evidence_only",
        "remediation_goal": (
            "retain descriptive coverage while distinguishing it from "
            "predictive-performance evidence"
        ),
    },
    {
        "category_id": "HCDGR-G04",
        "gap_category": "no_directional_evidence",
        "source_condition": (
            "aggregate evidence_status=no_directional_evidence_available"
        ),
        "remediation_goal": (
            "restore valid supported performance records before any "
            "directional comparison is attempted"
        ),
    },
    {
        "category_id": "HCDGR-G05",
        "gap_category": "source_lineage_invalid",
        "source_condition": (
            "evidence exclusion indicates source or lineage validation failure"
        ),
        "remediation_goal": (
            "repair deterministic identity and lineage propagation without "
            "changing historical values"
        ),
    },
    {
        "category_id": "HCDGR-G06",
        "gap_category": "uncertainty_unavailable",
        "source_condition": (
            "evidence exclusion indicates uncertainty unavailable"
        ),
        "remediation_goal": (
            "defer uncertainty work until valid directional evidence exists"
        ),
    },
]

REMEDIATION_ACTIONS = [
    {
        "action_id": "HCDGR-A01",
        "action_name": "inventory_source_outcome_fields",
        "gap_categories": "invalid_outcome_value",
        "action_type": "diagnostic",
        "mutation_allowed": False,
    },
    {
        "action_id": "HCDGR-A02",
        "action_name": "trace_outcome_value_provenance",
        "gap_categories": "invalid_outcome_value|source_lineage_invalid",
        "action_type": "lineage_audit",
        "mutation_allowed": False,
    },
    {
        "action_id": "HCDGR-A03",
        "action_name": "classify_outcome_failure_mode",
        "gap_categories": "invalid_outcome_value",
        "action_type": "diagnostic",
        "mutation_allowed": False,
    },
    {
        "action_id": "HCDGR-A04",
        "action_name": "identify_additional_valid_historical_pairs",
        "gap_categories": "insufficient_metric_support|no_directional_evidence",
        "action_type": "coverage_discovery",
        "mutation_allowed": False,
    },
    {
        "action_id": "HCDGR-A05",
        "action_name": "audit_pair_exclusion_reasons",
        "gap_categories": "insufficient_metric_support|no_directional_evidence",
        "action_type": "diagnostic",
        "mutation_allowed": False,
    },
    {
        "action_id": "HCDGR-A06",
        "action_name": "verify_prediction_outcome_type_compatibility",
        "gap_categories": "invalid_outcome_value",
        "action_type": "schema_validation",
        "mutation_allowed": False,
    },
    {
        "action_id": "HCDGR-A07",
        "action_name": "preserve_coverage_only_classification",
        "gap_categories": "coverage_only",
        "action_type": "classification_guardrail",
        "mutation_allowed": False,
    },
    {
        "action_id": "HCDGR-A08",
        "action_name": "repair_source_mapping_in_isolated_candidate_artifact",
        "gap_categories": "invalid_outcome_value|source_lineage_invalid",
        "action_type": "candidate_remediation",
        "mutation_allowed": True,
    },
    {
        "action_id": "HCDGR-A09",
        "action_name": "replay_existing_contract_chain_against_candidate_artifact",
        "gap_categories": (
            "invalid_outcome_value|source_lineage_invalid|"
            "insufficient_metric_support|no_directional_evidence"
        ),
        "action_type": "verification",
        "mutation_allowed": False,
    },
    {
        "action_id": "HCDGR-A10",
        "action_name": "compare_before_and_after_gap_counts",
        "gap_categories": (
            "invalid_outcome_value|insufficient_metric_support|"
            "no_directional_evidence"
        ),
        "action_type": "verification",
        "mutation_allowed": False,
    },
    {
        "action_id": "HCDGR-A11",
        "action_name": "retain_minimum_support_thresholds",
        "gap_categories": "insufficient_metric_support",
        "action_type": "threshold_guardrail",
        "mutation_allowed": False,
    },
    {
        "action_id": "HCDGR-A12",
        "action_name": "defer_uncertainty_until_directional_evidence_exists",
        "gap_categories": "uncertainty_unavailable|no_directional_evidence",
        "action_type": "sequencing_guardrail",
        "mutation_allowed": False,
    },
]

REMEDIATION_PRIORITIES = [
    {
        "priority": 1,
        "gap_category": "invalid_outcome_value",
        "reason": (
            "Invalid outcomes directly prevent performance-metric calculation."
        ),
    },
    {
        "priority": 2,
        "gap_category": "source_lineage_invalid",
        "reason": (
            "Invalid lineage prevents trustworthy replay and verification."
        ),
    },
    {
        "priority": 3,
        "gap_category": "insufficient_metric_support",
        "reason": (
            "Valid additional paired observations are required before metrics "
            "can be emitted."
        ),
    },
    {
        "priority": 4,
        "gap_category": "no_directional_evidence",
        "reason": (
            "Directional evidence can only emerge after validity and support "
            "gaps are resolved."
        ),
    },
    {
        "priority": 5,
        "gap_category": "coverage_only",
        "reason": (
            "Coverage is already valid descriptive evidence and should remain "
            "separate from performance evidence."
        ),
    },
    {
        "priority": 6,
        "gap_category": "uncertainty_unavailable",
        "reason": (
            "Uncertainty analysis is premature while no valid directional "
            "performance records exist."
        ),
    },
]

COMPLETION_CRITERIA = [
    {
        "criterion_id": "HCDGR-C01",
        "criterion": (
            "Every remediation record preserves its source evidence identity "
            "and digest."
        ),
    },
    {
        "criterion_id": "HCDGR-C02",
        "criterion": (
            "Every observed record-scope gap maps to at least one authorized "
            "remediation action."
        ),
    },
    {
        "criterion_id": "HCDGR-C03",
        "criterion": (
            "Outcome-invalid records are assigned provenance, failure-mode, "
            "schema, and candidate-repair actions."
        ),
    },
    {
        "criterion_id": "HCDGR-C04",
        "criterion": (
            "Insufficient-support records are assigned pair-discovery and "
            "exclusion-audit actions without threshold relaxation."
        ),
    },
    {
        "criterion_id": "HCDGR-C05",
        "criterion": (
            "Coverage-only records remain explicitly excluded from predictive "
            "quality conclusions."
        ),
    },
    {
        "criterion_id": "HCDGR-C06",
        "criterion": (
            "Candidate remediation occurs only in isolated diagnostic artifacts."
        ),
    },
    {
        "criterion_id": "HCDGR-C07",
        "criterion": (
            "The full existing contract chain is replayed after candidate "
            "remediation."
        ),
    },
    {
        "criterion_id": "HCDGR-C08",
        "criterion": (
            "Before-and-after counts are compared without asserting superiority "
            "or readiness."
        ),
    },
    {
        "criterion_id": "HCDGR-C09",
        "criterion": (
            "Uncertainty and significance work remains blocked until valid "
            "directional evidence exists."
        ),
    },
]

CLAIM_BOUNDARIES = [
    {
        "boundary_id": "HCDGR-B01",
        "rule": (
            "A remediation plan identifies work; it does not prove the gap "
            "can be resolved."
        ),
    },
    {
        "boundary_id": "HCDGR-B02",
        "rule": (
            "Additional support must come from valid historical observations, "
            "not duplicated or synthetic records."
        ),
    },
    {
        "boundary_id": "HCDGR-B03",
        "rule": (
            "Minimum-support thresholds may not be lowered to manufacture "
            "metric eligibility."
        ),
    },
    {
        "boundary_id": "HCDGR-B04",
        "rule": (
            "Invalid outcomes may not be silently coerced, defaulted, or imputed."
        ),
    },
    {
        "boundary_id": "HCDGR-B05",
        "rule": (
            "Coverage-only evidence may not be converted into performance "
            "evidence."
        ),
    },
    {
        "boundary_id": "HCDGR-B06",
        "rule": (
            "A reduction in gap counts does not establish predictive improvement."
        ),
    },
    {
        "boundary_id": "HCDGR-B07",
        "rule": (
            "No directional evidence remains distinct from equivalence or no "
            "effect."
        ),
    },
    {
        "boundary_id": "HCDGR-B08",
        "rule": (
            "Remediation cannot authorize activation, production use, pricing, "
            "market comparison, or betting."
        ),
    },
]

REMEDIATION_RECORD_FIELDS = [
    {"ordinal": 1, "field": "remediation_contract_version"},
    {"ordinal": 2, "field": "remediation_record_id"},
    {"ordinal": 3, "field": "source_evidence_record_id"},
    {"ordinal": 4, "field": "assessment_scope"},
    {"ordinal": 5, "field": "assessment_key"},
    {"ordinal": 6, "field": "metric_name"},
    {"ordinal": 7, "field": "aggregation_name"},
    {"ordinal": 8, "field": "aggregation_key"},
    {"ordinal": 9, "field": "source_evidence_status"},
    {"ordinal": 10, "field": "gap_category"},
    {"ordinal": 11, "field": "gap_priority"},
    {"ordinal": 12, "field": "source_record_count"},
    {"ordinal": 13, "field": "directional_record_count"},
    {"ordinal": 14, "field": "coverage_only_record_count"},
    {"ordinal": 15, "field": "insufficient_support_record_count"},
    {"ordinal": 16, "field": "invalid_input_record_count"},
    {"ordinal": 17, "field": "recommended_action_ids"},
    {"ordinal": 18, "field": "remediation_goal"},
    {"ordinal": 19, "field": "mutation_scope"},
    {"ordinal": 20, "field": "verification_requirements"},
    {"ordinal": 21, "field": "completion_criteria"},
    {"ordinal": 22, "field": "remediation_limitations"},
    {"ordinal": 23, "field": "remediation_exclusion_codes"},
    {"ordinal": 24, "field": "source_evidence_record_digest"},
    {"ordinal": 25, "field": "source_interpretation_digest"},
    {"ordinal": 26, "field": "remediation_identity_digest"},
    {"ordinal": 27, "field": "remediation_record_digest"},
]

EXCLUSION_CODES = [
    {
        "code": "historical_remediation_source_evidence_invalid",
        "category": "source",
    },
    {
        "code": "historical_remediation_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_remediation_status_unrecognized",
        "category": "contract",
    },
    {
        "code": "historical_remediation_gap_category_unmapped",
        "category": "mapping",
    },
    {
        "code": "historical_remediation_insufficient_support",
        "category": "support",
    },
    {
        "code": "historical_remediation_invalid_outcome_value",
        "category": "outcome",
    },
    {
        "code": "historical_remediation_coverage_only",
        "category": "coverage",
    },
    {
        "code": "historical_remediation_no_directional_evidence",
        "category": "direction",
    },
    {
        "code": "historical_remediation_source_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_remediation_uncertainty_deferred",
        "category": "uncertainty",
    },
    {
        "code": "historical_remediation_threshold_relaxation_prohibited",
        "category": "guardrail",
    },
    {
        "code": "historical_remediation_source_mutation_prohibited",
        "category": "guardrail",
    },
]

ORDERING_FIELDS = [
    {"ordinal": 1, "field": "gap_priority"},
    {"ordinal": 2, "field": "gap_category"},
    {"ordinal": 3, "field": "assessment_scope"},
    {"ordinal": 4, "field": "assessment_key"},
    {"ordinal": 5, "field": "metric_name"},
    {"ordinal": 6, "field": "aggregation_name"},
    {"ordinal": 7, "field": "aggregation_key"},
    {"ordinal": 8, "field": "remediation_record_id"},
]

IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "verify_layer_9y_plan_and_layer_9x_predecessor",
    },
    {
        "ordinal": 2,
        "step": "replay_layer_9x_evidence_records",
    },
    {
        "ordinal": 3,
        "step": "validate_evidence_identity_lineage_status_and_counts",
    },
    {
        "ordinal": 4,
        "step": "resolve_record_scope_source_metric_and_interpretation_lineage",
    },
    {
        "ordinal": 5,
        "step": "classify_support_outcome_coverage_direction_and_lineage_gaps",
    },
    {
        "ordinal": 6,
        "step": "assign_priority_and_authorized_remediation_actions",
    },
    {
        "ordinal": 7,
        "step": "attach_mutation_scope_verification_and_completion_requirements",
    },
    {
        "ordinal": 8,
        "step": "preserve_threshold_source_record_and_claim_guardrails",
    },
    {
        "ordinal": 9,
        "step": "derive_remediation_identity_and_record_digests",
    },
    {
        "ordinal": 10,
        "step": "reconcile_gap_counts_to_layer_9x_evidence",
    },
    {
        "ordinal": 11,
        "step": "replay_remediation_planning_under_reversed_input_order",
    },
    {
        "ordinal": 12,
        "step": "write_temporary_diagnostic_artifacts_only",
    },
]

DIAGNOSTIC_ARTIFACTS = [
    {"artifact": "planning_checks.csv"},
    {"artifact": "input_rules.csv"},
    {"artifact": "gap_categories.csv"},
    {"artifact": "remediation_actions.csv"},
    {"artifact": "remediation_priorities.csv"},
    {"artifact": "completion_criteria.csv"},
    {"artifact": "claim_boundaries.csv"},
    {"artifact": "remediation_record_field_contract.csv"},
    {"artifact": "exclusion_code_catalog.csv"},
    {"artifact": "ordering_fields.csv"},
    {"artifact": "implementation_steps.csv"},
    {"artifact": "authority_boundaries.csv"},
    {"artifact": "data_gap_remediation_plan_summary.json"},
    {"artifact": "diagnosis.json"},
]

PROHIBITED_AUTHORITIES = [
    "activation_recommendation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "canonical_probability_authority_change",
    "dataset_split_execution",
    "edge_detection",
    "equivalence_declaration",
    "market_comparison",
    "model_training",
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
        for row in REMEDIATION_RECORD_FIELDS
    ]

    category_names = [
        row["gap_category"]
        for row in GAP_CATEGORIES
    ]

    action_names = [
        row["action_name"]
        for row in REMEDIATION_ACTIONS
    ]

    priority_categories = [
        row["gap_category"]
        for row in REMEDIATION_PRIORITIES
    ]

    checks = [
        {
            "check": "nine_x_predecessor_verified",
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
            "check": "six_gap_categories_defined",
            "actual": len(GAP_CATEGORIES),
            "expected": 6,
            "passed": (
                len(GAP_CATEGORIES) == 6
                and len(set(category_names)) == 6
            ),
        },
        {
            "check": "twelve_remediation_actions_defined",
            "actual": len(REMEDIATION_ACTIONS),
            "expected": 12,
            "passed": (
                len(REMEDIATION_ACTIONS) == 12
                and len(set(action_names)) == 12
            ),
        },
        {
            "check": "six_remediation_priorities_defined",
            "actual": len(REMEDIATION_PRIORITIES),
            "expected": 6,
            "passed": (
                len(REMEDIATION_PRIORITIES) == 6
                and set(priority_categories)
                == set(category_names)
            ),
        },
        {
            "check": "nine_completion_criteria_defined",
            "actual": len(COMPLETION_CRITERIA),
            "expected": 9,
            "passed": len(COMPLETION_CRITERIA) == 9,
        },
        {
            "check": "eight_claim_boundaries_defined",
            "actual": len(CLAIM_BOUNDARIES),
            "expected": 8,
            "passed": len(CLAIM_BOUNDARIES) == 8,
        },
        {
            "check": "twenty_seven_remediation_fields_defined",
            "actual": len(REMEDIATION_RECORD_FIELDS),
            "expected": 27,
            "passed": (
                len(REMEDIATION_RECORD_FIELDS) == 27
                and len(set(field_names)) == 27
            ),
        },
        {
            "check": "twelve_exclusion_codes_defined",
            "actual": len(EXCLUSION_CODES),
            "expected": 12,
            "passed": len(EXCLUSION_CODES) == 12,
        },
        {
            "check": "eight_ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 8,
            "passed": len(ORDERING_FIELDS) == 8,
        },
        {
            "check": "twelve_implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 12,
            "passed": len(IMPLEMENTATION_STEPS) == 12,
        },
        {
            "check": "fourteen_diagnostic_artifacts_defined",
            "actual": len(DIAGNOSTIC_ARTIFACTS),
            "expected": 14,
            "passed": len(DIAGNOSTIC_ARTIFACTS) == 14,
        },
        {
            "check": "invalid_outcome_remediation_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "invalid_outcome_value"
                in set(category_names)
            ),
        },
        {
            "check": "support_remediation_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "insufficient_metric_support"
                in set(category_names)
            ),
        },
        {
            "check": "no_directional_evidence_remediation_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "no_directional_evidence"
                in set(category_names)
            ),
        },
        {
            "check": "threshold_relaxation_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                "may not be lowered"
                in row["rule"]
                for row in CLAIM_BOUNDARIES
            ),
        },
        {
            "check": "silent_outcome_imputation_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                "may not be silently coerced"
                in row["rule"]
                for row in CLAIM_BOUNDARIES
            ),
        },
        {
            "check": "source_mutation_restricted_to_candidate_artifact",
            "actual": True,
            "expected": True,
            "passed": any(
                row["mutation_allowed"]
                for row in REMEDIATION_ACTIONS
            )
            and sum(
                bool(row["mutation_allowed"])
                for row in REMEDIATION_ACTIONS
            )
            == 1,
        },
        {
            "check": "remediation_records_not_materialized",
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
            "check": "metrics_interpretations_and_evidence_not_recomputed",
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
            "gap_categories": GAP_CATEGORIES,
            "remediation_actions": REMEDIATION_ACTIONS,
            "remediation_priorities": REMEDIATION_PRIORITIES,
            "completion_criteria": COMPLETION_CRITERIA,
            "claim_boundaries": CLAIM_BOUNDARIES,
            "remediation_record_fields": REMEDIATION_RECORD_FIELDS,
            "exclusion_codes": EXCLUSION_CODES,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_comparative_"
        "data_gap_remediation_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_comparative_"
        "data_gap_remediation_contract_plan_failed"
    )

    next_layer = (
        "9Z_pitch_type_matchup_overlay_historical_comparative_"
        "data_gap_remediation_contract_implementation"
        if all_checks_passed
        else
        "9Y_pitch_type_matchup_overlay_historical_comparative_"
        "data_gap_remediation_contract_plan_remediation"
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
        OUTPUT_DIR / "gap_categories.csv",
        [
            "category_id",
            "gap_category",
            "source_condition",
            "remediation_goal",
        ],
        GAP_CATEGORIES,
    )

    write_csv(
        OUTPUT_DIR / "remediation_actions.csv",
        [
            "action_id",
            "action_name",
            "gap_categories",
            "action_type",
            "mutation_allowed",
        ],
        REMEDIATION_ACTIONS,
    )

    write_csv(
        OUTPUT_DIR / "remediation_priorities.csv",
        ["priority", "gap_category", "reason"],
        REMEDIATION_PRIORITIES,
    )

    write_csv(
        OUTPUT_DIR / "completion_criteria.csv",
        ["criterion_id", "criterion"],
        COMPLETION_CRITERIA,
    )

    write_csv(
        OUTPUT_DIR / "claim_boundaries.csv",
        ["boundary_id", "rule"],
        CLAIM_BOUNDARIES,
    )

    write_csv(
        OUTPUT_DIR
        / "remediation_record_field_contract.csv",
        ["ordinal", "field"],
        REMEDIATION_RECORD_FIELDS,
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
                    "Layer 9Y is planning-only and grants no source mutation, "
                    "threshold relaxation, uncertainty, significance, "
                    "superiority, equivalence, activation, production, market, "
                    "pricing, or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_comparative_data_gap_"
                    "remediation_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9Z may materialize deterministic remediation records "
                    "and candidate-only repair instructions without changing "
                    "canonical historical data or model behavior."
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
        "gap_categories": len(GAP_CATEGORIES),
        "remediation_actions": len(REMEDIATION_ACTIONS),
        "remediation_priorities": len(REMEDIATION_PRIORITIES),
        "completion_criteria": len(COMPLETION_CRITERIA),
        "claim_boundaries": len(CLAIM_BOUNDARIES),
        "remediation_record_fields": len(
            REMEDIATION_RECORD_FIELDS
        ),
        "exclusion_codes": len(EXCLUSION_CODES),
        "ordering_fields": len(ORDERING_FIELDS),
        "implementation_steps": len(
            IMPLEMENTATION_STEPS
        ),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required": len(checks),
        "plan_digest": plan_digest,
        "remediation_records_materialized": 0,
        "source_records_changed": 0,
        "metrics_recomputed": 0,
        "interpretations_recomputed": 0,
        "evidence_recomputed": 0,
        "uncertainty_estimates_calculated": 0,
        "statistical_significance_tests_calculated": 0,
        "superiority_decisions_emitted": 0,
        "equivalence_decisions_emitted": 0,
        "activation_recommendations_emitted": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "data_gap_remediation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_comparative_data_gap_"
            "remediation_contract_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "recommended_next_layer": next_layer,
        "output_directory": str(
            OUTPUT_DIR.relative_to(ROOT)
        ),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(f"Layer: {LAYER_ID} — {LAYER_NAME}")
    print(f"Plan version: {PLAN_VERSION}")
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
        "Gap categories: "
        f"{len(GAP_CATEGORIES)}"
    )
    print(
        "Remediation actions: "
        f"{len(REMEDIATION_ACTIONS)}"
    )
    print(
        "Remediation priorities: "
        f"{len(REMEDIATION_PRIORITIES)}"
    )
    print(
        "Remediation record fields: "
        f"{len(REMEDIATION_RECORD_FIELDS)}"
    )
    print(f"Plan digest: {plan_digest}")
    print("Remediation records materialized: 0")
    print("Source records changed: 0")
    print("Metrics recomputed: 0")
    print("Interpretations recomputed: 0")
    print("Evidence recomputed: 0")
    print("Uncertainty estimates calculated: 0")
    print(
        "Statistical significance tests calculated: 0"
    )
    print("Superiority decisions emitted: 0")
    print("Equivalence decisions emitted: 0")
    print("Activation recommendations emitted: 0")
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
