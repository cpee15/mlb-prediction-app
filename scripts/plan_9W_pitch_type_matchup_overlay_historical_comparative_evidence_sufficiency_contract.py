#!/usr/bin/env python3
"""
Layer 9W
Pitch-Type Matchup Overlay Historical Comparative Evidence Sufficiency Contract Plan

Plans bounded evidence-sufficiency classification for Layer 9V diagnostic
interpretation records.

Planning only.

This layer defines:

- admissible interpretation-record inputs and lineage;
- evidence dimensions for support, validity, coverage, direction, and scope;
- deterministic sufficiency statuses and reason codes;
- rules preventing absence of evidence from being treated as evidence of
  equivalence or no effect;
- deterministic evidence-record fields, ordering, reconciliation, and artifacts;
- authority boundaries for Layer 9X.

This layer does not:

- recompute metrics or interpretations;
- estimate uncertainty or statistical significance;
- declare superiority, equivalence, activation, or production readiness;
- select thresholds, train models, or execute dataset splits;
- modify production probabilities, simulations, pricing, markets, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9W"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "evidence_sufficiency_contract_plan"
)

PLAN_VERSION = (
    "layer_9W_historical_comparative_evidence_"
    "sufficiency_contract_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9W_pitch_type_matchup_overlay_"
    "historical_comparative_evidence_sufficiency_contract_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9V_pitch_type_matchup_overlay_"
    "historical_comparative_metric_result_interpretation_contract.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9V_historical_comparative_metric_result_"
    "interpretation_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "metric_result_interpretation_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_comparative_evidence_"
    "sufficiency_contract_planning"
)


INPUT_RULES = [
    {
        "rule_id": "HCEVS-I01",
        "rule": "interpretation_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HCEVS-I02",
        "rule": "interpretation_record_id_must_be_unique",
    },
    {
        "rule_id": "HCEVS-I03",
        "rule": "source_metric_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HCEVS-I04",
        "rule": "source_comparison_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HCEVS-I05",
        "rule": "interpretation_status_must_be_recognized",
    },
    {
        "rule_id": "HCEVS-I06",
        "rule": "candidate_eligible_and_excluded_counts_must_reconcile",
    },
    {
        "rule_id": "HCEVS-I07",
        "rule": "interpretation_eligible_must_match_interpretation_status",
    },
    {
        "rule_id": "HCEVS-I08",
        "rule": "directional_records_must_include_finite_paired_metric_values",
    },
    {
        "rule_id": "HCEVS-I09",
        "rule": "coverage_suppressed_and_invalid_records_must_not_be_directional",
    },
]

EVIDENCE_DIMENSIONS = [
    {
        "dimension_id": "HCEVS-D01",
        "dimension_name": "source_validity",
        "question": "Are interpretation and metric lineage valid?",
    },
    {
        "dimension_id": "HCEVS-D02",
        "dimension_name": "metric_support",
        "question": "Was the source metric minimum-support rule satisfied?",
    },
    {
        "dimension_id": "HCEVS-D03",
        "dimension_name": "directional_observability",
        "question": "Was an authorized directional point estimate observed?",
    },
    {
        "dimension_id": "HCEVS-D04",
        "dimension_name": "coverage_availability",
        "question": "Is the record limited to descriptive coverage?",
    },
    {
        "dimension_id": "HCEVS-D05",
        "dimension_name": "aggregation_scope",
        "question": "Is the evidence restricted to its exact aggregation key?",
    },
    {
        "dimension_id": "HCEVS-D06",
        "dimension_name": "cross_record_consistency",
        "question": "Do comparable directional records conflict or align?",
    },
    {
        "dimension_id": "HCEVS-D07",
        "dimension_name": "uncertainty_availability",
        "question": "Has uncertainty been estimated under an authorized contract?",
    },
]

SUFFICIENCY_STATUSES = [
    {
        "status": "directional_evidence_available",
        "applies_when": (
            "valid interpretation record with interpretation_eligible=true "
            "and authorized directional status"
        ),
        "sufficient_for": "bounded_directional_review_only",
    },
    {
        "status": "insufficient_metric_support",
        "applies_when": "interpretation_status=insufficient_support",
        "sufficient_for": "coverage_and_support_diagnosis_only",
    },
    {
        "status": "coverage_evidence_only",
        "applies_when": "interpretation_status=coverage_only",
        "sufficient_for": "availability_description_only",
    },
    {
        "status": "invalid_source_evidence",
        "applies_when": "interpretation_status=input_invalid",
        "sufficient_for": "source_failure_diagnosis_only",
    },
    {
        "status": "directional_conflict_present",
        "applies_when": (
            "comparable directional records contain opposing observed directions"
        ),
        "sufficient_for": "conflict_description_only",
    },
    {
        "status": "directionally_consistent_observations",
        "applies_when": (
            "multiple comparable directional records share one observed direction"
        ),
        "sufficient_for": "consistency_description_only",
    },
    {
        "status": "no_directional_evidence_available",
        "applies_when": (
            "no eligible directional interpretation exists in the assessed scope"
        ),
        "sufficient_for": "absence_of_directional_evidence_statement_only",
    },
    {
        "status": "not_assessable",
        "applies_when": "source contract validation fails",
        "sufficient_for": "contract_failure_diagnosis_only",
    },
]

ASSESSMENT_SCOPES = [
    {
        "scope_id": "HCEVS-S01",
        "scope_name": "record",
        "grouping_fields": "interpretation_record_id",
        "required": True,
    },
    {
        "scope_id": "HCEVS-S02",
        "scope_name": "metric_aggregation",
        "grouping_fields": "metric_name|aggregation_name|aggregation_key",
        "required": True,
    },
    {
        "scope_id": "HCEVS-S03",
        "scope_name": "metric_overall",
        "grouping_fields": "metric_name",
        "required": True,
    },
    {
        "scope_id": "HCEVS-S04",
        "scope_name": "aggregation_overall",
        "grouping_fields": "aggregation_name|aggregation_key",
        "required": True,
    },
    {
        "scope_id": "HCEVS-S05",
        "scope_name": "global",
        "grouping_fields": "",
        "required": True,
    },
]

CONSISTENCY_RULES = [
    {
        "rule_id": "HCEVS-C01",
        "rule": (
            "Only interpretation_eligible directional records participate "
            "in direction-consistency assessment."
        ),
    },
    {
        "rule_id": "HCEVS-C02",
        "rule": (
            "Coverage-only, insufficient-support, and invalid records remain "
            "counted but cannot contribute directional evidence."
        ),
    },
    {
        "rule_id": "HCEVS-C03",
        "rule": (
            "Opposing directions must be labeled conflict and must not be "
            "collapsed into a neutral conclusion."
        ),
    },
    {
        "rule_id": "HCEVS-C04",
        "rule": (
            "Zero eligible directional records means no directional evidence "
            "available, not equivalence and not no effect."
        ),
    },
    {
        "rule_id": "HCEVS-C05",
        "rule": (
            "Directionally equal observed values remain point observations and "
            "do not establish statistical equivalence."
        ),
    },
    {
        "rule_id": "HCEVS-C06",
        "rule": (
            "Evidence conclusions cannot generalize beyond the exact assessment "
            "scope and aggregation keys."
        ),
    },
    {
        "rule_id": "HCEVS-C07",
        "rule": (
            "Cross-metric consistency cannot create a composite score or "
            "superiority decision."
        ),
    },
]

CLAIM_BOUNDARIES = [
    {
        "boundary_id": "HCEVS-B01",
        "rule": (
            "Sufficient means sufficient only for the explicitly named bounded "
            "diagnostic use."
        ),
    },
    {
        "boundary_id": "HCEVS-B02",
        "rule": (
            "Directional evidence available does not mean statistically "
            "significant evidence."
        ),
    },
    {
        "boundary_id": "HCEVS-B03",
        "rule": (
            "Directionally consistent observations do not establish superiority."
        ),
    },
    {
        "boundary_id": "HCEVS-B04",
        "rule": (
            "No directional evidence available does not establish equivalence, "
            "no effect, or model parity."
        ),
    },
    {
        "boundary_id": "HCEVS-B05",
        "rule": (
            "Insufficient support cannot be interpreted as unfavorable or "
            "favorable model evidence."
        ),
    },
    {
        "boundary_id": "HCEVS-B06",
        "rule": (
            "Coverage evidence cannot be interpreted as predictive-quality evidence."
        ),
    },
    {
        "boundary_id": "HCEVS-B07",
        "rule": (
            "Evidence sufficiency cannot authorize threshold selection or tuning."
        ),
    },
    {
        "boundary_id": "HCEVS-B08",
        "rule": (
            "Evidence sufficiency cannot authorize activation, production use, "
            "pricing, market comparison, or betting."
        ),
    },
]

EVIDENCE_RECORD_FIELDS = [
    {"ordinal": 1, "field": "evidence_contract_version"},
    {"ordinal": 2, "field": "evidence_record_id"},
    {"ordinal": 3, "field": "assessment_scope"},
    {"ordinal": 4, "field": "assessment_key"},
    {"ordinal": 5, "field": "metric_name"},
    {"ordinal": 6, "field": "aggregation_name"},
    {"ordinal": 7, "field": "aggregation_key"},
    {"ordinal": 8, "field": "source_interpretation_record_count"},
    {"ordinal": 9, "field": "directional_record_count"},
    {"ordinal": 10, "field": "coverage_only_record_count"},
    {"ordinal": 11, "field": "insufficient_support_record_count"},
    {"ordinal": 12, "field": "invalid_input_record_count"},
    {"ordinal": 13, "field": "lower_augmented_loss_count"},
    {"ordinal": 14, "field": "higher_augmented_loss_count"},
    {"ordinal": 15, "field": "higher_augmented_score_count"},
    {"ordinal": 16, "field": "lower_augmented_score_count"},
    {"ordinal": 17, "field": "equal_observed_value_count"},
    {"ordinal": 18, "field": "direction_conflict_present"},
    {"ordinal": 19, "field": "evidence_status"},
    {"ordinal": 20, "field": "evidence_sufficient_for"},
    {"ordinal": 21, "field": "evidence_observation"},
    {"ordinal": 22, "field": "evidence_limitations"},
    {"ordinal": 23, "field": "evidence_exclusion_codes"},
    {"ordinal": 24, "field": "source_interpretation_digest"},
    {"ordinal": 25, "field": "evidence_identity_digest"},
    {"ordinal": 26, "field": "evidence_record_digest"},
]

EXCLUSION_CODES = [
    {
        "code": "historical_evidence_source_interpretation_invalid",
        "category": "source",
    },
    {
        "code": "historical_evidence_interpretation_lineage_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_evidence_interpretation_status_unrecognized",
        "category": "contract",
    },
    {
        "code": "historical_evidence_insufficient_metric_support",
        "category": "support",
    },
    {
        "code": "historical_evidence_coverage_only",
        "category": "coverage",
    },
    {
        "code": "historical_evidence_invalid_source_input",
        "category": "source",
    },
    {
        "code": "historical_evidence_no_directional_records",
        "category": "direction",
    },
    {
        "code": "historical_evidence_directional_conflict",
        "category": "consistency",
    },
    {
        "code": "historical_evidence_scope_invalid",
        "category": "scope",
    },
    {
        "code": "historical_evidence_duplicate_interpretation_identity",
        "category": "cardinality",
    },
    {
        "code": "historical_evidence_uncertainty_unavailable",
        "category": "uncertainty",
    },
]

ORDERING_FIELDS = [
    {"ordinal": 1, "field": "assessment_scope"},
    {"ordinal": 2, "field": "assessment_key"},
    {"ordinal": 3, "field": "metric_name"},
    {"ordinal": 4, "field": "aggregation_name"},
    {"ordinal": 5, "field": "aggregation_key"},
    {"ordinal": 6, "field": "evidence_status"},
    {"ordinal": 7, "field": "evidence_record_id"},
]

IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "verify_layer_9w_plan_and_layer_9v_predecessor",
    },
    {
        "ordinal": 2,
        "step": "replay_layer_9v_interpretation_records",
    },
    {
        "ordinal": 3,
        "step": "validate_interpretation_identity_lineage_and_status",
    },
    {
        "ordinal": 4,
        "step": "construct_canonical_evidence_assessment_scopes",
    },
    {
        "ordinal": 5,
        "step": "count_directional_coverage_support_and_invalid_records",
    },
    {
        "ordinal": 6,
        "step": "detect_directional_consistency_or_conflict",
    },
    {
        "ordinal": 7,
        "step": "classify_bounded_evidence_sufficiency_status",
    },
    {
        "ordinal": 8,
        "step": "attach_scope_support_uncertainty_and_claim_limitations",
    },
    {
        "ordinal": 9,
        "step": "derive_evidence_identity_and_record_digests",
    },
    {
        "ordinal": 10,
        "step": "reconcile_source_interpretation_counts",
    },
    {
        "ordinal": 11,
        "step": "replay_evidence_assessment_under_reversed_input_order",
    },
    {
        "ordinal": 12,
        "step": "write_temporary_diagnostic_artifacts_only",
    },
]

DIAGNOSTIC_ARTIFACTS = [
    {"artifact": "planning_checks.csv"},
    {"artifact": "input_rules.csv"},
    {"artifact": "evidence_dimensions.csv"},
    {"artifact": "sufficiency_statuses.csv"},
    {"artifact": "assessment_scopes.csv"},
    {"artifact": "consistency_rules.csv"},
    {"artifact": "claim_boundaries.csv"},
    {"artifact": "evidence_record_field_contract.csv"},
    {"artifact": "exclusion_code_catalog.csv"},
    {"artifact": "ordering_fields.csv"},
    {"artifact": "implementation_steps.csv"},
    {"artifact": "authority_boundaries.csv"},
    {"artifact": "evidence_sufficiency_plan_summary.json"},
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
        for row in EVIDENCE_RECORD_FIELDS
    ]

    status_names = [
        row["status"]
        for row in SUFFICIENCY_STATUSES
    ]

    checks = [
        {
            "check": "nine_v_predecessor_verified",
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
            "check": "seven_evidence_dimensions_defined",
            "actual": len(EVIDENCE_DIMENSIONS),
            "expected": 7,
            "passed": len(EVIDENCE_DIMENSIONS) == 7,
        },
        {
            "check": "eight_sufficiency_statuses_defined",
            "actual": len(SUFFICIENCY_STATUSES),
            "expected": 8,
            "passed": (
                len(SUFFICIENCY_STATUSES) == 8
                and len(set(status_names)) == 8
            ),
        },
        {
            "check": "five_assessment_scopes_defined",
            "actual": len(ASSESSMENT_SCOPES),
            "expected": 5,
            "passed": len(ASSESSMENT_SCOPES) == 5,
        },
        {
            "check": "seven_consistency_rules_defined",
            "actual": len(CONSISTENCY_RULES),
            "expected": 7,
            "passed": len(CONSISTENCY_RULES) == 7,
        },
        {
            "check": "eight_claim_boundaries_defined",
            "actual": len(CLAIM_BOUNDARIES),
            "expected": 8,
            "passed": len(CLAIM_BOUNDARIES) == 8,
        },
        {
            "check": "twenty_six_evidence_fields_defined",
            "actual": len(EVIDENCE_RECORD_FIELDS),
            "expected": 26,
            "passed": (
                len(EVIDENCE_RECORD_FIELDS) == 26
                and len(set(field_names)) == 26
            ),
        },
        {
            "check": "eleven_exclusion_codes_defined",
            "actual": len(EXCLUSION_CODES),
            "expected": 11,
            "passed": len(EXCLUSION_CODES) == 11,
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
            "check": "fourteen_diagnostic_artifacts_defined",
            "actual": len(DIAGNOSTIC_ARTIFACTS),
            "expected": 14,
            "passed": len(DIAGNOSTIC_ARTIFACTS) == 14,
        },
        {
            "check": "no_directional_evidence_status_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "no_directional_evidence_available"
                in set(status_names)
            ),
        },
        {
            "check": "insufficient_support_status_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "insufficient_metric_support"
                in set(status_names)
            ),
        },
        {
            "check": "coverage_only_status_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "coverage_evidence_only"
                in set(status_names)
            ),
        },
        {
            "check": "directional_conflict_status_defined",
            "actual": True,
            "expected": True,
            "passed": (
                "directional_conflict_present"
                in set(status_names)
            ),
        },
        {
            "check": "absence_not_equivalence_rule_defined",
            "actual": True,
            "expected": True,
            "passed": any(
                "not equivalence"
                in row["rule"].lower()
                for row in CONSISTENCY_RULES
            ),
        },
        {
            "check": "uncertainty_dimension_defined",
            "actual": True,
            "expected": True,
            "passed": any(
                row["dimension_name"]
                == "uncertainty_availability"
                for row in EVIDENCE_DIMENSIONS
            ),
        },
        {
            "check": "evidence_records_not_materialized",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "metrics_and_interpretations_not_recomputed",
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
            "evidence_dimensions": EVIDENCE_DIMENSIONS,
            "sufficiency_statuses": SUFFICIENCY_STATUSES,
            "assessment_scopes": ASSESSMENT_SCOPES,
            "consistency_rules": CONSISTENCY_RULES,
            "claim_boundaries": CLAIM_BOUNDARIES,
            "evidence_record_fields": EVIDENCE_RECORD_FIELDS,
            "exclusion_codes": EXCLUSION_CODES,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_comparative_"
        "evidence_sufficiency_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_comparative_"
        "evidence_sufficiency_contract_plan_failed"
    )

    next_layer = (
        "9X_pitch_type_matchup_overlay_historical_comparative_"
        "evidence_sufficiency_contract_implementation"
        if all_checks_passed
        else
        "9W_pitch_type_matchup_overlay_historical_comparative_"
        "evidence_sufficiency_contract_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "input_rules.csv",
        ["rule_id", "rule"],
        INPUT_RULES,
    )

    write_csv(
        OUTPUT_DIR / "evidence_dimensions.csv",
        [
            "dimension_id",
            "dimension_name",
            "question",
        ],
        EVIDENCE_DIMENSIONS,
    )

    write_csv(
        OUTPUT_DIR / "sufficiency_statuses.csv",
        [
            "status",
            "applies_when",
            "sufficient_for",
        ],
        SUFFICIENCY_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "assessment_scopes.csv",
        [
            "scope_id",
            "scope_name",
            "grouping_fields",
            "required",
        ],
        ASSESSMENT_SCOPES,
    )

    write_csv(
        OUTPUT_DIR / "consistency_rules.csv",
        ["rule_id", "rule"],
        CONSISTENCY_RULES,
    )

    write_csv(
        OUTPUT_DIR / "claim_boundaries.csv",
        ["boundary_id", "rule"],
        CLAIM_BOUNDARIES,
    )

    write_csv(
        OUTPUT_DIR
        / "evidence_record_field_contract.csv",
        ["ordinal", "field"],
        EVIDENCE_RECORD_FIELDS,
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
        [
            "authority",
            "granted",
            "reason",
        ],
        [
            {
                "authority": authority,
                "granted": False,
                "reason": (
                    "Layer 9W is planning-only and grants no uncertainty, "
                    "significance, superiority, equivalence, activation, "
                    "production, market, pricing, or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_comparative_evidence_"
                    "sufficiency_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9X may classify bounded diagnostic evidence "
                    "availability and insufficiency without making a model "
                    "selection, superiority, or activation decision."
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
        "evidence_dimensions": len(
            EVIDENCE_DIMENSIONS
        ),
        "sufficiency_statuses": len(
            SUFFICIENCY_STATUSES
        ),
        "assessment_scopes": len(
            ASSESSMENT_SCOPES
        ),
        "consistency_rules": len(
            CONSISTENCY_RULES
        ),
        "claim_boundaries": len(
            CLAIM_BOUNDARIES
        ),
        "evidence_record_fields": len(
            EVIDENCE_RECORD_FIELDS
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
        "evidence_records_materialized": 0,
        "metrics_recomputed": 0,
        "interpretations_recomputed": 0,
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
        / "evidence_sufficiency_plan_summary.json",
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
            "historical_comparative_evidence_"
            "sufficiency_contract_implementation"
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
        "Evidence dimensions: "
        f"{len(EVIDENCE_DIMENSIONS)}"
    )
    print(
        "Sufficiency statuses: "
        f"{len(SUFFICIENCY_STATUSES)}"
    )
    print(
        "Assessment scopes: "
        f"{len(ASSESSMENT_SCOPES)}"
    )
    print(
        "Evidence record fields: "
        f"{len(EVIDENCE_RECORD_FIELDS)}"
    )
    print("Evidence records materialized: 0")
    print("Metrics recomputed: 0")
    print("Interpretations recomputed: 0")
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
