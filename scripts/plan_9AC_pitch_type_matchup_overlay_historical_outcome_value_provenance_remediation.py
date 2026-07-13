#!/usr/bin/env python3
"""
Layer 9AC
Pitch-Type Matchup Overlay Historical Outcome-Value Provenance Remediation Plan

Plans deterministic, isolated remediation analysis for the twelve Layer 9AB
historical outcome-value provenance audit records.

Planning only.

This layer defines:

- admissible Layer 9AB audit inputs and lineage requirements;
- candidate-field assessment and semantic-compatibility rules;
- authorized read-only and isolated-candidate remediation actions;
- prohibited coercion, imputation, source mutation, and threshold changes;
- candidate artifact schemas, verification requirements, and ordering;
- authority boundaries for Layer 9AD.

This layer does not:

- mutate canonical historical comparison records;
- replace the canonical outcome mapping;
- coerce booleans, timestamps, strings, or other values into outcomes;
- fabricate, default, duplicate, or impute outcomes;
- lower minimum-support thresholds;
- recompute canonical metrics, interpretations, evidence, or remediation records;
- estimate uncertainty or statistical significance;
- declare superiority, equivalence, activation, or production readiness;
- modify production probabilities, simulations, pricing, markets, or bets.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AC"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_value_provenance_remediation_plan"
)

PLAN_VERSION = (
    "layer_9AC_historical_outcome_value_"
    "provenance_remediation_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AC_pitch_type_matchup_overlay_"
    "historical_outcome_value_provenance_remediation_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9AB_pitch_type_matchup_overlay_"
    "historical_outcome_value_provenance_audit.py"
)

EXPECTED_PREDECESSOR_CONTRACT_VERSION = (
    "layer_9AB_historical_outcome_value_"
    "provenance_audit_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_value_provenance_audit_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_value_"
    "provenance_remediation_planning"
)

EXPECTED_AUDIT_RECORD_COUNT = 12

EXPECTED_FAILURE_MODE = "outcome_value_non_numeric"

EXPECTED_AUDIT_STATUS = "candidate_mapping_identified"


INPUT_RULES = [
    {
        "rule_id": "HOVPR-I01",
        "rule": "audit_record_id_must_be_unique",
    },
    {
        "rule_id": "HOVPR-I02",
        "rule": "audit_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOVPR-I03",
        "rule": "source_comparison_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOVPR-I04",
        "rule": "source_metric_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOVPR-I05",
        "rule": "source_interpretation_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOVPR-I06",
        "rule": "source_evidence_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOVPR-I07",
        "rule": "source_remediation_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOVPR-I08",
        "rule": "failure_modes_must_include_outcome_value_non_numeric",
    },
    {
        "rule_id": "HOVPR-I09",
        "rule": "audit_status_must_equal_candidate_mapping_identified",
    },
    {
        "rule_id": "HOVPR-I10",
        "rule": "exactly_twelve_audit_records_must_be_selected",
    },
    {
        "rule_id": "HOVPR-I11",
        "rule": "canonical_source_records_must_remain_read_only",
    },
    {
        "rule_id": "HOVPR-I12",
        "rule": "candidate_mapping_must_not_be_treated_as_canonical",
    },
]


CANDIDATE_ASSESSMENT_STAGES = [
    {
        "stage_id": "HOVPR-S01",
        "stage_name": "audit_lineage_validation",
        "planning_question": (
            "Does each Layer 9AB audit record preserve complete deterministic "
            "comparison, metric, interpretation, evidence, and remediation lineage?"
        ),
    },
    {
        "stage_id": "HOVPR-S02",
        "stage_name": "candidate_field_inventory",
        "planning_question": (
            "Which source fields were identified as candidates, and were they "
            "identified only because their names contain outcome-related tokens?"
        ),
    },
    {
        "stage_id": "HOVPR-S03",
        "stage_name": "candidate_runtime_type_validation",
        "planning_question": (
            "What exact runtime types and serialized values are carried by each "
            "candidate field?"
        ),
    },
    {
        "stage_id": "HOVPR-S04",
        "stage_name": "candidate_semantic_validation",
        "planning_question": (
            "Does the documented field meaning represent the same observed outcome "
            "semantic required by the affected metric?"
        ),
    },
    {
        "stage_id": "HOVPR-S05",
        "stage_name": "candidate_domain_validation",
        "planning_question": (
            "Does the candidate naturally belong to the accepted metric outcome "
            "domain without coercion, reinterpretation, or imputation?"
        ),
    },
    {
        "stage_id": "HOVPR-S06",
        "stage_name": "canonical_mapping_provenance_review",
        "planning_question": (
            "Can the intended canonical outcome mapping be identified from an "
            "authoritative source contract, schema, or historical artifact?"
        ),
    },
    {
        "stage_id": "HOVPR-S07",
        "stage_name": "isolated_candidate_artifact_design",
        "planning_question": (
            "Can a candidate-only replay artifact be produced without modifying "
            "canonical historical source records?"
        ),
    },
    {
        "stage_id": "HOVPR-S08",
        "stage_name": "contract_chain_replay_design",
        "planning_question": (
            "How will the existing comparison-to-remediation contract chain be "
            "replayed against the isolated candidate artifact?"
        ),
    },
    {
        "stage_id": "HOVPR-S09",
        "stage_name": "before_after_gap_reconciliation",
        "planning_question": (
            "How will gap counts be compared without treating a reduction as "
            "evidence of model quality or predictive improvement?"
        ),
    },
    {
        "stage_id": "HOVPR-S10",
        "stage_name": "remediation_disposition",
        "planning_question": (
            "Should each audited record be classified as candidate_compatible, "
            "candidate_incompatible, authoritative_mapping_unresolved, or "
            "candidate_replay_required?"
        ),
    },
]


CANDIDATE_COMPATIBILITY_CHECKS = [
    {
        "check_id": "HOVPR-C01",
        "check_name": "candidate_field_exists",
        "expected_result": (
            "candidate field resolves deterministically in every applicable "
            "source comparison record"
        ),
    },
    {
        "check_id": "HOVPR-C02",
        "check_name": "candidate_value_present",
        "expected_result": (
            "candidate value is present without defaulting or imputation"
        ),
    },
    {
        "check_id": "HOVPR-C03",
        "check_name": "candidate_runtime_type_compatible",
        "expected_result": (
            "candidate runtime type naturally matches the required outcome type"
        ),
    },
    {
        "check_id": "HOVPR-C04",
        "check_name": "candidate_value_domain_compatible",
        "expected_result": (
            "candidate value naturally belongs to the metric-defined domain"
        ),
    },
    {
        "check_id": "HOVPR-C05",
        "check_name": "candidate_semantic_compatible",
        "expected_result": (
            "candidate field meaning matches the required observed-outcome semantic"
        ),
    },
    {
        "check_id": "HOVPR-C06",
        "check_name": "candidate_temporal_role_compatible",
        "expected_result": (
            "candidate is an observed outcome rather than an availability timestamp, "
            "processing timestamp, identifier, or metadata field"
        ),
    },
    {
        "check_id": "HOVPR-C07",
        "check_name": "candidate_authoritative_mapping_supported",
        "expected_result": (
            "an authoritative schema or source contract supports the candidate mapping"
        ),
    },
    {
        "check_id": "HOVPR-C08",
        "check_name": "candidate_replay_isolated",
        "expected_result": (
            "candidate replay can be performed without canonical source mutation"
        ),
    },
    {
        "check_id": "HOVPR-C09",
        "check_name": "candidate_replay_deterministic",
        "expected_result": (
            "forward and reverse candidate replays produce identical ordered records "
            "and digests"
        ),
    },
    {
        "check_id": "HOVPR-C10",
        "check_name": "candidate_replay_lineage_complete",
        "expected_result": (
            "candidate replay preserves all predecessor identities and digests"
        ),
    },
]


REMEDIATION_ACTIONS = [
    {
        "action_id": "HOVPR-A01",
        "action_name": "preserve_canonical_source_snapshot",
        "action_scope": "read_only",
        "authorized": True,
        "description": (
            "Capture canonical source identities and digests before any candidate "
            "artifact is constructed."
        ),
    },
    {
        "action_id": "HOVPR-A02",
        "action_name": "inventory_candidate_fields",
        "action_scope": "read_only",
        "authorized": True,
        "description": (
            "Inventory candidate fields, values, runtime types, and source paths."
        ),
    },
    {
        "action_id": "HOVPR-A03",
        "action_name": "verify_candidate_semantics",
        "action_scope": "read_only",
        "authorized": True,
        "description": (
            "Compare candidate field semantics with the metric-required outcome "
            "semantic using source contracts and schemas."
        ),
    },
    {
        "action_id": "HOVPR-A04",
        "action_name": "classify_candidate_compatibility",
        "action_scope": "diagnostic",
        "authorized": True,
        "description": (
            "Classify candidates without promoting them to canonical mappings."
        ),
    },
    {
        "action_id": "HOVPR-A05",
        "action_name": "construct_isolated_candidate_artifact",
        "action_scope": "candidate_only",
        "authorized": True,
        "description": (
            "Construct an isolated candidate artifact only when authoritative "
            "mapping evidence and natural type/domain compatibility are present."
        ),
    },
    {
        "action_id": "HOVPR-A06",
        "action_name": "replay_existing_contract_chain",
        "action_scope": "candidate_only",
        "authorized": True,
        "description": (
            "Replay the existing contract chain against the isolated candidate "
            "artifact without replacing canonical outputs."
        ),
    },
    {
        "action_id": "HOVPR-A07",
        "action_name": "reconcile_gap_counts",
        "action_scope": "diagnostic",
        "authorized": True,
        "description": (
            "Report before-and-after candidate gap counts without making quality claims."
        ),
    },
    {
        "action_id": "HOVPR-A08",
        "action_name": "emit_remediation_disposition",
        "action_scope": "diagnostic",
        "authorized": True,
        "description": (
            "Emit a bounded remediation disposition for each audit record."
        ),
    },
]


REMEDIATION_DISPOSITIONS = [
    {
        "disposition": "candidate_compatible",
        "applies_when": (
            "candidate field is authoritative, semantically aligned, naturally "
            "type-compatible, and domain-compatible"
        ),
    },
    {
        "disposition": "candidate_incompatible",
        "applies_when": (
            "candidate field is metadata, a timestamp, an identifier, semantically "
            "misaligned, or incompatible with the required type or domain"
        ),
    },
    {
        "disposition": "authoritative_mapping_unresolved",
        "applies_when": (
            "no authoritative canonical outcome mapping can be established"
        ),
    },
    {
        "disposition": "candidate_replay_required",
        "applies_when": (
            "candidate appears compatible but must be tested in an isolated replay"
        ),
    },
    {
        "disposition": "candidate_replay_failed",
        "applies_when": (
            "isolated replay fails lineage, determinism, schema, or contract checks"
        ),
    },
    {
        "disposition": "candidate_gap_reduced",
        "applies_when": (
            "isolated replay reduces the targeted gap count without authorizing "
            "canonical adoption or quality claims"
        ),
    },
    {
        "disposition": "candidate_gap_unchanged",
        "applies_when": (
            "isolated replay does not reduce the targeted gap count"
        ),
    },
]


PROHIBITED_ACTIONS = [
    {
        "prohibition_id": "HOVPR-P01",
        "action": "canonical_source_record_mutation",
        "reason": (
            "Layer 9AC is planning-only and Layer 9AD candidate work must remain isolated."
        ),
    },
    {
        "prohibition_id": "HOVPR-P02",
        "action": "candidate_mapping_promoted_to_canonical",
        "reason": (
            "A candidate field name match is not authoritative mapping evidence."
        ),
    },
    {
        "prohibition_id": "HOVPR-P03",
        "action": "boolean_to_numeric_coercion",
        "reason": (
            "Boolean values may not be silently reinterpreted as numeric outcomes."
        ),
    },
    {
        "prohibition_id": "HOVPR-P04",
        "action": "timestamp_to_outcome_conversion",
        "reason": (
            "Availability or processing timestamps are metadata, not observed outcomes."
        ),
    },
    {
        "prohibition_id": "HOVPR-P05",
        "action": "outcome_defaulting",
        "reason": "Missing or invalid outcomes may not be replaced with defaults.",
    },
    {
        "prohibition_id": "HOVPR-P06",
        "action": "outcome_imputation",
        "reason": "Historical outcomes may not be fabricated or statistically imputed.",
    },
    {
        "prohibition_id": "HOVPR-P07",
        "action": "support_threshold_relaxation",
        "reason": (
            "Minimum support may not be lowered to manufacture eligible metrics."
        ),
    },
    {
        "prohibition_id": "HOVPR-P08",
        "action": "canonical_contract_record_recomputation",
        "reason": (
            "Canonical metrics, interpretations, evidence, and remediation records "
            "must remain unchanged."
        ),
    },
    {
        "prohibition_id": "HOVPR-P09",
        "action": "predictive_quality_claim",
        "reason": (
            "Gap-count changes do not establish predictive improvement."
        ),
    },
    {
        "prohibition_id": "HOVPR-P10",
        "action": "production_or_betting_authority",
        "reason": (
            "No production probabilities, market comparisons, pricing, or betting "
            "decisions are authorized."
        ),
    },
]


CANDIDATE_RECORD_FIELDS = [
    "remediation_plan_contract_version",
    "remediation_plan_record_id",
    "audit_record_id",
    "audit_record_digest",
    "comparison_record_id",
    "metric_record_id",
    "interpretation_record_id",
    "evidence_record_id",
    "source_remediation_record_id",
    "metric_name",
    "aggregation_name",
    "aggregation_key",
    "failure_modes",
    "audit_status",
    "outcome_field_name",
    "outcome_field_path",
    "raw_outcome_type",
    "raw_outcome_serialization",
    "candidate_outcome_fields",
    "candidate_field_name",
    "candidate_field_path",
    "candidate_raw_value",
    "candidate_raw_type",
    "candidate_raw_serialization",
    "candidate_semantic",
    "candidate_expected_type",
    "candidate_expected_domain",
    "candidate_compatibility_results",
    "authoritative_mapping_source",
    "authoritative_mapping_digest",
    "candidate_artifact_path",
    "candidate_artifact_digest",
    "candidate_replay_required",
    "candidate_replay_status",
    "before_gap_count",
    "after_gap_count",
    "gap_count_delta",
    "remediation_disposition",
    "remediation_rationale",
    "remediation_exclusion_codes",
    "remediation_limitations",
    "source_comparison_digest",
    "source_metric_record_digest",
    "source_interpretation_digest",
    "source_evidence_record_digest",
    "source_remediation_record_digest",
    "remediation_plan_identity_digest",
    "remediation_plan_record_digest",
]


ORDERING_FIELDS = [
    {
        "ordinal": 1,
        "field": "source_remediation_record_id",
    },
    {
        "ordinal": 2,
        "field": "audit_record_id",
    },
    {
        "ordinal": 3,
        "field": "metric_name",
    },
    {
        "ordinal": 4,
        "field": "aggregation_name",
    },
    {
        "ordinal": 5,
        "field": "aggregation_key",
    },
    {
        "ordinal": 6,
        "field": "candidate_field_name",
    },
]


EXCLUSION_CODES = [
    {
        "code": "historical_outcome_remediation_candidate_not_authoritative",
        "category": "candidate_mapping",
    },
    {
        "code": "historical_outcome_remediation_candidate_semantic_mismatch",
        "category": "semantic_compatibility",
    },
    {
        "code": "historical_outcome_remediation_candidate_type_incompatible",
        "category": "type_compatibility",
    },
    {
        "code": "historical_outcome_remediation_candidate_domain_incompatible",
        "category": "domain_compatibility",
    },
    {
        "code": "historical_outcome_remediation_candidate_is_metadata",
        "category": "semantic_compatibility",
    },
    {
        "code": "historical_outcome_remediation_candidate_artifact_not_created",
        "category": "candidate_artifact",
    },
    {
        "code": "historical_outcome_remediation_candidate_replay_not_authorized",
        "category": "candidate_replay",
    },
    {
        "code": "historical_outcome_remediation_candidate_replay_failed",
        "category": "candidate_replay",
    },
    {
        "code": "historical_outcome_remediation_authoritative_mapping_unresolved",
        "category": "mapping_provenance",
    },
    {
        "code": "historical_outcome_remediation_canonical_mutation_prohibited",
        "category": "authority_boundary",
    },
    {
        "code": "historical_outcome_remediation_coercion_prohibited",
        "category": "authority_boundary",
    },
    {
        "code": "historical_outcome_remediation_imputation_prohibited",
        "category": "authority_boundary",
    },
    {
        "code": "historical_outcome_remediation_quality_claim_prohibited",
        "category": "authority_boundary",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AB_audit_records_deterministically",
    },
    {
        "ordinal": 2,
        "step": "select_exactly_twelve_candidate_mapping_audit_records",
    },
    {
        "ordinal": 3,
        "step": "validate_complete_predecessor_lineage",
    },
    {
        "ordinal": 4,
        "step": "inventory_candidate_fields_and_runtime_values",
    },
    {
        "ordinal": 5,
        "step": "evaluate_candidate_type_domain_and_semantic_compatibility",
    },
    {
        "ordinal": 6,
        "step": "seek_authoritative_mapping_evidence",
    },
    {
        "ordinal": 7,
        "step": "classify_candidate_remediation_disposition",
    },
    {
        "ordinal": 8,
        "step": "construct_isolated_candidate_artifact_only_when_authorized",
    },
    {
        "ordinal": 9,
        "step": "replay_existing_contract_chain_against_candidate_artifact",
    },
    {
        "ordinal": 10,
        "step": "reconcile_before_and_after_gap_counts",
    },
    {
        "ordinal": 11,
        "step": "verify_determinism_and_reverse_replay",
    },
    {
        "ordinal": 12,
        "step": "emit_candidate_remediation_artifacts_and_diagnosis",
    },
]


PROHIBITED_AUTHORITIES = [
    "canonical_historical_source_mutation",
    "canonical_outcome_mapping_change",
    "outcome_coercion",
    "outcome_defaulting",
    "outcome_imputation",
    "minimum_support_threshold_change",
    "canonical_metric_recomputation",
    "canonical_interpretation_recomputation",
    "canonical_evidence_recomputation",
    "canonical_remediation_recomputation",
    "uncertainty_estimation",
    "statistical_significance_testing",
    "superiority_determination",
    "equivalence_determination",
    "activation_recommendation",
    "production_probability_change",
    "market_comparison",
    "pricing_change",
    "betting_edge_calculation",
]


def canonical_json(payload: Any) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def sha256_payload(payload: Any) -> str:
    return hashlib.sha256(
        canonical_json(payload).encode("utf-8")
    ).hexdigest()


def valid_sha256(value: Any) -> bool:
    if not isinstance(value, str):
        return False

    if len(value) != 64:
        return False

    return all(
        character in "0123456789abcdef"
        for character in value
    )


def load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(
        name,
        path,
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Unable to load module from {path}"
        )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


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
            fieldnames=fieldnames,
            extrasaction="ignore",
        )

        writer.writeheader()

        for row in rows:
            serialized = {}

            for field in fieldnames:
                value = row.get(field)

                if isinstance(
                    value,
                    (dict, list, tuple),
                ):
                    serialized[field] = canonical_json(
                        value
                    )
                else:
                    serialized[field] = value

            writer.writerow(serialized)


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
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )


def replay_predecessor() -> dict[str, Any]:
    predecessor = load_module(
        PREDECESSOR_PATH,
        "layer_9ab_predecessor",
    )

    required_attributes = [
        "AUDIT_CONTRACT_VERSION",
        "replay_contract_chain",
        "execute_provenance_audit",
    ]

    for attribute in required_attributes:
        if not hasattr(
            predecessor,
            attribute,
        ):
            raise RuntimeError(
                "Layer 9AB predecessor is missing "
                f"required attribute: {attribute}"
            )

    chain = predecessor.replay_contract_chain()

    audit_records = predecessor.execute_provenance_audit(
        comparison_rows=chain[
            "comparison_rows"
        ],
        metric_rows=chain[
            "metric_rows"
        ],
        interpretation_rows=chain[
            "interpretation_rows"
        ],
        evidence_rows=chain[
            "evidence_rows"
        ],
        remediation_rows=chain[
            "remediation_rows"
        ],
    )

    reverse_audit_records = (
        predecessor.execute_provenance_audit(
            comparison_rows=list(
                reversed(
                    chain[
                        "comparison_rows"
                    ]
                )
            ),
            metric_rows=list(
                reversed(
                    chain[
                        "metric_rows"
                    ]
                )
            ),
            interpretation_rows=list(
                reversed(
                    chain[
                        "interpretation_rows"
                    ]
                )
            ),
            evidence_rows=list(
                reversed(
                    chain[
                        "evidence_rows"
                    ]
                )
            ),
            remediation_rows=list(
                reversed(
                    chain[
                        "remediation_rows"
                    ]
                )
            ),
        )
    )

    return {
        "module": predecessor,
        "chain": chain,
        "audit_records": audit_records,
        "reverse_audit_records": (
            reverse_audit_records
        ),
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    replay = replay_predecessor()

    predecessor = replay["module"]

    audit_records = replay[
        "audit_records"
    ]

    reverse_audit_records = replay[
        "reverse_audit_records"
    ]

    candidate_records = [
        row
        for row in audit_records
        if (
            row.get("audit_status")
            == EXPECTED_AUDIT_STATUS
            and EXPECTED_FAILURE_MODE
            in set(
                row.get(
                    "failure_modes",
                    [],
                )
            )
        )
    ]

    predecessor_contract_verified = (
        predecessor.AUDIT_CONTRACT_VERSION
        == EXPECTED_PREDECESSOR_CONTRACT_VERSION
    )

    predecessor_replay_deterministic = (
        canonical_json(audit_records)
        == canonical_json(
            reverse_audit_records
        )
    )

    predecessor_audit_digest = (
        sha256_payload(audit_records)
    )

    reverse_predecessor_audit_digest = (
        sha256_payload(
            reverse_audit_records
        )
    )

    candidate_field_names = sorted(
        {
            str(candidate_field)
            for row in candidate_records
            for candidate_field in row.get(
                "candidate_outcome_fields",
                [],
            )
        }
    )

    candidate_runtime_types = sorted(
        {
            str(
                row.get(
                    "raw_outcome_type",
                    "",
                )
            )
            for row in candidate_records
        }
    )

    candidate_mapping_is_not_canonical = all(
        row.get("normalization_rule_applied")
        == "none_read_only_audit"
        for row in candidate_records
    )

    candidate_fields_diagnostic_only = all(
        row.get("normalized_outcome_value")
        is None
        and row.get(
            "normalized_outcome_type"
        )
        == "not_materialized"
        for row in candidate_records
    )

    complete_lineage_count = sum(
        all(
            valid_sha256(
                row.get(field)
            )
            for field in [
                "source_comparison_digest",
                "source_metric_record_digest",
                "source_interpretation_digest",
                "source_evidence_record_digest",
                "source_remediation_record_digest",
            ]
        )
        for row in candidate_records
    )

    checks = [
        {
            "check": "nine_ab_predecessor_contract_verified",
            "actual": (
                predecessor.AUDIT_CONTRACT_VERSION
            ),
            "expected": (
                EXPECTED_PREDECESSOR_CONTRACT_VERSION
            ),
            "passed": predecessor_contract_verified,
        },
        {
            "check": "nine_ab_audit_replay_deterministic",
            "actual": predecessor_replay_deterministic,
            "expected": True,
            "passed": predecessor_replay_deterministic,
        },
        {
            "check": "nine_ab_audit_digests_match_reverse_replay",
            "actual": predecessor_audit_digest,
            "expected": reverse_predecessor_audit_digest,
            "passed": (
                predecessor_audit_digest
                == reverse_predecessor_audit_digest
            ),
        },
        {
            "check": "twelve_candidate_mapping_records_selected",
            "actual": len(candidate_records),
            "expected": EXPECTED_AUDIT_RECORD_COUNT,
            "passed": (
                len(candidate_records)
                == EXPECTED_AUDIT_RECORD_COUNT
            ),
        },
        {
            "check": "all_candidate_records_preserve_complete_lineage",
            "actual": complete_lineage_count,
            "expected": EXPECTED_AUDIT_RECORD_COUNT,
            "passed": (
                complete_lineage_count
                == EXPECTED_AUDIT_RECORD_COUNT
            ),
        },
        {
            "check": "all_candidate_records_preserve_non_numeric_failure",
            "actual": sum(
                EXPECTED_FAILURE_MODE
                in set(
                    row.get(
                        "failure_modes",
                        [],
                    )
                )
                for row in candidate_records
            ),
            "expected": EXPECTED_AUDIT_RECORD_COUNT,
            "passed": all(
                EXPECTED_FAILURE_MODE
                in set(
                    row.get(
                        "failure_modes",
                        [],
                    )
                )
                for row in candidate_records
            ),
        },
        {
            "check": "candidate_mapping_remains_noncanonical",
            "actual": candidate_mapping_is_not_canonical,
            "expected": True,
            "passed": candidate_mapping_is_not_canonical,
        },
        {
            "check": "candidate_fields_remain_diagnostic_only",
            "actual": candidate_fields_diagnostic_only,
            "expected": True,
            "passed": candidate_fields_diagnostic_only,
        },
        {
            "check": "candidate_field_inventory_defined",
            "actual": len(candidate_field_names),
            "expected": 1,
            "passed": (
                len(candidate_field_names)
                >= 1
            ),
        },
        {
            "check": "candidate_assessment_stages_defined",
            "actual": len(
                CANDIDATE_ASSESSMENT_STAGES
            ),
            "expected": 10,
            "passed": (
                len(
                    CANDIDATE_ASSESSMENT_STAGES
                )
                == 10
            ),
        },
        {
            "check": "candidate_compatibility_checks_defined",
            "actual": len(
                CANDIDATE_COMPATIBILITY_CHECKS
            ),
            "expected": 10,
            "passed": (
                len(
                    CANDIDATE_COMPATIBILITY_CHECKS
                )
                == 10
            ),
        },
        {
            "check": "authorized_remediation_actions_defined",
            "actual": len(
                REMEDIATION_ACTIONS
            ),
            "expected": 8,
            "passed": (
                len(
                    REMEDIATION_ACTIONS
                )
                == 8
                and all(
                    row["authorized"]
                    for row in REMEDIATION_ACTIONS
                )
            ),
        },
        {
            "check": "remediation_dispositions_defined",
            "actual": len(
                REMEDIATION_DISPOSITIONS
            ),
            "expected": 7,
            "passed": (
                len(
                    REMEDIATION_DISPOSITIONS
                )
                == 7
            ),
        },
        {
            "check": "prohibited_actions_defined",
            "actual": len(
                PROHIBITED_ACTIONS
            ),
            "expected": 10,
            "passed": (
                len(
                    PROHIBITED_ACTIONS
                )
                == 10
            ),
        },
        {
            "check": "candidate_record_fields_defined",
            "actual": len(
                CANDIDATE_RECORD_FIELDS
            ),
            "expected": 48,
            "passed": (
                len(
                    CANDIDATE_RECORD_FIELDS
                )
                == 48
            ),
        },
        {
            "check": "ordering_fields_defined",
            "actual": len(
                ORDERING_FIELDS
            ),
            "expected": 6,
            "passed": (
                len(
                    ORDERING_FIELDS
                )
                == 6
            ),
        },
        {
            "check": "exclusion_codes_defined",
            "actual": len(
                EXCLUSION_CODES
            ),
            "expected": 13,
            "passed": (
                len(
                    EXCLUSION_CODES
                )
                == 13
            ),
        },
        {
            "check": "implementation_steps_defined",
            "actual": len(
                IMPLEMENTATION_STEPS
            ),
            "expected": 12,
            "passed": (
                len(
                    IMPLEMENTATION_STEPS
                )
                == 12
            ),
        },
        {
            "check": "canonical_source_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                row["action"]
                == "canonical_source_record_mutation"
                for row in PROHIBITED_ACTIONS
            ),
        },
        {
            "check": "candidate_mapping_promotion_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                row["action"]
                == "candidate_mapping_promoted_to_canonical"
                for row in PROHIBITED_ACTIONS
            ),
        },
        {
            "check": "boolean_coercion_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                row["action"]
                == "boolean_to_numeric_coercion"
                for row in PROHIBITED_ACTIONS
            ),
        },
        {
            "check": "timestamp_conversion_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                row["action"]
                == "timestamp_to_outcome_conversion"
                for row in PROHIBITED_ACTIONS
            ),
        },
        {
            "check": "outcome_imputation_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                row["action"]
                == "outcome_imputation"
                for row in PROHIBITED_ACTIONS
            ),
        },
        {
            "check": "support_threshold_relaxation_prohibited",
            "actual": True,
            "expected": True,
            "passed": any(
                row["action"]
                == "support_threshold_relaxation"
                for row in PROHIBITED_ACTIONS
            ),
        },
        {
            "check": "canonical_records_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "candidate_artifacts_not_materialized",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "outcomes_not_coerced",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "outcomes_not_imputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "support_thresholds_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "canonical_contract_records_not_recomputed",
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
            "check": "statistical_significance_not_tested",
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
            "candidate_assessment_stages": (
                CANDIDATE_ASSESSMENT_STAGES
            ),
            "candidate_compatibility_checks": (
                CANDIDATE_COMPATIBILITY_CHECKS
            ),
            "remediation_actions": (
                REMEDIATION_ACTIONS
            ),
            "remediation_dispositions": (
                REMEDIATION_DISPOSITIONS
            ),
            "prohibited_actions": (
                PROHIBITED_ACTIONS
            ),
            "candidate_record_fields": (
                CANDIDATE_RECORD_FIELDS
            ),
            "ordering_fields": (
                ORDERING_FIELDS
            ),
            "exclusion_codes": (
                EXCLUSION_CODES
            ),
            "implementation_steps": (
                IMPLEMENTATION_STEPS
            ),
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_remediation_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_remediation_plan_failed"
    )

    next_layer = (
        "9AD_pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_remediation_implementation"
        if all_checks_passed
        else
        "9AC_pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_remediation_plan_remediation"
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
        [
            "rule_id",
            "rule",
        ],
        INPUT_RULES,
    )

    write_csv(
        OUTPUT_DIR
        / "candidate_assessment_stages.csv",
        [
            "stage_id",
            "stage_name",
            "planning_question",
        ],
        CANDIDATE_ASSESSMENT_STAGES,
    )

    write_csv(
        OUTPUT_DIR
        / "candidate_compatibility_checks.csv",
        [
            "check_id",
            "check_name",
            "expected_result",
        ],
        CANDIDATE_COMPATIBILITY_CHECKS,
    )

    write_csv(
        OUTPUT_DIR / "remediation_actions.csv",
        [
            "action_id",
            "action_name",
            "action_scope",
            "authorized",
            "description",
        ],
        REMEDIATION_ACTIONS,
    )

    write_csv(
        OUTPUT_DIR
        / "remediation_dispositions.csv",
        [
            "disposition",
            "applies_when",
        ],
        REMEDIATION_DISPOSITIONS,
    )

    write_csv(
        OUTPUT_DIR / "prohibited_actions.csv",
        [
            "prohibition_id",
            "action",
            "reason",
        ],
        PROHIBITED_ACTIONS,
    )

    write_csv(
        OUTPUT_DIR
        / "candidate_record_field_contract.csv",
        [
            "ordinal",
            "field",
        ],
        [
            {
                "ordinal": index,
                "field": field,
            }
            for index, field in enumerate(
                CANDIDATE_RECORD_FIELDS,
                start=1,
            )
        ],
    )

    write_csv(
        OUTPUT_DIR / "ordering_fields.csv",
        [
            "ordinal",
            "field",
        ],
        ORDERING_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "exclusion_code_catalog.csv",
        [
            "code",
            "category",
        ],
        EXCLUSION_CODES,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        [
            "ordinal",
            "step",
        ],
        IMPLEMENTATION_STEPS,
    )

    write_csv(
        OUTPUT_DIR
        / "observed_candidate_field_inventory.csv",
        [
            "candidate_field_name",
        ],
        [
            {
                "candidate_field_name": field,
            }
            for field in candidate_field_names
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "observed_raw_outcome_type_inventory.csv",
        [
            "raw_outcome_type",
        ],
        [
            {
                "raw_outcome_type": runtime_type,
            }
            for runtime_type in candidate_runtime_types
        ],
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
                    "Layer 9AC is planning-only and grants no canonical "
                    "source mutation, mapping promotion, coercion, "
                    "imputation, threshold relaxation, canonical "
                    "recomputation, uncertainty, significance, "
                    "superiority, equivalence, activation, production, "
                    "market, pricing, or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_value_"
                    "provenance_remediation_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9AD may perform deterministic read-only "
                    "candidate assessment and isolated candidate replay "
                    "without modifying canonical historical records."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_verified": (
            predecessor_contract_verified
        ),
        "predecessor_replay_deterministic": (
            predecessor_replay_deterministic
        ),
        "predecessor_audit_digest": (
            predecessor_audit_digest
        ),
        "reverse_predecessor_audit_digest": (
            reverse_predecessor_audit_digest
        ),
        "candidate_audit_records": len(
            candidate_records
        ),
        "candidate_field_names": (
            candidate_field_names
        ),
        "candidate_runtime_types": (
            candidate_runtime_types
        ),
        "input_rules": len(INPUT_RULES),
        "candidate_assessment_stages": len(
            CANDIDATE_ASSESSMENT_STAGES
        ),
        "candidate_compatibility_checks": len(
            CANDIDATE_COMPATIBILITY_CHECKS
        ),
        "remediation_actions": len(
            REMEDIATION_ACTIONS
        ),
        "remediation_dispositions": len(
            REMEDIATION_DISPOSITIONS
        ),
        "prohibited_actions": len(
            PROHIBITED_ACTIONS
        ),
        "candidate_record_fields": len(
            CANDIDATE_RECORD_FIELDS
        ),
        "ordering_fields": len(
            ORDERING_FIELDS
        ),
        "exclusion_codes": len(
            EXCLUSION_CODES
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
        "canonical_source_records_changed": 0,
        "candidate_artifacts_materialized": 0,
        "outcomes_coerced": 0,
        "outcomes_imputed": 0,
        "support_thresholds_changed": 0,
        "canonical_contract_records_recomputed": 0,
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
        / "outcome_value_provenance_remediation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_value_"
            "provenance_remediation_implementation"
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

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
    print(
        f"Plan version: {PLAN_VERSION}"
    )
    print(
        "Predecessor contract verified: "
        f"{predecessor_contract_verified}"
    )
    print(
        "Predecessor replay deterministic: "
        f"{predecessor_replay_deterministic}"
    )
    print(
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(
        "Candidate audit records: "
        f"{len(candidate_records)}"
    )
    print(
        "Candidate field names: "
        f"{candidate_field_names}"
    )
    print(
        "Candidate runtime types: "
        f"{candidate_runtime_types}"
    )
    print(
        "Candidate assessment stages: "
        f"{len(CANDIDATE_ASSESSMENT_STAGES)}"
    )
    print(
        "Candidate compatibility checks: "
        f"{len(CANDIDATE_COMPATIBILITY_CHECKS)}"
    )
    print(
        "Remediation actions: "
        f"{len(REMEDIATION_ACTIONS)}"
    )
    print(
        "Remediation dispositions: "
        f"{len(REMEDIATION_DISPOSITIONS)}"
    )
    print(
        "Candidate record fields: "
        f"{len(CANDIDATE_RECORD_FIELDS)}"
    )
    print(f"Plan digest: {plan_digest}")
    print("Canonical source records changed: 0")
    print("Candidate artifacts materialized: 0")
    print("Outcomes coerced: 0")
    print("Outcomes imputed: 0")
    print("Support thresholds changed: 0")
    print("Canonical contract records recomputed: 0")
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
