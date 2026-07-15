#!/usr/bin/env python3
"""
Layer 9AY
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Submission
Source Evidence Validation Plan

Plans the deterministic validation contract for source evidence associated with
an explicitly supplied authoritative historical-outcome endpoint candidate and
evidence-locator submission.

Layer 9AX established that no endpoint candidate or locator-submission envelope
exists. This layer defines how a future implementation must validate supplied
source evidence without inventing, retrieving, or fabricating evidence.

Planning only.

This layer does not:
- invent or select an endpoint candidate;
- invent or complete a locator submission;
- invent, retrieve, or fabricate source evidence;
- store credential literals;
- perform network retrieval;
- retrieve historical outcome values;
- mutate canonical source values or mappings;
- transform, infer, default, impute, or substitute values;
- recompute downstream records;
- grant production, market, pricing, or betting authority.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AY"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_submission_source_evidence_validation_plan"
)

PLAN_VERSION = (
    "layer_9AY_historical_outcome_authoritative_source_endpoint_candidate_"
    "submission_source_evidence_validation_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AY_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_submission_source_evidence_"
    "validation_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "submit_9AX_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_evidence_locator.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AX_historical_outcome_authoritative_source_endpoint_candidate_"
    "evidence_locator_submission_contract_v1"
)

EXPECTED_SUBMISSION_RECORDS = 16
EXPECTED_SUBMISSION_COMPARISONS = 16

EXPECTED_SUBMISSION_STATUS = "candidate_not_supplied"

EXPECTED_SUBMISSION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


VALIDATION_PRINCIPLES = [
    {
        "principle_id": "HOASEV-P01",
        "principle": (
            "Source evidence may be validated only for an explicitly supplied "
            "candidate and locator-submission envelope."
        ),
    },
    {
        "principle_id": "HOASEV-P02",
        "principle": (
            "Every evidence artifact must preserve exact candidate, locator, "
            "comparison, and evidence-class lineage."
        ),
    },
    {
        "principle_id": "HOASEV-P03",
        "principle": (
            "Evidence contents must be evaluated as supplied and may not be "
            "completed, inferred, summarized into missing claims, or fabricated."
        ),
    },
    {
        "principle_id": "HOASEV-P04",
        "principle": (
            "Immutable evidence artifacts must be validated against declared "
            "SHA-256 digests before semantic validation."
        ),
    },
    {
        "principle_id": "HOASEV-P05",
        "principle": (
            "Source-owner authority and source-class claims must be supported "
            "by explicit retained evidence."
        ),
    },
    {
        "principle_id": "HOASEV-P06",
        "principle": (
            "Historical coverage, identity semantics, and outcome semantics "
            "must be validated independently."
        ),
    },
    {
        "principle_id": "HOASEV-P07",
        "principle": (
            "Licensing, availability, schema, versioning, and credential "
            "requirements must be validated before retrieval authority."
        ),
    },
    {
        "principle_id": "HOASEV-P08",
        "principle": (
            "Successful source-evidence validation grants validation "
            "implementation authority only, not source retrieval authority."
        ),
    },
]


EVIDENCE_CLASSES = [
    {
        "evidence_class_id": "HOASEV-C01",
        "evidence_class": "source_owner_authority_evidence",
        "required": True,
        "priority": 1,
    },
    {
        "evidence_class_id": "HOASEV-C02",
        "evidence_class": "historical_coverage_evidence",
        "required": True,
        "priority": 2,
    },
    {
        "evidence_class_id": "HOASEV-C03",
        "evidence_class": "identity_semantics_evidence",
        "required": True,
        "priority": 3,
    },
    {
        "evidence_class_id": "HOASEV-C04",
        "evidence_class": "outcome_semantics_evidence",
        "required": True,
        "priority": 4,
    },
    {
        "evidence_class_id": "HOASEV-C05",
        "evidence_class": "licensing_and_terms_evidence",
        "required": True,
        "priority": 5,
    },
    {
        "evidence_class_id": "HOASEV-C06",
        "evidence_class": "availability_and_stability_evidence",
        "required": True,
        "priority": 6,
    },
    {
        "evidence_class_id": "HOASEV-C07",
        "evidence_class": "schema_version_snapshot_evidence",
        "required": True,
        "priority": 7,
    },
]


VALIDATION_STAGES = [
    {
        "stage_id": "HOASEV-S01",
        "stage_name": "locator_submission_replay",
        "priority": 1,
    },
    {
        "stage_id": "HOASEV-S02",
        "stage_name": "candidate_and_submission_presence_gate",
        "priority": 2,
    },
    {
        "stage_id": "HOASEV-S03",
        "stage_name": "supplied_evidence_inventory",
        "priority": 3,
    },
    {
        "stage_id": "HOASEV-S04",
        "stage_name": "evidence_identity_and_lineage_validation",
        "priority": 4,
    },
    {
        "stage_id": "HOASEV-S05",
        "stage_name": "artifact_integrity_and_digest_validation",
        "priority": 5,
    },
    {
        "stage_id": "HOASEV-S06",
        "stage_name": "source_owner_authority_validation",
        "priority": 6,
    },
    {
        "stage_id": "HOASEV-S07",
        "stage_name": "coverage_identity_and_outcome_semantics_validation",
        "priority": 7,
    },
    {
        "stage_id": "HOASEV-S08",
        "stage_name": "licensing_availability_and_schema_validation",
        "priority": 8,
    },
    {
        "stage_id": "HOASEV-S09",
        "stage_name": "cross_evidence_consistency_validation",
        "priority": 9,
    },
    {
        "stage_id": "HOASEV-S10",
        "stage_name": "validation_blocker_and_disposition_assignment",
        "priority": 10,
    },
    {
        "stage_id": "HOASEV-S11",
        "stage_name": "deterministic_validation_record_emission",
        "priority": 11,
    },
]


VALIDATION_REQUIREMENTS = [
    {
        "requirement_id": "HOASEV-R01",
        "requirement": "candidate_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R02",
        "requirement": "locator_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R03",
        "requirement": "candidate_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R04",
        "requirement": "candidate_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R05",
        "requirement": "locator_submission_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R06",
        "requirement": "locator_submission_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R07",
        "requirement": "source_evidence_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R08",
        "requirement": "source_evidence_submission_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R09",
        "requirement": "source_evidence_submission_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R10",
        "requirement": "evidence_artifact_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R11",
        "requirement": "evidence_class_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R12",
        "requirement": "candidate_lineage_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R13",
        "requirement": "locator_lineage_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R14",
        "requirement": "comparison_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R15",
        "requirement": "source_owner_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R16",
        "requirement": "source_class_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R17",
        "requirement": "artifact_content_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R18",
        "requirement": "artifact_digest_algorithm",
        "expected": "sha256",
    },
    {
        "requirement_id": "HOASEV-R19",
        "requirement": "artifact_digest_valid",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R20",
        "requirement": "source_owner_authority_supported",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R21",
        "requirement": "historical_coverage_supported",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R22",
        "requirement": "identity_semantics_supported",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R23",
        "requirement": "outcome_semantics_supported",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R24",
        "requirement": "licensing_and_terms_supported",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R25",
        "requirement": "availability_and_stability_supported",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R26",
        "requirement": "schema_version_snapshot_supported",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R27",
        "requirement": "required_evidence_classes_complete",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R28",
        "requirement": "duplicate_evidence_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R29",
        "requirement": "conflicting_evidence_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R30",
        "requirement": "credential_literal_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOASEV-R31",
        "requirement": "network_retrieval_executed",
        "expected": False,
    },
]


VALIDATION_STATUSES = [
    {
        "status": "source_evidence_validation_approved",
        "implementation_authority": True,
    },
    {
        "status": "candidate_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "locator_submission_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "source_evidence_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "evidence_identity_or_lineage_invalid",
        "implementation_authority": False,
    },
    {
        "status": "artifact_integrity_invalid",
        "implementation_authority": False,
    },
    {
        "status": "source_authority_not_supported",
        "implementation_authority": False,
    },
    {
        "status": "semantic_evidence_incomplete",
        "implementation_authority": False,
    },
    {
        "status": "licensing_availability_or_schema_invalid",
        "implementation_authority": False,
    },
    {
        "status": "evidence_conflict",
        "implementation_authority": False,
    },
]


BLOCKER_CODES = [
    {
        "code": "historical_outcome_endpoint_candidate_missing",
        "category": "submission",
    },
    {
        "code": "historical_outcome_evidence_locator_submission_missing",
        "category": "submission",
    },
    {
        "code": "historical_outcome_source_evidence_submission_missing",
        "category": "submission",
    },
    {
        "code": "historical_outcome_source_evidence_submission_id_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_evidence_submission_version_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_evidence_artifact_id_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_evidence_class_missing",
        "category": "classification",
    },
    {
        "code": "historical_outcome_source_evidence_candidate_lineage_mismatch",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_source_evidence_locator_lineage_mismatch",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_source_evidence_comparison_scope_mismatch",
        "category": "scope",
    },
    {
        "code": "historical_outcome_source_evidence_owner_scope_mismatch",
        "category": "scope",
    },
    {
        "code": "historical_outcome_source_evidence_class_scope_mismatch",
        "category": "scope",
    },
    {
        "code": "historical_outcome_source_evidence_artifact_content_missing",
        "category": "artifact",
    },
    {
        "code": "historical_outcome_source_evidence_digest_missing",
        "category": "integrity",
    },
    {
        "code": "historical_outcome_source_evidence_digest_invalid",
        "category": "integrity",
    },
    {
        "code": "historical_outcome_source_owner_authority_not_supported",
        "category": "authority",
    },
    {
        "code": "historical_outcome_source_historical_coverage_not_supported",
        "category": "coverage",
    },
    {
        "code": "historical_outcome_source_identity_semantics_not_supported",
        "category": "semantics",
    },
    {
        "code": "historical_outcome_source_outcome_semantics_not_supported",
        "category": "semantics",
    },
    {
        "code": "historical_outcome_source_licensing_terms_not_supported",
        "category": "licensing",
    },
    {
        "code": "historical_outcome_source_availability_stability_not_supported",
        "category": "stability",
    },
    {
        "code": "historical_outcome_source_schema_version_snapshot_not_supported",
        "category": "schema",
    },
    {
        "code": "historical_outcome_source_required_evidence_class_missing",
        "category": "completeness",
    },
    {
        "code": "historical_outcome_source_evidence_duplicate",
        "category": "conflict",
    },
    {
        "code": "historical_outcome_source_evidence_conflict",
        "category": "conflict",
    },
    {
        "code": "historical_outcome_source_evidence_credential_literal_detected",
        "category": "security",
    },
    {
        "code": "historical_outcome_source_evidence_invention_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_source_evidence_retrieval_requested",
        "category": "authority",
    },
]


VALIDATION_RECORD_FIELDS = [
    "source_evidence_validation_plan_contract_version",
    "source_evidence_validation_plan_record_id",
    "evidence_locator_submission_plan_record_id",
    "evidence_locator_submission_plan_record_digest",
    "evidence_locator_specification_plan_record_id",
    "source_evidence_acquisition_plan_record_id",
    "endpoint_candidate_specification_record_id",
    "authoritative_source_endpoint_configuration_record_id",
    "comparison_record_id",
    "metric_record_id",
    "metric_name",
    "aggregation_name",
    "aggregation_key",
    "authoritative_field_name",
    "authoritative_field_path",
    "rejected_metadata_field_name",
    "defect_source_path",
    "defect_source_symbol",
    "defect_source_record_id",
    "defect_source_record_digest",
    "locator_submission_status",
    "locator_submission_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "source_owner",
    "source_class",
    "locator_submission_supplied",
    "locator_submission_id",
    "locator_submission_version",
    "source_evidence_submission_supplied",
    "source_evidence_submission_id",
    "source_evidence_submission_version",
    "source_evidence_artifacts",
    "required_evidence_classes",
    "source_evidence_validation_status",
    "source_evidence_validation_blocker_codes",
    "source_evidence_validation_implementation_authority_granted",
    "source_evidence_validation_rationale",
    "source_evidence_validation_limitations",
    "source_evidence_validation_plan_identity_digest",
    "source_evidence_validation_plan_record_digest",
]


ORDERING_FIELDS = [
    {
        "ordinal": 1,
        "field": "comparison_record_id",
    },
    {
        "ordinal": 2,
        "field": "defect_source_record_id",
    },
    {
        "ordinal": 3,
        "field": "candidate_id",
    },
    {
        "ordinal": 4,
        "field": "locator_submission_id",
    },
    {
        "ordinal": 5,
        "field": "source_evidence_submission_id",
    },
    {
        "ordinal": 6,
        "field": "source_evidence_validation_plan_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AX_locator_submission_records",
    },
    {
        "ordinal": 2,
        "step": "preserve_candidate_locator_and_defect_lineage",
    },
    {
        "ordinal": 3,
        "step": "require_explicit_candidate_and_locator_submission",
    },
    {
        "ordinal": 4,
        "step": "load_explicit_source_evidence_submissions",
    },
    {
        "ordinal": 5,
        "step": "inventory_supplied_evidence_artifacts",
    },
    {
        "ordinal": 6,
        "step": "validate_evidence_submission_identity_and_version",
    },
    {
        "ordinal": 7,
        "step": "validate_candidate_locator_comparison_and_source_scope",
    },
    {
        "ordinal": 8,
        "step": "validate_artifact_content_and_sha256_integrity",
    },
    {
        "ordinal": 9,
        "step": "validate_source_owner_authority_evidence",
    },
    {
        "ordinal": 10,
        "step": "validate_coverage_identity_and_outcome_semantics_evidence",
    },
    {
        "ordinal": 11,
        "step": "validate_licensing_availability_and_schema_evidence",
    },
    {
        "ordinal": 12,
        "step": "validate_required_evidence_class_completeness",
    },
    {
        "ordinal": 13,
        "step": "validate_duplicate_and_conflicting_evidence",
    },
    {
        "ordinal": 14,
        "step": "emit_deterministic_validation_records",
    },
    {
        "ordinal": 15,
        "step": "verify_forward_and_reverse_replay",
    },
    {
        "ordinal": 16,
        "step": "grant_validation_implementation_only_when_complete",
    },
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "endpoint_candidate_selection_without_submission",
    "evidence_locator_invention",
    "evidence_locator_selection_without_submission",
    "locator_submission_invention",
    "locator_submission_completion_by_inference",
    "source_evidence_invention",
    "source_evidence_completion_by_inference",
    "source_evidence_fabrication",
    "credential_literal_storage",
    "source_evidence_retrieval_planning",
    "source_evidence_fetch_execution",
    "source_evidence_parse_execution",
    "candidate_approval",
    "candidate_materialization",
    "historical_outcome_retrieval_planning",
    "historical_outcome_fetch_execution",
    "historical_outcome_parse_execution",
    "raw_endpoint_response_materialization",
    "canonical_source_value_mutation",
    "canonical_outcome_mapping_change",
    "boolean_to_integer_coercion",
    "source_value_defaulting",
    "source_value_inference",
    "source_value_imputation",
    "heuristic_candidate_substitution",
    "canonical_evaluation_row_recomputation",
    "canonical_join_record_recomputation",
    "canonical_comparison_record_recomputation",
    "canonical_metric_recomputation",
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
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(
            character in "0123456789abcdef"
            for character in value
        )
    )


def load_module(
    path: Path,
    module_name: str,
) -> Any:
    spec = importlib.util.spec_from_file_location(
        module_name,
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
            fieldnames=list(fieldnames),
            extrasaction="ignore",
        )

        writer.writeheader()

        for row in rows:
            serialized: dict[str, Any] = {}

            for field in fieldnames:
                value = row.get(field)

                serialized[field] = (
                    canonical_json(value)
                    if isinstance(
                        value,
                        (dict, list, tuple),
                    )
                    else value
                )

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
        "layer_9ax_predecessor",
    )

    if (
        predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AX contract version: "
            f"{predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_locator_submission_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_locator_submission_records(
        plan,
        list(
            reversed(
                replay["reverse_records"]
            )
        ),
    )

    return {
        "module": predecessor,
        "records": records,
        "reverse_records": reverse_records,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    replay = replay_predecessor()

    predecessor = replay["module"]
    records = replay["records"]
    reverse_records = replay["reverse_records"]

    comparison_ids = {
        row["comparison_record_id"]
        for row in records
    }

    status_counts = Counter(
        row["locator_submission_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "locator_submission_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_ax_contract_version_verified",
            "actual":
                predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_ax_replay_deterministic",
            "actual": (
                canonical_json(records)
                == canonical_json(reverse_records)
            ),
            "expected": True,
            "passed": (
                canonical_json(records)
                == canonical_json(reverse_records)
            ),
        },
        {
            "check": "nine_ax_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": (
                sha256_payload(records)
                == sha256_payload(reverse_records)
            ),
        },
        {
            "check": "expected_submission_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_SUBMISSION_RECORDS,
            "passed": (
                len(records)
                == EXPECTED_SUBMISSION_RECORDS
            ),
        },
        {
            "check": "expected_submission_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_SUBMISSION_COMPARISONS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_SUBMISSION_COMPARISONS
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(
                sorted(status_counts.items())
            ),
            "expected": {
                EXPECTED_SUBMISSION_STATUS:
                    EXPECTED_SUBMISSION_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_SUBMISSION_STATUS:
                        EXPECTED_SUBMISSION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(
                sorted(blocker_counts.items())
            ),
            "expected": {
                EXPECTED_SUBMISSION_BLOCKER:
                    EXPECTED_SUBMISSION_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_SUBMISSION_BLOCKER:
                        EXPECTED_SUBMISSION_RECORDS
                }
            ),
        },
        {
            "check": "all_submission_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_locator_submission_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_locator_submission_plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_name"]
                    for row in records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_NAME],
            "passed": all(
                row["authoritative_field_name"]
                == AUTHORITATIVE_FIELD_NAME
                for row in records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_path"]
                    for row in records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_PATH],
            "passed": all(
                row["authoritative_field_path"]
                == AUTHORITATIVE_FIELD_PATH
                for row in records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row["rejected_metadata_field_name"]
                    for row in records
                }
            ),
            "expected": [REJECTED_METADATA_FIELD],
            "passed": all(
                row["rejected_metadata_field_name"]
                == REJECTED_METADATA_FIELD
                for row in records
            ),
        },
        {
            "check": "validation_principles_defined",
            "actual": len(VALIDATION_PRINCIPLES),
            "expected": 8,
            "passed": len(VALIDATION_PRINCIPLES) == 8,
        },
        {
            "check": "evidence_classes_defined",
            "actual": len(EVIDENCE_CLASSES),
            "expected": 7,
            "passed": len(EVIDENCE_CLASSES) == 7,
        },
        {
            "check": "validation_stages_defined",
            "actual": len(VALIDATION_STAGES),
            "expected": 11,
            "passed": len(VALIDATION_STAGES) == 11,
        },
        {
            "check": "validation_requirements_defined",
            "actual": len(VALIDATION_REQUIREMENTS),
            "expected": 31,
            "passed": len(VALIDATION_REQUIREMENTS) == 31,
        },
        {
            "check": "validation_statuses_defined",
            "actual": len(VALIDATION_STATUSES),
            "expected": 10,
            "passed": len(VALIDATION_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 28,
            "passed": len(BLOCKER_CODES) == 28,
        },
        {
            "check": "validation_record_fields_defined",
            "actual": len(VALIDATION_RECORD_FIELDS),
            "expected": 42,
            "passed": len(VALIDATION_RECORD_FIELDS) == 42,
        },
        {
            "check": "ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 6,
            "passed": len(ORDERING_FIELDS) == 6,
        },
        {
            "check": "implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 16,
            "passed": len(IMPLEMENTATION_STEPS) == 16,
        },
        {
            "check": "candidate_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "endpoint_candidate_invention"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "locator_submission_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "locator_submission_invention"
                in PROHIBITED_AUTHORITIES
                and
                "locator_submission_completion_by_inference"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "source_evidence_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "source_evidence_invention"
                in PROHIBITED_AUTHORITIES
                and
                "source_evidence_completion_by_inference"
                in PROHIBITED_AUTHORITIES
                and
                "source_evidence_fabrication"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "credential_literal_storage_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "credential_literal_storage"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "source_evidence_retrieval_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "source_evidence_fetch_execution"
                in PROHIBITED_AUTHORITIES
                and
                "source_evidence_parse_execution"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "historical_outcome_retrieval_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "historical_outcome_fetch_execution"
                in PROHIBITED_AUTHORITIES
                and
                "historical_outcome_parse_execution"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "canonical_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_source_value_mutation"
                in PROHIBITED_AUTHORITIES
                and
                "canonical_outcome_mapping_change"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "value_transformation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "boolean_to_integer_coercion"
                in PROHIBITED_AUTHORITIES
                and
                "source_value_defaulting"
                in PROHIBITED_AUTHORITIES
                and
                "source_value_inference"
                in PROHIBITED_AUTHORITIES
                and
                "source_value_imputation"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "source_evidence_validations_materialized_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "network_retrievals_executed_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "quality_and_production_authority_absent",
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
            "validation_principles":
                VALIDATION_PRINCIPLES,
            "evidence_classes":
                EVIDENCE_CLASSES,
            "validation_stages":
                VALIDATION_STAGES,
            "validation_requirements":
                VALIDATION_REQUIREMENTS,
            "validation_statuses":
                VALIDATION_STATUSES,
            "blocker_codes":
                BLOCKER_CODES,
            "validation_record_fields":
                VALIDATION_RECORD_FIELDS,
            "ordering_fields":
                ORDERING_FIELDS,
            "implementation_steps":
                IMPLEMENTATION_STEPS,
            "prohibited_authorities":
                PROHIBITED_AUTHORITIES,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_submission_source_evidence_validation_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_submission_source_evidence_validation_plan_failed"
    )

    next_layer = (
        "9AZ_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_submission_source_evidence_validation_implementation"
        if all_checks_passed
        else
        "9AY_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_submission_source_evidence_validation_plan_"
        "remediation"
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
        OUTPUT_DIR / "validation_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        VALIDATION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "evidence_classes.csv",
        [
            "evidence_class_id",
            "evidence_class",
            "required",
            "priority",
        ],
        EVIDENCE_CLASSES,
    )

    write_csv(
        OUTPUT_DIR / "validation_stages.csv",
        [
            "stage_id",
            "stage_name",
            "priority",
        ],
        VALIDATION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "validation_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        VALIDATION_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "validation_statuses.csv",
        [
            "status",
            "implementation_authority",
        ],
        VALIDATION_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "blocker_code_catalog.csv",
        [
            "code",
            "category",
        ],
        BLOCKER_CODES,
    )

    write_csv(
        OUTPUT_DIR / "validation_record_field_contract.csv",
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
                VALIDATION_RECORD_FIELDS,
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
        OUTPUT_DIR / "implementation_steps.csv",
        [
            "ordinal",
            "step",
        ],
        IMPLEMENTATION_STEPS,
    )

    write_csv(
        OUTPUT_DIR / "candidate_missing_locator_submission_inventory.csv",
        [
            "evidence_locator_submission_plan_record_id",
            "evidence_locator_submission_plan_record_digest",
            "evidence_locator_specification_plan_record_id",
            "source_evidence_acquisition_plan_record_id",
            "endpoint_candidate_specification_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "locator_submission_status",
            "locator_submission_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "source_owner",
            "source_class",
            "locator_submission_supplied",
            "locator_submission_id",
            "locator_submission_version",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION,
        "locator_submission_records":
            len(records),
        "locator_submission_comparisons":
            len(comparison_ids),
        "locator_submission_status_counts":
            dict(sorted(status_counts.items())),
        "locator_submission_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "validation_principles":
            len(VALIDATION_PRINCIPLES),
        "evidence_classes":
            len(EVIDENCE_CLASSES),
        "validation_stages":
            len(VALIDATION_STAGES),
        "validation_requirements":
            len(VALIDATION_REQUIREMENTS),
        "validation_statuses":
            len(VALIDATION_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "validation_record_fields":
            len(VALIDATION_RECORD_FIELDS),
        "ordering_fields":
            len(ORDERING_FIELDS),
        "implementation_steps":
            len(IMPLEMENTATION_STEPS),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required":
            len(checks),
        "predecessor_digest":
            sha256_payload(records),
        "reverse_predecessor_digest":
            sha256_payload(reverse_records),
        "plan_digest":
            plan_digest,
        "source_evidence_validations_materialized": 0,
        "source_evidence_artifacts_supplied": 0,
        "credentials_stored": 0,
        "network_retrievals_executed": 0,
        "source_evidence_acquired": 0,
        "historical_outcome_values_acquired": 0,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "candidate_values_transformed": 0,
        "downstream_records_recomputed": 0,
        "uncertainty_estimates_calculated": 0,
        "statistical_significance_tests_calculated": 0,
        "superiority_decisions_emitted": 0,
        "equivalence_decisions_emitted": 0,
        "activation_recommendations_emitted": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed":
            all_checks_passed,
        "recommended_next_layer":
            next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "source_evidence_validation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "submission_source_evidence_validation_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld":
            sorted(PROHIBITED_AUTHORITIES),
        "recommended_next_layer":
            next_layer,
        "output_directory":
            str(
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
        "Predecessor contract version: "
        f"{predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION}"
    )
    print(
        "Predecessor replay deterministic: "
        f"{canonical_json(records) == canonical_json(reverse_records)}"
    )
    print(
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(
        "Locator submission records replayed: "
        f"{len(records)}"
    )
    print(
        "Locator submission comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Locator submission status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Locator submission blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Validation principles: "
        f"{len(VALIDATION_PRINCIPLES)}"
    )
    print(
        "Evidence classes: "
        f"{len(EVIDENCE_CLASSES)}"
    )
    print(
        "Validation stages: "
        f"{len(VALIDATION_STAGES)}"
    )
    print(
        "Validation requirements: "
        f"{len(VALIDATION_REQUIREMENTS)}"
    )
    print(
        "Validation record fields: "
        f"{len(VALIDATION_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Source evidence validations materialized: 0")
    print("Source evidence artifacts supplied: 0")
    print("Credentials stored: 0")
    print("Network retrievals executed: 0")
    print("Source evidence acquired: 0")
    print("Historical outcome values acquired: 0")
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Candidate values transformed: 0")
    print("Downstream records recomputed: 0")
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
    print(
        f"Diagnosis: {diagnosis_name}"
    )
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
