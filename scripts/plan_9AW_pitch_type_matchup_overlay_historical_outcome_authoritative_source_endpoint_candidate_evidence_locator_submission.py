#!/usr/bin/env python3
"""
Layer 9AW
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Evidence Locator Submission Plan

Plans the deterministic contract through which an explicit evidence-locator
submission may be provided for an authoritative historical-outcome endpoint
candidate.

Layer 9AV established that no endpoint candidate or evidence-locator submission
exists. This layer defines the submission envelope, lineage, completeness,
cardinality, scope, credential-reference, version, digest, and conflict rules
that a future explicit submission must satisfy.

Planning only.

This layer does not:
- invent or select an endpoint candidate;
- invent or select evidence locators;
- fabricate source authority;
- store credential literals;
- retrieve source evidence or historical outcomes;
- retain raw endpoint responses;
- mutate canonical source values or mappings;
- coerce, infer, default, impute, or substitute values;
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


LAYER_ID = "9AW"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_evidence_locator_submission_plan"
)

PLAN_VERSION = (
    "layer_9AW_historical_outcome_authoritative_source_endpoint_candidate_"
    "evidence_locator_submission_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AW_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_evidence_locator_submission_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "specify_9AV_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_evidence_locator.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AV_historical_outcome_authoritative_source_endpoint_candidate_"
    "evidence_locator_specification_contract_v1"
)

EXPECTED_LOCATOR_SPECIFICATION_RECORDS = 16
EXPECTED_LOCATOR_SPECIFICATION_COMPARISONS = 16

EXPECTED_LOCATOR_SPECIFICATION_STATUS = "candidate_not_supplied"

EXPECTED_LOCATOR_SPECIFICATION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


SUBMISSION_PRINCIPLES = [
    {
        "principle_id": "HOAELSUB-P01",
        "principle": (
            "A locator submission may exist only for an explicitly supplied "
            "endpoint candidate."
        ),
    },
    {
        "principle_id": "HOAELSUB-P02",
        "principle": (
            "Every submission must have a stable submission identifier and "
            "submission version."
        ),
    },
    {
        "principle_id": "HOAELSUB-P03",
        "principle": (
            "Every locator entry must identify exactly one required evidence "
            "class."
        ),
    },
    {
        "principle_id": "HOAELSUB-P04",
        "principle": (
            "The submission must cover all required evidence classes without "
            "duplicate or conflicting entries."
        ),
    },
    {
        "principle_id": "HOAELSUB-P05",
        "principle": (
            "Candidate, comparison, source-owner, and source-class scope must be "
            "explicitly declared."
        ),
    },
    {
        "principle_id": "HOAELSUB-P06",
        "principle": (
            "Credential requirements may contain reference identifiers only and "
            "must never include secret literals."
        ),
    },
    {
        "principle_id": "HOAELSUB-P07",
        "principle": (
            "Immutable or versioned evidence locators must include applicable "
            "version and SHA-256 digest metadata."
        ),
    },
    {
        "principle_id": "HOAELSUB-P08",
        "principle": (
            "Submission approval grants locator-submission implementation "
            "authority only and does not grant evidence retrieval authority."
        ),
    },
]


SUBMISSION_COMPONENTS = [
    {
        "component_id": "HOAELSUB-C01",
        "component": "submission_identity",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOAELSUB-C02",
        "component": "candidate_identity",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOAELSUB-C03",
        "component": "candidate_scope",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOAELSUB-C04",
        "component": "source_authority_scope",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOAELSUB-C05",
        "component": "locator_entries",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOAELSUB-C06",
        "component": "credential_reference_contract",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOAELSUB-C07",
        "component": "version_and_digest_contract",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOAELSUB-C08",
        "component": "submission_attestation",
        "required": True,
        "priority": 8,
    },
]


SUBMISSION_STAGES = [
    {
        "stage_id": "HOAELSUB-S01",
        "stage_name": "locator_specification_replay",
        "priority": 1,
    },
    {
        "stage_id": "HOAELSUB-S02",
        "stage_name": "candidate_presence_gate",
        "priority": 2,
    },
    {
        "stage_id": "HOAELSUB-S03",
        "stage_name": "submission_envelope_inventory",
        "priority": 3,
    },
    {
        "stage_id": "HOAELSUB-S04",
        "stage_name": "submission_identity_validation",
        "priority": 4,
    },
    {
        "stage_id": "HOAELSUB-S05",
        "stage_name": "candidate_and_source_scope_validation",
        "priority": 5,
    },
    {
        "stage_id": "HOAELSUB-S06",
        "stage_name": "required_locator_class_cardinality_validation",
        "priority": 6,
    },
    {
        "stage_id": "HOAELSUB-S07",
        "stage_name": "locator_entry_schema_validation",
        "priority": 7,
    },
    {
        "stage_id": "HOAELSUB-S08",
        "stage_name": "credential_reference_validation",
        "priority": 8,
    },
    {
        "stage_id": "HOAELSUB-S09",
        "stage_name": "version_and_digest_validation",
        "priority": 9,
    },
    {
        "stage_id": "HOAELSUB-S10",
        "stage_name": "duplicate_conflict_and_attestation_validation",
        "priority": 10,
    },
    {
        "stage_id": "HOAELSUB-S11",
        "stage_name": "submission_disposition",
        "priority": 11,
    },
]


SUBMISSION_REQUIREMENTS = [
    {
        "requirement_id": "HOAELSUB-R01",
        "requirement": "candidate_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R02",
        "requirement": "candidate_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R03",
        "requirement": "candidate_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R04",
        "requirement": "locator_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R05",
        "requirement": "locator_submission_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R06",
        "requirement": "locator_submission_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R07",
        "requirement": "submission_created_at_utc_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R08",
        "requirement": "candidate_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R09",
        "requirement": "comparison_scope_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R10",
        "requirement": "source_owner_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R11",
        "requirement": "source_class_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R12",
        "requirement": "authority_locator_entry_count",
        "expected": 1,
    },
    {
        "requirement_id": "HOAELSUB-R13",
        "requirement": "coverage_locator_entry_count",
        "expected": 1,
    },
    {
        "requirement_id": "HOAELSUB-R14",
        "requirement": "identity_semantics_locator_entry_count",
        "expected": 1,
    },
    {
        "requirement_id": "HOAELSUB-R15",
        "requirement": "outcome_semantics_locator_entry_count",
        "expected": 1,
    },
    {
        "requirement_id": "HOAELSUB-R16",
        "requirement": "licensing_locator_entry_count",
        "expected": 1,
    },
    {
        "requirement_id": "HOAELSUB-R17",
        "requirement": "availability_locator_entry_count",
        "expected": 1,
    },
    {
        "requirement_id": "HOAELSUB-R18",
        "requirement": "schema_or_snapshot_locator_entry_count",
        "expected": 1,
    },
    {
        "requirement_id": "HOAELSUB-R19",
        "requirement": "locator_type_eligible",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R20",
        "requirement": "locator_value_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R21",
        "requirement": "secure_transport_or_immutable_path",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R22",
        "requirement": "credential_reference_only",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R23",
        "requirement": "credential_literal_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R24",
        "requirement": "version_present_when_required",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R25",
        "requirement": "immutable_digest_algorithm",
        "expected": "sha256",
    },
    {
        "requirement_id": "HOAELSUB-R26",
        "requirement": "immutable_digest_valid_when_required",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R27",
        "requirement": "duplicate_entries_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R28",
        "requirement": "conflicting_entries_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R29",
        "requirement": "submission_attestation_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSUB-R30",
        "requirement": "network_retrieval_executed",
        "expected": False,
    },
]


SUBMISSION_STATUSES = [
    {
        "status": "evidence_locator_submission_approved",
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
        "status": "submission_identity_incomplete",
        "implementation_authority": False,
    },
    {
        "status": "submission_scope_incomplete",
        "implementation_authority": False,
    },
    {
        "status": "required_locator_class_missing",
        "implementation_authority": False,
    },
    {
        "status": "locator_entry_invalid",
        "implementation_authority": False,
    },
    {
        "status": "credential_contract_invalid",
        "implementation_authority": False,
    },
    {
        "status": "version_or_digest_contract_invalid",
        "implementation_authority": False,
    },
    {
        "status": "submission_conflict_or_attestation_missing",
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
        "code": "historical_outcome_evidence_locator_submission_id_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_evidence_locator_submission_version_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_evidence_locator_submission_timestamp_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_evidence_locator_candidate_scope_missing",
        "category": "scope",
    },
    {
        "code": "historical_outcome_evidence_locator_comparison_scope_missing",
        "category": "scope",
    },
    {
        "code": "historical_outcome_evidence_locator_source_owner_missing",
        "category": "scope",
    },
    {
        "code": "historical_outcome_evidence_locator_source_class_missing",
        "category": "scope",
    },
    {
        "code": "historical_outcome_authority_documentation_locator_missing",
        "category": "authority",
    },
    {
        "code": "historical_outcome_coverage_documentation_locator_missing",
        "category": "coverage",
    },
    {
        "code": "historical_outcome_identity_semantics_locator_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_outcome_semantics_locator_missing",
        "category": "semantics",
    },
    {
        "code": "historical_outcome_licensing_terms_locator_missing",
        "category": "licensing",
    },
    {
        "code": "historical_outcome_availability_documentation_locator_missing",
        "category": "stability",
    },
    {
        "code": "historical_outcome_schema_or_snapshot_locator_missing",
        "category": "versioning",
    },
    {
        "code": "historical_outcome_evidence_locator_entry_type_ineligible",
        "category": "type",
    },
    {
        "code": "historical_outcome_evidence_locator_entry_value_missing",
        "category": "locator",
    },
    {
        "code": "historical_outcome_evidence_locator_transport_invalid",
        "category": "transport",
    },
    {
        "code": "historical_outcome_evidence_locator_credential_reference_missing",
        "category": "security",
    },
    {
        "code": "historical_outcome_evidence_locator_credential_literal_detected",
        "category": "security",
    },
    {
        "code": "historical_outcome_evidence_locator_version_missing",
        "category": "versioning",
    },
    {
        "code": "historical_outcome_evidence_locator_digest_missing",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_evidence_locator_digest_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_evidence_locator_duplicate",
        "category": "conflict",
    },
    {
        "code": "historical_outcome_evidence_locator_conflict",
        "category": "conflict",
    },
    {
        "code": "historical_outcome_evidence_locator_submission_attestation_missing",
        "category": "attestation",
    },
    {
        "code": "historical_outcome_evidence_locator_invention_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_source_evidence_retrieval_requested",
        "category": "authority",
    },
]


SUBMISSION_RECORD_FIELDS = [
    "evidence_locator_submission_plan_contract_version",
    "evidence_locator_submission_plan_record_id",
    "evidence_locator_specification_plan_record_id",
    "evidence_locator_specification_plan_record_digest",
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
    "locator_specification_status",
    "locator_specification_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "source_owner",
    "source_class",
    "locator_submission_supplied",
    "locator_submission_id",
    "locator_submission_version",
    "submission_created_at_utc",
    "candidate_scope",
    "comparison_scope",
    "source_owner_scope",
    "source_class_scope",
    "locator_entries",
    "required_locator_classes",
    "credential_reference_contract",
    "credential_literal_present",
    "version_and_digest_contract",
    "submission_attestation",
    "locator_submission_status",
    "locator_submission_blocker_codes",
    "locator_submission_implementation_authority_granted",
    "locator_submission_rationale",
    "locator_submission_limitations",
    "evidence_locator_submission_plan_identity_digest",
    "evidence_locator_submission_plan_record_digest",
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
        "field": "evidence_locator_specification_plan_record_id",
    },
    {
        "ordinal": 6,
        "field": "evidence_locator_submission_plan_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AV_locator_specification_records",
    },
    {
        "ordinal": 2,
        "step": "preserve_candidate_evidence_and_defect_lineage",
    },
    {
        "ordinal": 3,
        "step": "require_explicit_endpoint_candidate",
    },
    {
        "ordinal": 4,
        "step": "load_explicit_locator_submission_envelopes",
    },
    {
        "ordinal": 5,
        "step": "validate_submission_identity_and_version",
    },
    {
        "ordinal": 6,
        "step": "validate_candidate_and_source_scope",
    },
    {
        "ordinal": 7,
        "step": "validate_exactly_one_entry_per_required_locator_class",
    },
    {
        "ordinal": 8,
        "step": "validate_locator_entry_types_values_and_transport",
    },
    {
        "ordinal": 9,
        "step": "validate_credential_reference_only_contract",
    },
    {
        "ordinal": 10,
        "step": "validate_version_and_sha256_digest_contract",
    },
    {
        "ordinal": 11,
        "step": "validate_duplicate_and_conflicting_entries",
    },
    {
        "ordinal": 12,
        "step": "validate_submission_attestation",
    },
    {
        "ordinal": 13,
        "step": "emit_deterministic_submission_records",
    },
    {
        "ordinal": 14,
        "step": "verify_forward_and_reverse_replay",
    },
    {
        "ordinal": 15,
        "step": "withhold_evidence_retrieval_authority",
    },
    {
        "ordinal": 16,
        "step": "grant_submission_implementation_only_when_complete",
    },
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "endpoint_candidate_selection_without_submission",
    "evidence_locator_invention",
    "evidence_locator_selection_without_submission",
    "locator_submission_invention",
    "locator_submission_completion_by_inference",
    "authority_evidence_fabrication",
    "credential_literal_storage",
    "source_evidence_retrieval_planning",
    "source_evidence_fetch_execution",
    "source_evidence_parse_execution",
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
        "layer_9av_predecessor",
    )

    if (
        predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AV contract version: "
            f"{predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_locator_specification_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_locator_specification_records(
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
        row["locator_specification_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "locator_specification_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_av_contract_version_verified",
            "actual":
                predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_av_replay_deterministic",
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
            "check": "nine_av_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": (
                sha256_payload(records)
                == sha256_payload(reverse_records)
            ),
        },
        {
            "check": "expected_locator_specification_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_LOCATOR_SPECIFICATION_RECORDS,
            "passed": (
                len(records)
                == EXPECTED_LOCATOR_SPECIFICATION_RECORDS
            ),
        },
        {
            "check": "expected_locator_specification_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected":
                EXPECTED_LOCATOR_SPECIFICATION_COMPARISONS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_LOCATOR_SPECIFICATION_COMPARISONS
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(
                sorted(status_counts.items())
            ),
            "expected": {
                EXPECTED_LOCATOR_SPECIFICATION_STATUS:
                    EXPECTED_LOCATOR_SPECIFICATION_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_LOCATOR_SPECIFICATION_STATUS:
                        EXPECTED_LOCATOR_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(
                sorted(blocker_counts.items())
            ),
            "expected": {
                EXPECTED_LOCATOR_SPECIFICATION_BLOCKER:
                    EXPECTED_LOCATOR_SPECIFICATION_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_LOCATOR_SPECIFICATION_BLOCKER:
                        EXPECTED_LOCATOR_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check": "all_locator_specification_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_locator_specification_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_locator_specification_plan_record_digest"
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
            "check": "submission_principles_defined",
            "actual": len(SUBMISSION_PRINCIPLES),
            "expected": 8,
            "passed": len(SUBMISSION_PRINCIPLES) == 8,
        },
        {
            "check": "submission_components_defined",
            "actual": len(SUBMISSION_COMPONENTS),
            "expected": 8,
            "passed": len(SUBMISSION_COMPONENTS) == 8,
        },
        {
            "check": "submission_stages_defined",
            "actual": len(SUBMISSION_STAGES),
            "expected": 11,
            "passed": len(SUBMISSION_STAGES) == 11,
        },
        {
            "check": "submission_requirements_defined",
            "actual": len(SUBMISSION_REQUIREMENTS),
            "expected": 30,
            "passed": len(SUBMISSION_REQUIREMENTS) == 30,
        },
        {
            "check": "submission_statuses_defined",
            "actual": len(SUBMISSION_STATUSES),
            "expected": 10,
            "passed": len(SUBMISSION_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 29,
            "passed": len(BLOCKER_CODES) == 29,
        },
        {
            "check": "submission_record_fields_defined",
            "actual": len(SUBMISSION_RECORD_FIELDS),
            "expected": 47,
            "passed": len(SUBMISSION_RECORD_FIELDS) == 47,
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
            "check": "locator_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "evidence_locator_invention"
                in PROHIBITED_AUTHORITIES
                and
                "evidence_locator_selection_without_submission"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "submission_invention_prohibited",
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
            "check": "locator_submissions_materialized_zero",
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
            "submission_principles":
                SUBMISSION_PRINCIPLES,
            "submission_components":
                SUBMISSION_COMPONENTS,
            "submission_stages":
                SUBMISSION_STAGES,
            "submission_requirements":
                SUBMISSION_REQUIREMENTS,
            "submission_statuses":
                SUBMISSION_STATUSES,
            "blocker_codes":
                BLOCKER_CODES,
            "submission_record_fields":
                SUBMISSION_RECORD_FIELDS,
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
        "endpoint_candidate_evidence_locator_submission_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_submission_plan_failed"
    )

    next_layer = (
        "9AX_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_submission_implementation"
        if all_checks_passed
        else
        "9AW_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_submission_plan_remediation"
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
        OUTPUT_DIR / "submission_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        SUBMISSION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "submission_components.csv",
        [
            "component_id",
            "component",
            "required",
            "priority",
        ],
        SUBMISSION_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "submission_stages.csv",
        [
            "stage_id",
            "stage_name",
            "priority",
        ],
        SUBMISSION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "submission_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        SUBMISSION_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "submission_statuses.csv",
        [
            "status",
            "implementation_authority",
        ],
        SUBMISSION_STATUSES,
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
        OUTPUT_DIR / "submission_record_field_contract.csv",
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
                SUBMISSION_RECORD_FIELDS,
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
        OUTPUT_DIR / "candidate_missing_locator_specification_inventory.csv",
        [
            "evidence_locator_specification_plan_record_id",
            "evidence_locator_specification_plan_record_digest",
            "source_evidence_acquisition_plan_record_id",
            "endpoint_candidate_specification_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "locator_specification_status",
            "locator_specification_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "source_owner",
            "source_class",
            "locator_submission_supplied",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION,
        "locator_specification_records":
            len(records),
        "locator_specification_comparisons":
            len(comparison_ids),
        "locator_specification_status_counts":
            dict(sorted(status_counts.items())),
        "locator_specification_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "submission_principles":
            len(SUBMISSION_PRINCIPLES),
        "submission_components":
            len(SUBMISSION_COMPONENTS),
        "submission_stages":
            len(SUBMISSION_STAGES),
        "submission_requirements":
            len(SUBMISSION_REQUIREMENTS),
        "submission_statuses":
            len(SUBMISSION_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "submission_record_fields":
            len(SUBMISSION_RECORD_FIELDS),
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
        "locator_submissions_materialized": 0,
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
        / "evidence_locator_submission_plan_summary.json",
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
            "evidence_locator_submission_implementation"
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
        f"{predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION}"
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
        "Locator specification records replayed: "
        f"{len(records)}"
    )
    print(
        "Locator specification comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Locator specification status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Locator specification blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Submission principles: "
        f"{len(SUBMISSION_PRINCIPLES)}"
    )
    print(
        "Submission components: "
        f"{len(SUBMISSION_COMPONENTS)}"
    )
    print(
        "Submission stages: "
        f"{len(SUBMISSION_STAGES)}"
    )
    print(
        "Submission requirements: "
        f"{len(SUBMISSION_REQUIREMENTS)}"
    )
    print(
        "Submission record fields: "
        f"{len(SUBMISSION_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Locator submissions materialized: 0")
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
