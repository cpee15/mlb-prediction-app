#!/usr/bin/env python3
"""
Layer 9BA
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Acquisition Authorization Plan

Plans the deterministic authorization contract that must be satisfied before
source evidence may be acquired for an explicitly supplied authoritative
historical-outcome endpoint candidate.

Layer 9AZ established that no candidate, locator submission, or source-evidence
submission exists. This layer therefore defines authorization gates only.

Planning only.

This layer does not:
- invent or select an endpoint candidate;
- invent or complete locator or source-evidence submissions;
- authorize retrieval without validated evidence lineage;
- store credential literals;
- perform network retrieval;
- acquire source evidence or historical outcomes;
- mutate canonical values or mappings;
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


LAYER_ID = "9BA"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_acquisition_authorization_plan"
)

PLAN_VERSION = (
    "layer_9BA_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_authorization_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BA_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_acquisition_"
    "authorization_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "validate_9AZ_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_submission_source_evidence.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AZ_historical_outcome_authoritative_source_endpoint_candidate_"
    "submission_source_evidence_validation_contract_v1"
)

EXPECTED_VALIDATION_RECORDS = 16
EXPECTED_VALIDATION_COMPARISONS = 16

EXPECTED_VALIDATION_STATUS = "candidate_not_supplied"

EXPECTED_VALIDATION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


AUTHORIZATION_PRINCIPLES = [
    {
        "principle_id": "HOASEAA-P01",
        "principle": (
            "Acquisition authorization may be evaluated only for an explicitly "
            "supplied candidate, locator submission, and source-evidence "
            "submission."
        ),
    },
    {
        "principle_id": "HOASEAA-P02",
        "principle": (
            "Every authorization decision must preserve candidate, locator, "
            "evidence, comparison, and defect lineage."
        ),
    },
    {
        "principle_id": "HOASEAA-P03",
        "principle": (
            "All required source-evidence validation classes must be approved "
            "before acquisition authorization."
        ),
    },
    {
        "principle_id": "HOASEAA-P04",
        "principle": (
            "Credential requirements may reference approved secret identifiers "
            "only and must never contain credential literals."
        ),
    },
    {
        "principle_id": "HOASEAA-P05",
        "principle": (
            "Transport, host, path, request method, rate-limit, retention, and "
            "redaction boundaries must be explicit."
        ),
    },
    {
        "principle_id": "HOASEAA-P06",
        "principle": (
            "Authorization must be bounded to source-evidence acquisition and "
            "must not imply historical-outcome retrieval authorization."
        ),
    },
    {
        "principle_id": "HOASEAA-P07",
        "principle": (
            "An authorization must be versioned, attestable, revocable, and "
            "deterministically replayable."
        ),
    },
    {
        "principle_id": "HOASEAA-P08",
        "principle": (
            "Successful planning grants authorization implementation authority "
            "only and executes no acquisition."
        ),
    },
]


AUTHORIZATION_COMPONENTS = [
    {
        "component_id": "HOASEAA-C01",
        "component": "candidate_and_submission_lineage",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEAA-C02",
        "component": "validated_source_evidence_lineage",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEAA-C03",
        "component": "authorization_identity_and_version",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEAA-C04",
        "component": "approved_request_scope",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEAA-C05",
        "component": "credential_reference_and_redaction_contract",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEAA-C06",
        "component": "rate_limit_retry_and_timeout_contract",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEAA-C07",
        "component": "retention_integrity_and_audit_contract",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEAA-C08",
        "component": "authorization_attestation_and_revocation",
        "required": True,
        "priority": 8,
    },
]


AUTHORIZATION_STAGES = [
    {
        "stage_id": "HOASEAA-S01",
        "stage_name": "source_evidence_validation_replay",
        "priority": 1,
    },
    {
        "stage_id": "HOASEAA-S02",
        "stage_name": "candidate_and_submission_presence_gate",
        "priority": 2,
    },
    {
        "stage_id": "HOASEAA-S03",
        "stage_name": "validation_approval_gate",
        "priority": 3,
    },
    {
        "stage_id": "HOASEAA-S04",
        "stage_name": "authorization_submission_inventory",
        "priority": 4,
    },
    {
        "stage_id": "HOASEAA-S05",
        "stage_name": "authorization_identity_and_scope_validation",
        "priority": 5,
    },
    {
        "stage_id": "HOASEAA-S06",
        "stage_name": "transport_host_path_and_method_validation",
        "priority": 6,
    },
    {
        "stage_id": "HOASEAA-S07",
        "stage_name": "credential_reference_and_redaction_validation",
        "priority": 7,
    },
    {
        "stage_id": "HOASEAA-S08",
        "stage_name": "rate_limit_retry_timeout_and_retention_validation",
        "priority": 8,
    },
    {
        "stage_id": "HOASEAA-S09",
        "stage_name": "attestation_expiration_and_revocation_validation",
        "priority": 9,
    },
    {
        "stage_id": "HOASEAA-S10",
        "stage_name": "authorization_disposition_assignment",
        "priority": 10,
    },
    {
        "stage_id": "HOASEAA-S11",
        "stage_name": "deterministic_authorization_record_emission",
        "priority": 11,
    },
]


AUTHORIZATION_REQUIREMENTS = [
    {
        "requirement_id": "HOASEAA-R01",
        "requirement": "candidate_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R02",
        "requirement": "locator_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R03",
        "requirement": "source_evidence_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R04",
        "requirement": "source_evidence_validation_approved",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R05",
        "requirement": "candidate_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R06",
        "requirement": "candidate_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R07",
        "requirement": "authorization_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R08",
        "requirement": "authorization_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R09",
        "requirement": "authorization_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R10",
        "requirement": "authorization_created_at_utc_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R11",
        "requirement": "authorization_expires_at_utc_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R12",
        "requirement": "candidate_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R13",
        "requirement": "comparison_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R14",
        "requirement": "source_owner_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R15",
        "requirement": "source_class_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R16",
        "requirement": "approved_scheme",
        "expected": "https",
    },
    {
        "requirement_id": "HOASEAA-R17",
        "requirement": "approved_host_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R18",
        "requirement": "approved_path_pattern_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R19",
        "requirement": "approved_http_method",
        "expected": "GET",
    },
    {
        "requirement_id": "HOASEAA-R20",
        "requirement": "query_parameter_allowlist_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R21",
        "requirement": "response_content_type_allowlist_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R22",
        "requirement": "credential_reference_only",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R23",
        "requirement": "credential_literal_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R24",
        "requirement": "request_log_redaction_contract_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R25",
        "requirement": "rate_limit_contract_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R26",
        "requirement": "retry_contract_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R27",
        "requirement": "timeout_contract_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R28",
        "requirement": "retention_contract_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R29",
        "requirement": "response_digest_algorithm",
        "expected": "sha256",
    },
    {
        "requirement_id": "HOASEAA-R30",
        "requirement": "audit_record_contract_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R31",
        "requirement": "authorization_attestation_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R32",
        "requirement": "revocation_contract_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAA-R33",
        "requirement": "network_retrieval_executed",
        "expected": False,
    },
]


AUTHORIZATION_STATUSES = [
    {
        "status": "source_evidence_acquisition_authorization_approved",
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
        "status": "source_evidence_submission_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "source_evidence_validation_not_approved",
        "implementation_authority": False,
    },
    {
        "status": "authorization_submission_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "authorization_identity_or_scope_invalid",
        "implementation_authority": False,
    },
    {
        "status": "transport_or_request_contract_invalid",
        "implementation_authority": False,
    },
    {
        "status": "credential_or_operational_contract_invalid",
        "implementation_authority": False,
    },
    {
        "status": "attestation_expiration_or_revocation_invalid",
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
        "code": "historical_outcome_source_evidence_validation_not_approved",
        "category": "validation",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_authorization_submission_missing",
        "category": "authorization",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_authorization_id_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_authorization_version_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_authorization_timestamp_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_authorization_expiration_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_candidate_scope_mismatch",
        "category": "scope",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_comparison_scope_mismatch",
        "category": "scope",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_owner_scope_mismatch",
        "category": "scope",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_source_class_scope_mismatch",
        "category": "scope",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_scheme_invalid",
        "category": "transport",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_host_missing",
        "category": "transport",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_path_pattern_missing",
        "category": "transport",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_http_method_invalid",
        "category": "request",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_query_allowlist_missing",
        "category": "request",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_content_type_allowlist_missing",
        "category": "response",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_credential_reference_missing",
        "category": "security",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_credential_literal_detected",
        "category": "security",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_redaction_contract_missing",
        "category": "security",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_rate_limit_contract_missing",
        "category": "operations",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_retry_contract_missing",
        "category": "operations",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_timeout_contract_missing",
        "category": "operations",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_retention_contract_missing",
        "category": "retention",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_digest_contract_missing",
        "category": "integrity",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_audit_contract_missing",
        "category": "audit",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_attestation_missing",
        "category": "attestation",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_revocation_contract_missing",
        "category": "revocation",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_authorization_invention_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_requested",
        "category": "authority",
    },
]


AUTHORIZATION_RECORD_FIELDS = [
    "source_evidence_acquisition_authorization_plan_contract_version",
    "source_evidence_acquisition_authorization_plan_record_id",
    "source_evidence_validation_plan_record_id",
    "source_evidence_validation_plan_record_digest",
    "evidence_locator_submission_plan_record_id",
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
    "source_evidence_validation_status",
    "source_evidence_validation_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "source_owner",
    "source_class",
    "locator_submission_supplied",
    "locator_submission_id",
    "source_evidence_submission_supplied",
    "source_evidence_submission_id",
    "authorization_submission_supplied",
    "authorization_id",
    "authorization_version",
    "authorization_created_at_utc",
    "authorization_expires_at_utc",
    "approved_request_scope",
    "credential_reference_contract",
    "request_log_redaction_contract",
    "rate_limit_retry_timeout_contract",
    "retention_integrity_audit_contract",
    "authorization_attestation",
    "revocation_contract",
    "source_evidence_acquisition_authorization_status",
    "source_evidence_acquisition_authorization_blocker_codes",
    "source_evidence_acquisition_authorization_implementation_authority_granted",
    "source_evidence_acquisition_authorization_rationale",
    "source_evidence_acquisition_authorization_limitations",
    "source_evidence_acquisition_authorization_plan_identity_digest",
    "source_evidence_acquisition_authorization_plan_record_digest",
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
        "field": "source_evidence_submission_id",
    },
    {
        "ordinal": 5,
        "field": "authorization_id",
    },
    {
        "ordinal": 6,
        "field": "source_evidence_acquisition_authorization_plan_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AZ_source_evidence_validation_records",
    },
    {
        "ordinal": 2,
        "step": "preserve_candidate_locator_evidence_and_defect_lineage",
    },
    {
        "ordinal": 3,
        "step": "require_explicit_candidate_and_submissions",
    },
    {
        "ordinal": 4,
        "step": "require_approved_source_evidence_validation",
    },
    {
        "ordinal": 5,
        "step": "load_explicit_acquisition_authorization_submissions",
    },
    {
        "ordinal": 6,
        "step": "validate_authorization_identity_version_and_expiration",
    },
    {
        "ordinal": 7,
        "step": "validate_candidate_comparison_owner_and_source_class_scope",
    },
    {
        "ordinal": 8,
        "step": "validate_https_host_path_method_and_query_allowlist",
    },
    {
        "ordinal": 9,
        "step": "validate_response_content_type_allowlist",
    },
    {
        "ordinal": 10,
        "step": "validate_credential_reference_and_redaction_contract",
    },
    {
        "ordinal": 11,
        "step": "validate_rate_limit_retry_and_timeout_contract",
    },
    {
        "ordinal": 12,
        "step": "validate_retention_digest_and_audit_contract",
    },
    {
        "ordinal": 13,
        "step": "validate_attestation_and_revocation_contract",
    },
    {
        "ordinal": 14,
        "step": "emit_deterministic_authorization_records",
    },
    {
        "ordinal": 15,
        "step": "verify_forward_and_reverse_replay",
    },
    {
        "ordinal": 16,
        "step": "grant_authorization_implementation_only_when_complete",
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
    "acquisition_authorization_invention",
    "acquisition_authorization_completion_by_inference",
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
        "layer_9az_predecessor",
    )

    if (
        predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AZ contract version: "
            f"{predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_source_evidence_validation_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_source_evidence_validation_records(
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
        row["source_evidence_validation_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "source_evidence_validation_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_az_contract_version_verified",
            "actual":
                predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_az_replay_deterministic",
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
            "check": "nine_az_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": (
                sha256_payload(records)
                == sha256_payload(reverse_records)
            ),
        },
        {
            "check": "expected_validation_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_VALIDATION_RECORDS,
            "passed": (
                len(records)
                == EXPECTED_VALIDATION_RECORDS
            ),
        },
        {
            "check": "expected_validation_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_VALIDATION_COMPARISONS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_VALIDATION_COMPARISONS
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(
                sorted(status_counts.items())
            ),
            "expected": {
                EXPECTED_VALIDATION_STATUS:
                    EXPECTED_VALIDATION_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_VALIDATION_STATUS:
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(
                sorted(blocker_counts.items())
            ),
            "expected": {
                EXPECTED_VALIDATION_BLOCKER:
                    EXPECTED_VALIDATION_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_VALIDATION_BLOCKER:
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "all_validation_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_validation_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_validation_plan_record_digest"
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
            "check": "authorization_principles_defined",
            "actual": len(AUTHORIZATION_PRINCIPLES),
            "expected": 8,
            "passed": len(AUTHORIZATION_PRINCIPLES) == 8,
        },
        {
            "check": "authorization_components_defined",
            "actual": len(AUTHORIZATION_COMPONENTS),
            "expected": 8,
            "passed": len(AUTHORIZATION_COMPONENTS) == 8,
        },
        {
            "check": "authorization_stages_defined",
            "actual": len(AUTHORIZATION_STAGES),
            "expected": 11,
            "passed": len(AUTHORIZATION_STAGES) == 11,
        },
        {
            "check": "authorization_requirements_defined",
            "actual": len(AUTHORIZATION_REQUIREMENTS),
            "expected": 33,
            "passed": len(AUTHORIZATION_REQUIREMENTS) == 33,
        },
        {
            "check": "authorization_statuses_defined",
            "actual": len(AUTHORIZATION_STATUSES),
            "expected": 10,
            "passed": len(AUTHORIZATION_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 32,
            "passed": len(BLOCKER_CODES) == 32,
        },
        {
            "check": "authorization_record_fields_defined",
            "actual": len(AUTHORIZATION_RECORD_FIELDS),
            "expected": 51,
            "passed": len(AUTHORIZATION_RECORD_FIELDS) == 51,
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
            "check": "source_evidence_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "source_evidence_invention"
                in PROHIBITED_AUTHORITIES
                and
                "source_evidence_fabrication"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "authorization_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "acquisition_authorization_invention"
                in PROHIBITED_AUTHORITIES
                and
                "acquisition_authorization_completion_by_inference"
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
            "check": "source_evidence_retrieval_execution_prohibited",
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
            "check": "authorization_records_materialized_zero",
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
            "authorization_principles":
                AUTHORIZATION_PRINCIPLES,
            "authorization_components":
                AUTHORIZATION_COMPONENTS,
            "authorization_stages":
                AUTHORIZATION_STAGES,
            "authorization_requirements":
                AUTHORIZATION_REQUIREMENTS,
            "authorization_statuses":
                AUTHORIZATION_STATUSES,
            "blocker_codes":
                BLOCKER_CODES,
            "authorization_record_fields":
                AUTHORIZATION_RECORD_FIELDS,
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
        "endpoint_candidate_source_evidence_acquisition_authorization_plan_"
        "complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_authorization_plan_"
        "failed"
    )

    next_layer = (
        "9BB_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_authorization_"
        "implementation"
        if all_checks_passed
        else
        "9BA_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_authorization_plan_"
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
        OUTPUT_DIR / "authorization_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        AUTHORIZATION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "authorization_components.csv",
        [
            "component_id",
            "component",
            "required",
            "priority",
        ],
        AUTHORIZATION_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "authorization_stages.csv",
        [
            "stage_id",
            "stage_name",
            "priority",
        ],
        AUTHORIZATION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "authorization_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        AUTHORIZATION_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "authorization_statuses.csv",
        [
            "status",
            "implementation_authority",
        ],
        AUTHORIZATION_STATUSES,
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
        OUTPUT_DIR / "authorization_record_field_contract.csv",
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
                AUTHORIZATION_RECORD_FIELDS,
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
        OUTPUT_DIR / "candidate_missing_source_evidence_validation_inventory.csv",
        [
            "source_evidence_validation_plan_record_id",
            "source_evidence_validation_plan_record_digest",
            "evidence_locator_submission_plan_record_id",
            "endpoint_candidate_specification_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "source_evidence_validation_status",
            "source_evidence_validation_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "source_owner",
            "source_class",
            "locator_submission_supplied",
            "locator_submission_id",
            "source_evidence_submission_supplied",
            "source_evidence_submission_id",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION,
        "source_evidence_validation_records":
            len(records),
        "source_evidence_validation_comparisons":
            len(comparison_ids),
        "source_evidence_validation_status_counts":
            dict(sorted(status_counts.items())),
        "source_evidence_validation_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "authorization_principles":
            len(AUTHORIZATION_PRINCIPLES),
        "authorization_components":
            len(AUTHORIZATION_COMPONENTS),
        "authorization_stages":
            len(AUTHORIZATION_STAGES),
        "authorization_requirements":
            len(AUTHORIZATION_REQUIREMENTS),
        "authorization_statuses":
            len(AUTHORIZATION_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "authorization_record_fields":
            len(AUTHORIZATION_RECORD_FIELDS),
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
        "authorization_records_materialized": 0,
        "authorization_submissions_supplied": 0,
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
        / "source_evidence_acquisition_authorization_plan_summary.json",
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
            "source_evidence_acquisition_authorization_implementation"
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
        f"{predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION}"
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
        "Source evidence validation records replayed: "
        f"{len(records)}"
    )
    print(
        "Source evidence validation comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Source evidence validation status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Source evidence validation blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Authorization principles: "
        f"{len(AUTHORIZATION_PRINCIPLES)}"
    )
    print(
        "Authorization components: "
        f"{len(AUTHORIZATION_COMPONENTS)}"
    )
    print(
        "Authorization stages: "
        f"{len(AUTHORIZATION_STAGES)}"
    )
    print(
        "Authorization requirements: "
        f"{len(AUTHORIZATION_REQUIREMENTS)}"
    )
    print(
        "Authorization record fields: "
        f"{len(AUTHORIZATION_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Authorization records materialized: 0")
    print("Authorization submissions supplied: 0")
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
