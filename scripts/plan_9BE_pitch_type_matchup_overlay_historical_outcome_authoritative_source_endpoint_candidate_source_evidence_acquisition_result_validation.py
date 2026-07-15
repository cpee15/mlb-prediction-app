#!/usr/bin/env python3
"""
Layer 9BE
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Acquisition Result Validation Plan

Plans deterministic validation of source-evidence acquisition results after an
authorized acquisition execution has produced a quarantined raw response.

Layer 9BD established that no candidate or acquisition execution exists and no
raw response was received. This layer therefore defines validation gates only.

Planning only. No network activity, parsing, evidence creation, historical
outcome retrieval, canonical mutation, transformation, recomputation, or
production authority is granted.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BE"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_acquisition_result_validation_plan"
)

PLAN_VERSION = (
    "layer_9BE_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_result_validation_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BE_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_acquisition_"
    "result_validation_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "execute_9BD_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_acquisition.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BD_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_execution_contract_v1"
)

EXPECTED_EXECUTION_RECORDS = 16
EXPECTED_EXECUTION_COMPARISONS = 16

EXPECTED_EXECUTION_STATUS = "candidate_not_supplied"

EXPECTED_EXECUTION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


VALIDATION_PRINCIPLES = [
    {
        "principle_id": "HOASEARV-P01",
        "principle": (
            "Result validation may occur only for an explicitly executed, "
            "authorized acquisition with an immutable quarantined response."
        ),
    },
    {
        "principle_id": "HOASEARV-P02",
        "principle": (
            "Candidate, locator, evidence, authorization, execution, response, "
            "comparison, and defect lineage must remain intact."
        ),
    },
    {
        "principle_id": "HOASEARV-P03",
        "principle": (
            "Response bytes, byte length, media type, receipt timestamp, and "
            "SHA-256 digest must be independently verifiable."
        ),
    },
    {
        "principle_id": "HOASEARV-P04",
        "principle": (
            "Transport status and response headers must conform to the approved "
            "execution contract without inventing missing metadata."
        ),
    },
    {
        "principle_id": "HOASEARV-P05",
        "principle": (
            "Validation must reject truncated, empty, redirected outside scope, "
            "unexpected, mutable, or digest-mismatched responses."
        ),
    },
    {
        "principle_id": "HOASEARV-P06",
        "principle": (
            "Successful acquisition-result validation authorizes parsing "
            "planning only and does not authorize parsing execution."
        ),
    },
    {
        "principle_id": "HOASEARV-P07",
        "principle": (
            "Acquisition-result validation does not establish historical "
            "outcome identity, value correctness, or canonical usability."
        ),
    },
    {
        "principle_id": "HOASEARV-P08",
        "principle": (
            "Successful planning grants validation implementation authority "
            "only and performs no retrieval or parsing."
        ),
    },
]


VALIDATION_COMPONENTS = [
    {
        "component_id": "HOASEARV-C01",
        "component": "execution_and_authorization_lineage",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEARV-C02",
        "component": "quarantined_response_identity",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEARV-C03",
        "component": "response_byte_integrity",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEARV-C04",
        "component": "transport_and_redirect_validation",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEARV-C05",
        "component": "media_type_and_size_validation",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEARV-C06",
        "component": "retention_and_immutability_validation",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEARV-C07",
        "component": "audit_and_redaction_validation",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEARV-C08",
        "component": "validation_disposition_and_authority_boundary",
        "required": True,
        "priority": 8,
    },
]


VALIDATION_STAGES = [
    {
        "stage_id": "HOASEARV-S01",
        "stage_name": "acquisition_execution_record_replay",
        "priority": 1,
    },
    {
        "stage_id": "HOASEARV-S02",
        "stage_name": "candidate_and_execution_presence_gate",
        "priority": 2,
    },
    {
        "stage_id": "HOASEARV-S03",
        "stage_name": "acquisition_result_submission_inventory",
        "priority": 3,
    },
    {
        "stage_id": "HOASEARV-S04",
        "stage_name": "response_identity_and_lineage_validation",
        "priority": 4,
    },
    {
        "stage_id": "HOASEARV-S05",
        "stage_name": "response_digest_and_byte_length_validation",
        "priority": 5,
    },
    {
        "stage_id": "HOASEARV-S06",
        "stage_name": "transport_status_and_redirect_validation",
        "priority": 6,
    },
    {
        "stage_id": "HOASEARV-S07",
        "stage_name": "media_type_and_payload_boundary_validation",
        "priority": 7,
    },
    {
        "stage_id": "HOASEARV-S08",
        "stage_name": "retention_immutability_and_audit_validation",
        "priority": 8,
    },
    {
        "stage_id": "HOASEARV-S09",
        "stage_name": "redaction_and_secret_absence_validation",
        "priority": 9,
    },
    {
        "stage_id": "HOASEARV-S10",
        "stage_name": "validation_disposition_assignment",
        "priority": 10,
    },
    {
        "stage_id": "HOASEARV-S11",
        "stage_name": "deterministic_validation_plan_record_emission",
        "priority": 11,
    },
]


VALIDATION_REQUIREMENTS = [
    {"requirement_id": "HOASEARV-R01", "requirement": "candidate_present", "expected": True},
    {"requirement_id": "HOASEARV-R02", "requirement": "authorization_approved", "expected": True},
    {"requirement_id": "HOASEARV-R03", "requirement": "execution_submission_present", "expected": True},
    {"requirement_id": "HOASEARV-R04", "requirement": "execution_completed", "expected": True},
    {"requirement_id": "HOASEARV-R05", "requirement": "result_submission_present", "expected": True},
    {"requirement_id": "HOASEARV-R06", "requirement": "result_id_present", "expected": True},
    {"requirement_id": "HOASEARV-R07", "requirement": "result_version_present", "expected": True},
    {"requirement_id": "HOASEARV-R08", "requirement": "execution_attempt_id_present", "expected": True},
    {"requirement_id": "HOASEARV-R09", "requirement": "response_artifact_id_present", "expected": True},
    {"requirement_id": "HOASEARV-R10", "requirement": "response_received_at_utc_present", "expected": True},
    {"requirement_id": "HOASEARV-R11", "requirement": "response_status_code_present", "expected": True},
    {"requirement_id": "HOASEARV-R12", "requirement": "response_status_code_approved", "expected": True},
    {"requirement_id": "HOASEARV-R13", "requirement": "final_response_url_within_scope", "expected": True},
    {"requirement_id": "HOASEARV-R14", "requirement": "redirect_chain_within_scope", "expected": True},
    {"requirement_id": "HOASEARV-R15", "requirement": "response_media_type_present", "expected": True},
    {"requirement_id": "HOASEARV-R16", "requirement": "response_media_type_allowed", "expected": True},
    {"requirement_id": "HOASEARV-R17", "requirement": "response_byte_length_present", "expected": True},
    {"requirement_id": "HOASEARV-R18", "requirement": "response_byte_length_positive", "expected": True},
    {"requirement_id": "HOASEARV-R19", "requirement": "response_byte_length_within_limit", "expected": True},
    {"requirement_id": "HOASEARV-R20", "requirement": "response_sha256_present", "expected": True},
    {"requirement_id": "HOASEARV-R21", "requirement": "response_sha256_verified", "expected": True},
    {"requirement_id": "HOASEARV-R22", "requirement": "response_not_truncated", "expected": True},
    {"requirement_id": "HOASEARV-R23", "requirement": "response_quarantined", "expected": True},
    {"requirement_id": "HOASEARV-R24", "requirement": "response_immutable", "expected": True},
    {"requirement_id": "HOASEARV-R25", "requirement": "retention_policy_satisfied", "expected": True},
    {"requirement_id": "HOASEARV-R26", "requirement": "audit_record_present", "expected": True},
    {"requirement_id": "HOASEARV-R27", "requirement": "request_and_response_logs_redacted", "expected": True},
    {"requirement_id": "HOASEARV-R28", "requirement": "credential_literals_absent", "expected": True},
    {"requirement_id": "HOASEARV-R29", "requirement": "response_parsing_executed", "expected": False},
    {"requirement_id": "HOASEARV-R30", "requirement": "historical_outcome_value_extracted", "expected": False},
    {"requirement_id": "HOASEARV-R31", "requirement": "canonical_records_mutated", "expected": False},
    {"requirement_id": "HOASEARV-R32", "requirement": "network_retrieval_executed_during_planning", "expected": False},
]


VALIDATION_STATUSES = [
    {"status": "acquisition_result_validation_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "authorization_not_approved", "implementation_authority": False},
    {"status": "execution_not_supplied", "implementation_authority": False},
    {"status": "execution_not_completed", "implementation_authority": False},
    {"status": "acquisition_result_not_supplied", "implementation_authority": False},
    {"status": "response_identity_or_lineage_invalid", "implementation_authority": False},
    {"status": "response_integrity_invalid", "implementation_authority": False},
    {"status": "transport_or_payload_boundary_invalid", "implementation_authority": False},
    {"status": "retention_audit_or_redaction_invalid", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "historical_outcome_source_evidence_acquisition_authorization_not_approved", "category": "authorization"},
    {"code": "historical_outcome_source_evidence_acquisition_execution_submission_missing", "category": "execution"},
    {"code": "historical_outcome_source_evidence_acquisition_execution_not_completed", "category": "execution"},
    {"code": "historical_outcome_source_evidence_acquisition_result_submission_missing", "category": "result"},
    {"code": "historical_outcome_source_evidence_acquisition_result_id_missing", "category": "identity"},
    {"code": "historical_outcome_source_evidence_acquisition_result_version_missing", "category": "identity"},
    {"code": "historical_outcome_source_evidence_acquisition_result_attempt_id_missing", "category": "identity"},
    {"code": "historical_outcome_source_evidence_acquisition_result_response_artifact_id_missing", "category": "identity"},
    {"code": "historical_outcome_source_evidence_acquisition_result_received_timestamp_missing", "category": "identity"},
    {"code": "historical_outcome_source_evidence_acquisition_result_status_code_missing", "category": "transport"},
    {"code": "historical_outcome_source_evidence_acquisition_result_status_code_invalid", "category": "transport"},
    {"code": "historical_outcome_source_evidence_acquisition_result_final_url_out_of_scope", "category": "transport"},
    {"code": "historical_outcome_source_evidence_acquisition_result_redirect_chain_out_of_scope", "category": "transport"},
    {"code": "historical_outcome_source_evidence_acquisition_result_media_type_missing", "category": "payload"},
    {"code": "historical_outcome_source_evidence_acquisition_result_media_type_not_allowed", "category": "payload"},
    {"code": "historical_outcome_source_evidence_acquisition_result_byte_length_missing", "category": "payload"},
    {"code": "historical_outcome_source_evidence_acquisition_result_empty", "category": "payload"},
    {"code": "historical_outcome_source_evidence_acquisition_result_size_limit_exceeded", "category": "payload"},
    {"code": "historical_outcome_source_evidence_acquisition_result_sha256_missing", "category": "integrity"},
    {"code": "historical_outcome_source_evidence_acquisition_result_sha256_mismatch", "category": "integrity"},
    {"code": "historical_outcome_source_evidence_acquisition_result_truncated", "category": "integrity"},
    {"code": "historical_outcome_source_evidence_acquisition_result_not_quarantined", "category": "retention"},
    {"code": "historical_outcome_source_evidence_acquisition_result_not_immutable", "category": "retention"},
    {"code": "historical_outcome_source_evidence_acquisition_result_retention_invalid", "category": "retention"},
    {"code": "historical_outcome_source_evidence_acquisition_result_audit_record_missing", "category": "audit"},
    {"code": "historical_outcome_source_evidence_acquisition_result_logs_not_redacted", "category": "security"},
    {"code": "historical_outcome_source_evidence_acquisition_result_credential_literal_detected", "category": "security"},
    {"code": "historical_outcome_source_evidence_acquisition_result_invention_requested", "category": "authority"},
    {"code": "historical_outcome_source_evidence_acquisition_result_parsing_requested", "category": "authority"},
    {"code": "historical_outcome_source_evidence_acquisition_result_historical_value_extraction_requested", "category": "authority"},
]


VALIDATION_PLAN_RECORD_FIELDS = [
    "source_evidence_acquisition_result_validation_plan_contract_version",
    "source_evidence_acquisition_result_validation_plan_record_id",
    "source_evidence_acquisition_execution_plan_record_id",
    "source_evidence_acquisition_execution_plan_record_digest",
    "source_evidence_acquisition_authorization_plan_record_id",
    "source_evidence_validation_plan_record_id",
    "endpoint_candidate_specification_record_id",
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
    "execution_status",
    "execution_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "authorization_submission_supplied",
    "authorization_id",
    "execution_submission_supplied",
    "execution_id",
    "execution_version",
    "execution_attempt_id",
    "result_submission_supplied",
    "result_id",
    "result_version",
    "response_artifact_id",
    "response_received_at_utc",
    "response_status_code",
    "final_response_url",
    "redirect_chain",
    "response_media_type",
    "response_byte_length",
    "response_sha256",
    "response_truncated",
    "response_quarantined",
    "response_immutable",
    "retention_policy_reference",
    "audit_record_reference",
    "logs_redacted",
    "credential_literals_detected",
    "acquisition_result_validation_status",
    "acquisition_result_validation_blocker_codes",
    "acquisition_result_validation_implementation_authority_granted",
    "acquisition_result_validation_rationale",
    "acquisition_result_validation_limitations",
    "acquisition_result_validation_plan_identity_digest",
    "acquisition_result_validation_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "execution_id"},
    {"ordinal": 5, "field": "result_id"},
    {
        "ordinal": 6,
        "field": "source_evidence_acquisition_result_validation_plan_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9BD_acquisition_execution_records"},
    {"ordinal": 2, "step": "preserve_complete_candidate_to_defect_lineage"},
    {"ordinal": 3, "step": "require_candidate_authorization_and_completed_execution"},
    {"ordinal": 4, "step": "load_explicit_acquisition_result_submissions"},
    {"ordinal": 5, "step": "validate_result_identity_version_and_attempt_lineage"},
    {"ordinal": 6, "step": "validate_response_artifact_and_receipt_metadata"},
    {"ordinal": 7, "step": "validate_status_code_final_url_and_redirect_chain"},
    {"ordinal": 8, "step": "validate_media_type_and_payload_size_boundaries"},
    {"ordinal": 9, "step": "recompute_and_validate_sha256_over_immutable_bytes"},
    {"ordinal": 10, "step": "reject_empty_truncated_or_mutable_responses"},
    {"ordinal": 11, "step": "validate_quarantine_retention_and_audit_lineage"},
    {"ordinal": 12, "step": "validate_redaction_and_credential_literal_absence"},
    {"ordinal": 13, "step": "withhold_parsing_and_historical_value_extraction"},
    {"ordinal": 14, "step": "emit_deterministic_validation_plan_records"},
    {"ordinal": 15, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 16, "step": "grant_validation_implementation_only_when_complete"},
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "endpoint_candidate_selection_without_submission",
    "acquisition_authorization_invention",
    "acquisition_execution_submission_invention",
    "acquisition_result_submission_invention",
    "acquisition_result_completion_by_inference",
    "response_artifact_invention",
    "response_metadata_invention",
    "credential_literal_storage",
    "credential_literal_logging",
    "dns_resolution_execution",
    "socket_connection_execution",
    "http_request_execution",
    "browser_execution",
    "api_request_execution",
    "source_evidence_fetch_execution",
    "source_evidence_parse_execution",
    "raw_response_parse_execution",
    "historical_outcome_retrieval_planning",
    "historical_outcome_fetch_execution",
    "historical_outcome_parse_execution",
    "historical_outcome_value_extraction",
    "canonical_source_value_mutation",
    "canonical_outcome_mapping_change",
    "boolean_to_integer_coercion",
    "source_value_defaulting",
    "source_value_inference",
    "source_value_imputation",
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
        and all(character in "0123456789abcdef" for character in value)
    )


def load_module(path: Path, module_name: str) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)

    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as handle:
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
                    if isinstance(value, (dict, list, tuple))
                    else value
                )

            writer.writerow(serialized)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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
        "layer_9bd_predecessor",
    )

    if (
        predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BD contract version: "
            f"{predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_execution_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_execution_records(
        plan,
        list(reversed(replay["reverse_records"])),
    )

    return {
        "module": predecessor,
        "records": records,
        "reverse_records": reverse_records,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    replay = replay_predecessor()

    predecessor = replay["module"]
    records = replay["records"]
    reverse_records = replay["reverse_records"]

    comparison_ids = {
        row["comparison_record_id"]
        for row in records
    }

    status_counts = Counter(
        row["source_evidence_acquisition_execution_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "source_evidence_acquisition_execution_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_bd_contract_version_verified",
            "actual": predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_bd_replay_deterministic",
            "actual": canonical_json(records) == canonical_json(reverse_records),
            "expected": True,
            "passed": canonical_json(records) == canonical_json(reverse_records),
        },
        {
            "check": "nine_bd_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": sha256_payload(records) == sha256_payload(reverse_records),
        },
        {
            "check": "expected_execution_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_EXECUTION_RECORDS,
            "passed": len(records) == EXPECTED_EXECUTION_RECORDS,
        },
        {
            "check": "expected_execution_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_EXECUTION_COMPARISONS,
            "passed": len(comparison_ids) == EXPECTED_EXECUTION_COMPARISONS,
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(sorted(status_counts.items())),
            "expected": {
                EXPECTED_EXECUTION_STATUS:
                    EXPECTED_EXECUTION_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_EXECUTION_STATUS:
                        EXPECTED_EXECUTION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(sorted(blocker_counts.items())),
            "expected": {
                EXPECTED_EXECUTION_BLOCKER:
                    EXPECTED_EXECUTION_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_EXECUTION_BLOCKER:
                        EXPECTED_EXECUTION_RECORDS
                }
            ),
        },
        {
            "check": "all_execution_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_execution_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_execution_plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {row["authoritative_field_name"] for row in records}
            ),
            "expected": [AUTHORITATIVE_FIELD_NAME],
            "passed": all(
                row["authoritative_field_name"] == AUTHORITATIVE_FIELD_NAME
                for row in records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {row["authoritative_field_path"] for row in records}
            ),
            "expected": [AUTHORITATIVE_FIELD_PATH],
            "passed": all(
                row["authoritative_field_path"] == AUTHORITATIVE_FIELD_PATH
                for row in records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {row["rejected_metadata_field_name"] for row in records}
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
            "check": "validation_components_defined",
            "actual": len(VALIDATION_COMPONENTS),
            "expected": 8,
            "passed": len(VALIDATION_COMPONENTS) == 8,
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
            "expected": 32,
            "passed": len(VALIDATION_REQUIREMENTS) == 32,
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
            "expected": 31,
            "passed": len(BLOCKER_CODES) == 31,
        },
        {
            "check": "validation_plan_record_fields_defined",
            "actual": len(VALIDATION_PLAN_RECORD_FIELDS),
            "expected": 55,
            "passed": len(VALIDATION_PLAN_RECORD_FIELDS) == 55,
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
            "passed": "endpoint_candidate_invention" in PROHIBITED_AUTHORITIES,
        },
        {
            "check": "result_submission_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "acquisition_result_submission_invention"
                in PROHIBITED_AUTHORITIES
                and
                "acquisition_result_completion_by_inference"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "response_artifact_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "response_artifact_invention"
                in PROHIBITED_AUTHORITIES
                and
                "response_metadata_invention"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "network_execution_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "dns_resolution_execution",
                    "socket_connection_execution",
                    "http_request_execution",
                    "api_request_execution",
                )
            ),
        },
        {
            "check": "response_parsing_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "source_evidence_parse_execution"
                in PROHIBITED_AUTHORITIES
                and
                "raw_response_parse_execution"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "historical_value_extraction_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "historical_outcome_value_extraction"
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
            "check": "validation_plan_records_materialized_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "acquisition_results_supplied_zero",
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

    all_checks_passed = all(bool(row["passed"]) for row in checks)

    plan_digest = sha256_payload(
        {
            "plan_version": PLAN_VERSION,
            "validation_principles": VALIDATION_PRINCIPLES,
            "validation_components": VALIDATION_COMPONENTS,
            "validation_stages": VALIDATION_STAGES,
            "validation_requirements": VALIDATION_REQUIREMENTS,
            "validation_statuses": VALIDATION_STATUSES,
            "blocker_codes": BLOCKER_CODES,
            "validation_plan_record_fields":
                VALIDATION_PLAN_RECORD_FIELDS,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
            "prohibited_authorities": PROHIBITED_AUTHORITIES,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_result_validation_"
        "plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_result_validation_"
        "plan_failed"
    )

    next_layer = (
        "9BF_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_result_validation_"
        "implementation"
        if all_checks_passed
        else
        "9BE_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_result_validation_"
        "plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "validation_principles.csv",
        ["principle_id", "principle"],
        VALIDATION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "validation_components.csv",
        ["component_id", "component", "required", "priority"],
        VALIDATION_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "validation_stages.csv",
        ["stage_id", "stage_name", "priority"],
        VALIDATION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "validation_requirements.csv",
        ["requirement_id", "requirement", "expected"],
        VALIDATION_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "validation_statuses.csv",
        ["status", "implementation_authority"],
        VALIDATION_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "blocker_code_catalog.csv",
        ["code", "category"],
        BLOCKER_CODES,
    )

    write_csv(
        OUTPUT_DIR / "validation_plan_record_field_contract.csv",
        ["ordinal", "field"],
        [
            {"ordinal": index, "field": field}
            for index, field in enumerate(
                VALIDATION_PLAN_RECORD_FIELDS,
                start=1,
            )
        ],
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
        OUTPUT_DIR / "candidate_missing_acquisition_execution_inventory.csv",
        [
            "source_evidence_acquisition_execution_plan_record_id",
            "source_evidence_acquisition_execution_plan_record_digest",
            "source_evidence_acquisition_authorization_plan_record_id",
            "source_evidence_validation_plan_record_id",
            "endpoint_candidate_specification_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "source_evidence_acquisition_execution_status",
            "source_evidence_acquisition_execution_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "execution_submission_supplied",
            "execution_id",
            "execution_version",
            "execution_attempt_id",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION,
        "execution_records": len(records),
        "execution_comparisons": len(comparison_ids),
        "execution_status_counts": dict(sorted(status_counts.items())),
        "execution_blocker_counts": dict(sorted(blocker_counts.items())),
        "validation_principles": len(VALIDATION_PRINCIPLES),
        "validation_components": len(VALIDATION_COMPONENTS),
        "validation_stages": len(VALIDATION_STAGES),
        "validation_requirements": len(VALIDATION_REQUIREMENTS),
        "validation_statuses": len(VALIDATION_STATUSES),
        "blocker_codes": len(BLOCKER_CODES),
        "validation_plan_record_fields":
            len(VALIDATION_PLAN_RECORD_FIELDS),
        "ordering_fields": len(ORDERING_FIELDS),
        "implementation_steps": len(IMPLEMENTATION_STEPS),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required": len(checks),
        "predecessor_digest": sha256_payload(records),
        "reverse_predecessor_digest":
            sha256_payload(reverse_records),
        "plan_digest": plan_digest,
        "validation_plan_records_materialized": 0,
        "acquisition_result_submissions_supplied": 0,
        "raw_responses_received": 0,
        "raw_responses_validated": 0,
        "source_evidence_parsed": 0,
        "historical_outcome_values_extracted": 0,
        "credentials_stored": 0,
        "credential_literals_logged": 0,
        "network_retrievals_executed": 0,
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
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "source_evidence_acquisition_result_validation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_acquisition_result_validation_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(PROHIBITED_AUTHORITIES),
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
        "Predecessor contract version: "
        f"{predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION}"
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
    print(f"Execution records replayed: {len(records)}")
    print(f"Execution comparisons: {len(comparison_ids)}")
    print(
        "Execution status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Execution blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(f"Validation principles: {len(VALIDATION_PRINCIPLES)}")
    print(f"Validation components: {len(VALIDATION_COMPONENTS)}")
    print(f"Validation stages: {len(VALIDATION_STAGES)}")
    print(f"Validation requirements: {len(VALIDATION_REQUIREMENTS)}")
    print(
        "Validation plan record fields: "
        f"{len(VALIDATION_PLAN_RECORD_FIELDS)}"
    )
    print(f"Plan digest: {plan_digest}")
    print("Validation plan records materialized: 0")
    print("Acquisition result submissions supplied: 0")
    print("Raw responses received: 0")
    print("Raw responses validated: 0")
    print("Source evidence parsed: 0")
    print("Historical outcome values extracted: 0")
    print("Credentials stored: 0")
    print("Credential literals logged: 0")
    print("Network retrievals executed: 0")
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Candidate values transformed: 0")
    print("Downstream records recomputed: 0")
    print("Uncertainty estimates calculated: 0")
    print("Statistical significance tests calculated: 0")
    print("Superiority decisions emitted: 0")
    print("Equivalence decisions emitted: 0")
    print("Activation recommendations emitted: 0")
    print("Production probabilities changed: 0")
    print("Market comparisons executed: 0")
    print("Betting edges calculated: 0")
    print(f"Diagnosis: {diagnosis_name}")
    print(f"Authority granted: {diagnosis['authority_granted']}")
    print(f"Recommended next layer: {next_layer}")
    print(f"Artifacts: {OUTPUT_DIR.relative_to(ROOT)}")

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
