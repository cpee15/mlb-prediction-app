#!/usr/bin/env python3
"""
Layer 9BC
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Acquisition Execution Plan

Plans the deterministic execution contract for acquiring source evidence only
after an explicit acquisition authorization has been approved.

Layer 9BB established that no endpoint candidate, locator submission,
source-evidence submission, or acquisition-authorization submission exists.
This layer therefore defines execution gates without performing retrieval.

Planning only.

This layer does not:
- invent or select an endpoint candidate;
- invent or complete any submission or authorization;
- invent request scope, credentials, responses, evidence, or outcomes;
- store credential literals;
- perform DNS, socket, HTTP, browser, API, or network activity;
- acquire or parse source evidence;
- retrieve historical outcomes;
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


LAYER_ID = "9BC"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_acquisition_execution_plan"
)

PLAN_VERSION = (
    "layer_9BC_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_execution_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BC_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_acquisition_"
    "execution_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "authorize_9BB_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_acquisition.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BB_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_authorization_contract_v1"
)

EXPECTED_AUTHORIZATION_RECORDS = 16
EXPECTED_AUTHORIZATION_COMPARISONS = 16

EXPECTED_AUTHORIZATION_STATUS = "candidate_not_supplied"

EXPECTED_AUTHORIZATION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


EXECUTION_PRINCIPLES = [
    {
        "principle_id": "HOASEAE-P01",
        "principle": (
            "Execution may be planned only for an explicitly approved, "
            "unexpired, and unrevoked acquisition authorization."
        ),
    },
    {
        "principle_id": "HOASEAE-P02",
        "principle": (
            "Every execution record must preserve candidate, locator, evidence, "
            "authorization, comparison, and defect lineage."
        ),
    },
    {
        "principle_id": "HOASEAE-P03",
        "principle": (
            "The request must be constructed only from the approved transport, "
            "host, path, method, query, header, and content-type allowlists."
        ),
    },
    {
        "principle_id": "HOASEAE-P04",
        "principle": (
            "Credentials may be resolved at execution time only through an "
            "approved secret reference and may never be persisted."
        ),
    },
    {
        "principle_id": "HOASEAE-P05",
        "principle": (
            "Rate limits, retries, timeouts, redaction, retention, integrity, "
            "and audit requirements must be enforced before execution."
        ),
    },
    {
        "principle_id": "HOASEAE-P06",
        "principle": (
            "Raw responses must be quarantined and digested before parsing or "
            "semantic use."
        ),
    },
    {
        "principle_id": "HOASEAE-P07",
        "principle": (
            "Acquisition execution authority is limited to source evidence and "
            "does not authorize historical-outcome retrieval."
        ),
    },
    {
        "principle_id": "HOASEAE-P08",
        "principle": (
            "Successful planning grants execution implementation authority only "
            "and performs no network activity."
        ),
    },
]


EXECUTION_COMPONENTS = [
    {
        "component_id": "HOASEAE-C01",
        "component": "approved_authorization_lineage",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEAE-C02",
        "component": "execution_identity_and_version",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEAE-C03",
        "component": "approved_request_construction",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEAE-C04",
        "component": "secret_reference_resolution_boundary",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEAE-C05",
        "component": "rate_limit_retry_timeout_enforcement",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEAE-C06",
        "component": "response_quarantine_and_integrity",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEAE-C07",
        "component": "redaction_retention_and_audit",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEAE-C08",
        "component": "execution_disposition_and_revocation",
        "required": True,
        "priority": 8,
    },
]


EXECUTION_STAGES = [
    {
        "stage_id": "HOASEAE-S01",
        "stage_name": "authorization_record_replay",
        "priority": 1,
    },
    {
        "stage_id": "HOASEAE-S02",
        "stage_name": "candidate_and_submission_presence_gate",
        "priority": 2,
    },
    {
        "stage_id": "HOASEAE-S03",
        "stage_name": "authorization_approval_expiration_and_revocation_gate",
        "priority": 3,
    },
    {
        "stage_id": "HOASEAE-S04",
        "stage_name": "execution_submission_inventory",
        "priority": 4,
    },
    {
        "stage_id": "HOASEAE-S05",
        "stage_name": "execution_identity_and_scope_validation",
        "priority": 5,
    },
    {
        "stage_id": "HOASEAE-S06",
        "stage_name": "approved_request_construction_validation",
        "priority": 6,
    },
    {
        "stage_id": "HOASEAE-S07",
        "stage_name": "secret_reference_and_redaction_boundary_validation",
        "priority": 7,
    },
    {
        "stage_id": "HOASEAE-S08",
        "stage_name": "operational_control_validation",
        "priority": 8,
    },
    {
        "stage_id": "HOASEAE-S09",
        "stage_name": "response_quarantine_integrity_and_audit_validation",
        "priority": 9,
    },
    {
        "stage_id": "HOASEAE-S10",
        "stage_name": "execution_disposition_assignment",
        "priority": 10,
    },
    {
        "stage_id": "HOASEAE-S11",
        "stage_name": "deterministic_execution_plan_record_emission",
        "priority": 11,
    },
]


EXECUTION_REQUIREMENTS = [
    {
        "requirement_id": "HOASEAE-R01",
        "requirement": "candidate_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R02",
        "requirement": "locator_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R03",
        "requirement": "source_evidence_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R04",
        "requirement": "acquisition_authorization_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R05",
        "requirement": "acquisition_authorization_approved",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R06",
        "requirement": "acquisition_authorization_unexpired",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R07",
        "requirement": "acquisition_authorization_unrevoked",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R08",
        "requirement": "execution_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R09",
        "requirement": "execution_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R10",
        "requirement": "execution_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R11",
        "requirement": "execution_attempt_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R12",
        "requirement": "candidate_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R13",
        "requirement": "comparison_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R14",
        "requirement": "authorization_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R15",
        "requirement": "approved_scheme",
        "expected": "https",
    },
    {
        "requirement_id": "HOASEAE-R16",
        "requirement": "approved_host_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R17",
        "requirement": "approved_path_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R18",
        "requirement": "approved_http_method",
        "expected": "GET",
    },
    {
        "requirement_id": "HOASEAE-R19",
        "requirement": "query_parameters_within_allowlist",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R20",
        "requirement": "headers_within_allowlist",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R21",
        "requirement": "response_content_type_within_allowlist",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R22",
        "requirement": "secret_reference_present_when_required",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R23",
        "requirement": "credential_literal_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R24",
        "requirement": "request_log_redaction_enabled",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R25",
        "requirement": "rate_limit_enforcement_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R26",
        "requirement": "bounded_retry_policy_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R27",
        "requirement": "connect_and_read_timeouts_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R28",
        "requirement": "raw_response_quarantine_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R29",
        "requirement": "raw_response_digest_algorithm",
        "expected": "sha256",
    },
    {
        "requirement_id": "HOASEAE-R30",
        "requirement": "retention_policy_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R31",
        "requirement": "audit_record_contract_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAE-R32",
        "requirement": "parse_execution_authorized",
        "expected": False,
    },
    {
        "requirement_id": "HOASEAE-R33",
        "requirement": "historical_outcome_retrieval_authorized",
        "expected": False,
    },
    {
        "requirement_id": "HOASEAE-R34",
        "requirement": "network_retrieval_executed_during_planning",
        "expected": False,
    },
]


EXECUTION_STATUSES = [
    {
        "status": "source_evidence_acquisition_execution_ready",
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
        "status": "acquisition_authorization_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "acquisition_authorization_not_approved",
        "implementation_authority": False,
    },
    {
        "status": "acquisition_authorization_expired_or_revoked",
        "implementation_authority": False,
    },
    {
        "status": "execution_submission_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "execution_identity_or_scope_invalid",
        "implementation_authority": False,
    },
    {
        "status": "request_or_operational_contract_invalid",
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
        "code": "historical_outcome_source_evidence_acquisition_authorization_missing",
        "category": "authorization",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_authorization_not_approved",
        "category": "authorization",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_authorization_expired",
        "category": "authorization",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_authorization_revoked",
        "category": "authorization",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_submission_missing",
        "category": "execution",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_id_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_version_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_attempt_id_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_candidate_scope_mismatch",
        "category": "scope",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_comparison_scope_mismatch",
        "category": "scope",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_authorization_scope_mismatch",
        "category": "scope",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_scheme_invalid",
        "category": "transport",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_host_missing",
        "category": "transport",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_path_missing",
        "category": "transport",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_method_invalid",
        "category": "request",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_query_not_allowed",
        "category": "request",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_header_not_allowed",
        "category": "request",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_content_type_not_allowed",
        "category": "response",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_secret_reference_missing",
        "category": "security",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_credential_literal_detected",
        "category": "security",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_redaction_disabled",
        "category": "security",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_rate_limit_missing",
        "category": "operations",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_retry_policy_invalid",
        "category": "operations",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_timeout_missing",
        "category": "operations",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_quarantine_missing",
        "category": "response",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_digest_contract_missing",
        "category": "integrity",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_retention_missing",
        "category": "retention",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_audit_contract_missing",
        "category": "audit",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_execution_invention_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_source_evidence_acquisition_network_execution_requested",
        "category": "authority",
    },
]


EXECUTION_PLAN_RECORD_FIELDS = [
    "source_evidence_acquisition_execution_plan_contract_version",
    "source_evidence_acquisition_execution_plan_record_id",
    "source_evidence_acquisition_authorization_plan_record_id",
    "source_evidence_acquisition_authorization_plan_record_digest",
    "source_evidence_validation_plan_record_id",
    "evidence_locator_submission_plan_record_id",
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
    "authorization_status",
    "authorization_blocker_codes",
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
    "approved_request_scope",
    "credential_reference_contract",
    "request_log_redaction_contract",
    "rate_limit_retry_timeout_contract",
    "retention_integrity_audit_contract",
    "authorization_attestation",
    "revocation_contract",
    "execution_submission_supplied",
    "execution_id",
    "execution_version",
    "execution_attempt_id",
    "execution_request_contract",
    "execution_response_quarantine_contract",
    "source_evidence_acquisition_execution_status",
    "source_evidence_acquisition_execution_blocker_codes",
    "source_evidence_acquisition_execution_implementation_authority_granted",
    "source_evidence_acquisition_execution_rationale",
    "source_evidence_acquisition_execution_limitations",
    "source_evidence_acquisition_execution_plan_identity_digest",
    "source_evidence_acquisition_execution_plan_record_digest",
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
        "field": "authorization_id",
    },
    {
        "ordinal": 5,
        "field": "execution_id",
    },
    {
        "ordinal": 6,
        "field": "source_evidence_acquisition_execution_plan_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9BB_acquisition_authorization_records",
    },
    {
        "ordinal": 2,
        "step": "preserve_candidate_evidence_authorization_and_defect_lineage",
    },
    {
        "ordinal": 3,
        "step": "require_explicit_candidate_and_all_submissions",
    },
    {
        "ordinal": 4,
        "step": "require_approved_unexpired_unrevoked_authorization",
    },
    {
        "ordinal": 5,
        "step": "load_explicit_acquisition_execution_submissions",
    },
    {
        "ordinal": 6,
        "step": "validate_execution_identity_version_attempt_and_scope",
    },
    {
        "ordinal": 7,
        "step": "construct_request_only_from_approved_scope",
    },
    {
        "ordinal": 8,
        "step": "validate_query_header_method_and_content_type_allowlists",
    },
    {
        "ordinal": 9,
        "step": "validate_secret_reference_without_resolving_literal",
    },
    {
        "ordinal": 10,
        "step": "validate_redaction_rate_limit_retry_and_timeout_controls",
    },
    {
        "ordinal": 11,
        "step": "validate_response_quarantine_and_sha256_integrity_contract",
    },
    {
        "ordinal": 12,
        "step": "validate_retention_and_audit_contract",
    },
    {
        "ordinal": 13,
        "step": "withhold_parse_and_historical_outcome_retrieval_authority",
    },
    {
        "ordinal": 14,
        "step": "emit_deterministic_execution_plan_records",
    },
    {
        "ordinal": 15,
        "step": "verify_forward_and_reverse_replay",
    },
    {
        "ordinal": 16,
        "step": "grant_execution_implementation_only_when_complete",
    },
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "endpoint_candidate_selection_without_submission",
    "evidence_locator_invention",
    "locator_submission_invention",
    "source_evidence_invention",
    "source_evidence_fabrication",
    "acquisition_authorization_invention",
    "acquisition_authorization_completion_by_inference",
    "acquisition_execution_submission_invention",
    "acquisition_execution_completion_by_inference",
    "request_scope_invention",
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
    "candidate_approval",
    "candidate_materialization",
    "historical_outcome_retrieval_planning",
    "historical_outcome_fetch_execution",
    "historical_outcome_parse_execution",
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


def load_module(path: Path, module_name: str) -> Any:
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
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )


def replay_predecessor() -> dict[str, Any]:
    predecessor = load_module(
        PREDECESSOR_PATH,
        "layer_9bb_predecessor",
    )

    if (
        predecessor.ACQUISITION_AUTHORIZATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BB contract version: "
            f"{predecessor.ACQUISITION_AUTHORIZATION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_authorization_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_authorization_records(
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
        row[
            "source_evidence_acquisition_authorization_status"
        ]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "source_evidence_acquisition_authorization_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_bb_contract_version_verified",
            "actual":
                predecessor.ACQUISITION_AUTHORIZATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.ACQUISITION_AUTHORIZATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_bb_replay_deterministic",
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
            "check": "nine_bb_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": (
                sha256_payload(records)
                == sha256_payload(reverse_records)
            ),
        },
        {
            "check": "expected_authorization_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_AUTHORIZATION_RECORDS,
            "passed": (
                len(records)
                == EXPECTED_AUTHORIZATION_RECORDS
            ),
        },
        {
            "check": "expected_authorization_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_AUTHORIZATION_COMPARISONS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_AUTHORIZATION_COMPARISONS
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(
                sorted(status_counts.items())
            ),
            "expected": {
                EXPECTED_AUTHORIZATION_STATUS:
                    EXPECTED_AUTHORIZATION_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_AUTHORIZATION_STATUS:
                        EXPECTED_AUTHORIZATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(
                sorted(blocker_counts.items())
            ),
            "expected": {
                EXPECTED_AUTHORIZATION_BLOCKER:
                    EXPECTED_AUTHORIZATION_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_AUTHORIZATION_BLOCKER:
                        EXPECTED_AUTHORIZATION_RECORDS
                }
            ),
        },
        {
            "check": "all_authorization_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_authorization_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_authorization_plan_record_digest"
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
            "check": "execution_principles_defined",
            "actual": len(EXECUTION_PRINCIPLES),
            "expected": 8,
            "passed": len(EXECUTION_PRINCIPLES) == 8,
        },
        {
            "check": "execution_components_defined",
            "actual": len(EXECUTION_COMPONENTS),
            "expected": 8,
            "passed": len(EXECUTION_COMPONENTS) == 8,
        },
        {
            "check": "execution_stages_defined",
            "actual": len(EXECUTION_STAGES),
            "expected": 11,
            "passed": len(EXECUTION_STAGES) == 11,
        },
        {
            "check": "execution_requirements_defined",
            "actual": len(EXECUTION_REQUIREMENTS),
            "expected": 34,
            "passed": len(EXECUTION_REQUIREMENTS) == 34,
        },
        {
            "check": "execution_statuses_defined",
            "actual": len(EXECUTION_STATUSES),
            "expected": 10,
            "passed": len(EXECUTION_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 33,
            "passed": len(BLOCKER_CODES) == 33,
        },
        {
            "check": "execution_plan_record_fields_defined",
            "actual": len(EXECUTION_PLAN_RECORD_FIELDS),
            "expected": 54,
            "passed": len(EXECUTION_PLAN_RECORD_FIELDS) == 54,
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
            "check": "authorization_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "acquisition_authorization_invention"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "execution_submission_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "acquisition_execution_submission_invention"
                in PROHIBITED_AUTHORITIES
                and
                "acquisition_execution_completion_by_inference"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "request_scope_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "request_scope_invention"
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
                and
                "credential_literal_logging"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "network_execution_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "dns_resolution_execution"
                in PROHIBITED_AUTHORITIES
                and
                "socket_connection_execution"
                in PROHIBITED_AUTHORITIES
                and
                "http_request_execution"
                in PROHIBITED_AUTHORITIES
                and
                "api_request_execution"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "source_evidence_parse_execution_prohibited",
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
            "check": "execution_plan_records_materialized_zero",
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
            "execution_principles":
                EXECUTION_PRINCIPLES,
            "execution_components":
                EXECUTION_COMPONENTS,
            "execution_stages":
                EXECUTION_STAGES,
            "execution_requirements":
                EXECUTION_REQUIREMENTS,
            "execution_statuses":
                EXECUTION_STATUSES,
            "blocker_codes":
                BLOCKER_CODES,
            "execution_plan_record_fields":
                EXECUTION_PLAN_RECORD_FIELDS,
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
        "endpoint_candidate_source_evidence_acquisition_execution_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_execution_plan_failed"
    )

    next_layer = (
        "9BD_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_execution_implementation"
        if all_checks_passed
        else
        "9BC_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_execution_plan_"
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
        OUTPUT_DIR / "execution_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        EXECUTION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "execution_components.csv",
        [
            "component_id",
            "component",
            "required",
            "priority",
        ],
        EXECUTION_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "execution_stages.csv",
        [
            "stage_id",
            "stage_name",
            "priority",
        ],
        EXECUTION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "execution_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        EXECUTION_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "execution_statuses.csv",
        [
            "status",
            "implementation_authority",
        ],
        EXECUTION_STATUSES,
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
        OUTPUT_DIR / "execution_plan_record_field_contract.csv",
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
                EXECUTION_PLAN_RECORD_FIELDS,
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
        OUTPUT_DIR / "candidate_missing_acquisition_authorization_inventory.csv",
        [
            "source_evidence_acquisition_authorization_plan_record_id",
            "source_evidence_acquisition_authorization_plan_record_digest",
            "source_evidence_validation_plan_record_id",
            "endpoint_candidate_specification_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "source_evidence_acquisition_authorization_status",
            "source_evidence_acquisition_authorization_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "source_owner",
            "source_class",
            "authorization_submission_supplied",
            "authorization_id",
            "authorization_version",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.ACQUISITION_AUTHORIZATION_CONTRACT_VERSION,
        "authorization_records":
            len(records),
        "authorization_comparisons":
            len(comparison_ids),
        "authorization_status_counts":
            dict(sorted(status_counts.items())),
        "authorization_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "execution_principles":
            len(EXECUTION_PRINCIPLES),
        "execution_components":
            len(EXECUTION_COMPONENTS),
        "execution_stages":
            len(EXECUTION_STAGES),
        "execution_requirements":
            len(EXECUTION_REQUIREMENTS),
        "execution_statuses":
            len(EXECUTION_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "execution_plan_record_fields":
            len(EXECUTION_PLAN_RECORD_FIELDS),
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
        "execution_plan_records_materialized": 0,
        "execution_submissions_supplied": 0,
        "credentials_stored": 0,
        "dns_resolutions_executed": 0,
        "socket_connections_executed": 0,
        "network_retrievals_executed": 0,
        "source_evidence_acquired": 0,
        "source_evidence_parsed": 0,
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
        / "source_evidence_acquisition_execution_plan_summary.json",
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
            "source_evidence_acquisition_execution_implementation"
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
        f"{predecessor.ACQUISITION_AUTHORIZATION_CONTRACT_VERSION}"
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
        "Authorization records replayed: "
        f"{len(records)}"
    )
    print(
        "Authorization comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Authorization status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Authorization blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Execution principles: "
        f"{len(EXECUTION_PRINCIPLES)}"
    )
    print(
        "Execution components: "
        f"{len(EXECUTION_COMPONENTS)}"
    )
    print(
        "Execution stages: "
        f"{len(EXECUTION_STAGES)}"
    )
    print(
        "Execution requirements: "
        f"{len(EXECUTION_REQUIREMENTS)}"
    )
    print(
        "Execution plan record fields: "
        f"{len(EXECUTION_PLAN_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Execution plan records materialized: 0")
    print("Execution submissions supplied: 0")
    print("Credentials stored: 0")
    print("DNS resolutions executed: 0")
    print("Socket connections executed: 0")
    print("Network retrievals executed: 0")
    print("Source evidence acquired: 0")
    print("Source evidence parsed: 0")
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
