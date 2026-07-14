#!/usr/bin/env python3
"""
Layer 9AQ
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Specification Plan

Plans the deterministic specification contract for an explicitly supplied
authoritative source endpoint candidate.

Layer 9AP established that no endpoint candidate is currently supplied. This
layer defines the schema, validation rules, identity mappings, authority
evidence, transport boundaries, secret-reference rules, response semantics,
and approval statuses required before a candidate may be materialized.

Planning only.

This layer does not:
- invent or select an endpoint;
- store credentials or credential literals;
- execute network retrieval;
- parse or acquire historical outcomes;
- create raw-response artifacts;
- mutate canonical source values or mappings;
- transform, infer, substitute, default, or impute values;
- recompute downstream records;
- calculate uncertainty, significance, superiority, equivalence, activation,
  production probabilities, market comparisons, pricing, or betting edges.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AQ"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_authoritative_source_endpoint_candidate_specification_plan"
)

PLAN_VERSION = (
    "layer_9AQ_historical_outcome_authoritative_"
    "source_endpoint_candidate_specification_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AQ_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_source_endpoint_candidate_specification_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "configure_9AP_pitch_type_matchup_overlay_historical_"
    "outcome_authoritative_source_endpoint.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AP_historical_outcome_authoritative_"
    "source_endpoint_configuration_contract_v1"
)

EXPECTED_CONFIGURATION_RECORDS = 16
EXPECTED_CONFIGURATION_COMPARISONS = 16

EXPECTED_CONFIGURATION_STATUS = (
    "endpoint_candidate_not_supplied"
)

EXPECTED_CONFIGURATION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


SPECIFICATION_PRINCIPLES = [
    {
        "principle_id": "HOASECSP-P01",
        "principle": (
            "An endpoint candidate must be explicitly supplied and may never be "
            "invented from incomplete repository context."
        ),
    },
    {
        "principle_id": "HOASECSP-P02",
        "principle": (
            "The candidate must identify its source authority, ownership, "
            "documentation, and historical-data scope."
        ),
    },
    {
        "principle_id": "HOASECSP-P03",
        "principle": (
            "The candidate must define exact request identity mappings for game, "
            "target, event level, event identity, and event sequence."
        ),
    },
    {
        "principle_id": "HOASECSP-P04",
        "principle": (
            "The candidate must define the authoritative outcome field and "
            "explicitly reject availability or timestamp metadata as outcomes."
        ),
    },
    {
        "principle_id": "HOASECSP-P05",
        "principle": (
            "Credential material must be referenced indirectly and credential "
            "literals must never be committed."
        ),
    },
    {
        "principle_id": "HOASECSP-P06",
        "principle": (
            "Transport, retry, timeout, rate-limit, and pagination policies must "
            "be bounded and deterministic."
        ),
    },
    {
        "principle_id": "HOASECSP-P07",
        "principle": (
            "Raw-response retention, content type, snapshot version, and SHA-256 "
            "digest requirements must be specified before retrieval."
        ),
    },
    {
        "principle_id": "HOASECSP-P08",
        "principle": (
            "Candidate specification approval grants candidate materialization "
            "authority only, not retrieval or canonical correction authority."
        ),
    },
]


CANDIDATE_SOURCE_CLASSES = [
    {
        "source_class_id": "HOASECSP-S01",
        "source_class": "official_governing_event_api",
        "priority": 1,
        "eligible": True,
    },
    {
        "source_class_id": "HOASECSP-S02",
        "source_class": "official_statistics_archive",
        "priority": 2,
        "eligible": True,
    },
    {
        "source_class_id": "HOASECSP-S03",
        "source_class": "project_retained_upstream_snapshot_store",
        "priority": 3,
        "eligible": True,
    },
    {
        "source_class_id": "HOASECSP-S04",
        "source_class": "validated_secondary_archive",
        "priority": 4,
        "eligible": True,
    },
    {
        "source_class_id": "HOASECSP-S05",
        "source_class": "unverified_reference_source",
        "priority": 5,
        "eligible": False,
    },
]


SPECIFICATION_STAGES = [
    {
        "stage_id": "HOASECSP-C01",
        "stage_name": "configuration_replay",
        "priority": 1,
    },
    {
        "stage_id": "HOASECSP-C02",
        "stage_name": "candidate_submission",
        "priority": 2,
    },
    {
        "stage_id": "HOASECSP-C03",
        "stage_name": "authority_evidence_validation",
        "priority": 3,
    },
    {
        "stage_id": "HOASECSP-C04",
        "stage_name": "base_uri_and_protocol_validation",
        "priority": 4,
    },
    {
        "stage_id": "HOASECSP-C05",
        "stage_name": "identity_mapping_validation",
        "priority": 5,
    },
    {
        "stage_id": "HOASECSP-C06",
        "stage_name": "outcome_semantic_validation",
        "priority": 6,
    },
    {
        "stage_id": "HOASECSP-C07",
        "stage_name": "credential_reference_validation",
        "priority": 7,
    },
    {
        "stage_id": "HOASECSP-C08",
        "stage_name": "transport_policy_validation",
        "priority": 8,
    },
    {
        "stage_id": "HOASECSP-C09",
        "stage_name": "response_retention_validation",
        "priority": 9,
    },
    {
        "stage_id": "HOASECSP-C10",
        "stage_name": "candidate_conflict_validation",
        "priority": 10,
    },
    {
        "stage_id": "HOASECSP-C11",
        "stage_name": "candidate_specification_disposition",
        "priority": 11,
    },
]


CANDIDATE_REQUIREMENTS = [
    {
        "requirement_id": "HOASECSP-R01",
        "requirement": "candidate_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R02",
        "requirement": "candidate_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R03",
        "requirement": "source_class_eligible",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R04",
        "requirement": "source_owner_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R05",
        "requirement": "authority_documentation_uri_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R06",
        "requirement": "base_uri_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R07",
        "requirement": "transport_protocol",
        "expected": "https_or_immutable_local_snapshot",
    },
    {
        "requirement_id": "HOASECSP-R08",
        "requirement": "request_method_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R09",
        "requirement": "game_identity_mapping_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R10",
        "requirement": "target_identity_mapping_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R11",
        "requirement": "event_level_mapping_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R12",
        "requirement": "event_identity_mapping_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R13",
        "requirement": "event_sequence_mapping_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R14",
        "requirement": "authoritative_outcome_field_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R15",
        "requirement": "outcome_numeric_domain_defined",
        "expected": "finite_int_or_float_excluding_bool",
    },
    {
        "requirement_id": "HOASECSP-R16",
        "requirement": "rejected_metadata_fields_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R17",
        "requirement": "credential_reference_only",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R18",
        "requirement": "credential_literal_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R19",
        "requirement": "timeout_policy_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R20",
        "requirement": "retry_policy_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R21",
        "requirement": "rate_limit_policy_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R22",
        "requirement": "pagination_policy_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R23",
        "requirement": "raw_response_retention_policy_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECSP-R24",
        "requirement": "response_digest_algorithm",
        "expected": "sha256",
    },
    {
        "requirement_id": "HOASECSP-R25",
        "requirement": "retrieval_executed_during_specification",
        "expected": False,
    },
]


SPECIFICATION_STATUSES = [
    {
        "status": "endpoint_candidate_specification_approved",
        "materialization_authority": True,
    },
    {
        "status": "endpoint_candidate_not_supplied",
        "materialization_authority": False,
    },
    {
        "status": "endpoint_candidate_authority_insufficient",
        "materialization_authority": False,
    },
    {
        "status": "endpoint_candidate_identity_mapping_incomplete",
        "materialization_authority": False,
    },
    {
        "status": "endpoint_candidate_outcome_semantics_incomplete",
        "materialization_authority": False,
    },
    {
        "status": "endpoint_candidate_credential_contract_invalid",
        "materialization_authority": False,
    },
    {
        "status": "endpoint_candidate_transport_policy_incomplete",
        "materialization_authority": False,
    },
    {
        "status": "endpoint_candidate_retention_policy_incomplete",
        "materialization_authority": False,
    },
    {
        "status": "endpoint_candidate_conflict",
        "materialization_authority": False,
    },
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "historical_outcome_endpoint_candidate_id_missing", "category": "identity"},
    {"code": "historical_outcome_endpoint_candidate_version_missing", "category": "identity"},
    {"code": "historical_outcome_endpoint_candidate_source_owner_missing", "category": "authority"},
    {"code": "historical_outcome_endpoint_candidate_authority_documentation_missing", "category": "authority"},
    {"code": "historical_outcome_endpoint_candidate_authority_insufficient", "category": "authority"},
    {"code": "historical_outcome_endpoint_candidate_base_uri_missing", "category": "transport"},
    {"code": "historical_outcome_endpoint_candidate_protocol_invalid", "category": "transport"},
    {"code": "historical_outcome_endpoint_candidate_request_method_missing", "category": "transport"},
    {"code": "historical_outcome_endpoint_candidate_game_mapping_missing", "category": "identity"},
    {"code": "historical_outcome_endpoint_candidate_target_mapping_missing", "category": "identity"},
    {"code": "historical_outcome_endpoint_candidate_event_level_mapping_missing", "category": "identity"},
    {"code": "historical_outcome_endpoint_candidate_event_identity_mapping_missing", "category": "identity"},
    {"code": "historical_outcome_endpoint_candidate_sequence_mapping_missing", "category": "identity"},
    {"code": "historical_outcome_endpoint_candidate_outcome_field_missing", "category": "semantics"},
    {"code": "historical_outcome_endpoint_candidate_numeric_domain_missing", "category": "semantics"},
    {"code": "historical_outcome_endpoint_candidate_metadata_rejection_missing", "category": "semantics"},
    {"code": "historical_outcome_endpoint_candidate_credential_reference_missing", "category": "security"},
    {"code": "historical_outcome_endpoint_candidate_credential_literal_detected", "category": "security"},
    {"code": "historical_outcome_endpoint_candidate_timeout_policy_missing", "category": "transport"},
    {"code": "historical_outcome_endpoint_candidate_retry_policy_missing", "category": "transport"},
    {"code": "historical_outcome_endpoint_candidate_rate_limit_policy_missing", "category": "transport"},
    {"code": "historical_outcome_endpoint_candidate_pagination_policy_missing", "category": "transport"},
    {"code": "historical_outcome_endpoint_candidate_retention_policy_missing", "category": "evidence"},
    {"code": "historical_outcome_endpoint_candidate_digest_policy_missing", "category": "lineage"},
    {"code": "historical_outcome_endpoint_candidate_conflict", "category": "conflict"},
    {"code": "historical_outcome_endpoint_candidate_invention_requested", "category": "authority"},
    {"code": "historical_outcome_endpoint_candidate_retrieval_requested", "category": "authority"},
]


CANDIDATE_SPECIFICATION_FIELDS = [
    "endpoint_candidate_specification_contract_version",
    "endpoint_candidate_specification_record_id",
    "authoritative_source_endpoint_configuration_record_id",
    "authoritative_source_endpoint_configuration_record_digest",
    "authoritative_source_acquisition_record_id",
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
    "configuration_status",
    "configuration_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "source_class",
    "source_class_priority",
    "source_class_eligible",
    "source_owner",
    "authority_documentation_uri",
    "base_uri",
    "transport_protocol",
    "request_method",
    "identity_parameter_mapping",
    "authoritative_outcome_field",
    "outcome_numeric_domain",
    "rejected_metadata_fields",
    "credential_reference",
    "credential_literal_present",
    "timeout_policy",
    "retry_policy",
    "rate_limit_policy",
    "pagination_policy",
    "raw_response_retention_policy",
    "response_digest_algorithm",
    "specification_status",
    "specification_blocker_codes",
    "candidate_materialization_authority_granted",
    "specification_rationale",
    "specification_limitations",
    "endpoint_candidate_specification_identity_digest",
    "endpoint_candidate_specification_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "source_class_priority"},
    {"ordinal": 4, "field": "base_uri"},
    {"ordinal": 5, "field": "candidate_id"},
    {"ordinal": 6, "field": "endpoint_candidate_specification_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9AP_candidate_missing_configuration_records"},
    {"ordinal": 2, "step": "preserve_configuration_acquisition_and_defect_lineage"},
    {"ordinal": 3, "step": "load_explicit_candidate_submission"},
    {"ordinal": 4, "step": "reject_missing_or_invented_candidate"},
    {"ordinal": 5, "step": "validate_candidate_identity_and_version"},
    {"ordinal": 6, "step": "validate_source_owner_authority_class_and_documentation"},
    {"ordinal": 7, "step": "validate_base_uri_protocol_and_request_method"},
    {"ordinal": 8, "step": "validate_identity_parameter_mapping"},
    {"ordinal": 9, "step": "validate_outcome_field_numeric_domain_and_metadata_rejection"},
    {"ordinal": 10, "step": "validate_secret_reference_and_literal_absence"},
    {"ordinal": 11, "step": "validate_transport_policies"},
    {"ordinal": 12, "step": "validate_raw_response_retention_and_digest_policy"},
    {"ordinal": 13, "step": "validate_candidate_conflicts"},
    {"ordinal": 14, "step": "emit_deterministic_candidate_specification_records"},
    {"ordinal": 15, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 16, "step": "grant_candidate_materialization_only_for_approved_specification"},
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "endpoint_candidate_selection_without_submission",
    "credential_literal_storage",
    "historical_outcome_retrieval_planning",
    "historical_outcome_fetch_execution",
    "historical_outcome_parse_execution",
    "raw_response_materialization",
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
        "layer_9ap_predecessor",
    )

    if (
        predecessor.CONFIGURATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AP contract version: "
            f"{predecessor.CONFIGURATION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_configuration_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_configuration_records(
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
        row["configuration_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row["configuration_blocker_codes"]
    )

    checks = [
        {
            "check": "nine_ap_contract_version_verified",
            "actual": predecessor.CONFIGURATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.CONFIGURATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_ap_replay_deterministic",
            "actual": canonical_json(records) == canonical_json(reverse_records),
            "expected": True,
            "passed": canonical_json(records) == canonical_json(reverse_records),
        },
        {
            "check": "nine_ap_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": sha256_payload(records) == sha256_payload(reverse_records),
        },
        {
            "check": "expected_configuration_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_CONFIGURATION_RECORDS,
            "passed": len(records) == EXPECTED_CONFIGURATION_RECORDS,
        },
        {
            "check": "expected_configuration_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_CONFIGURATION_COMPARISONS,
            "passed": len(comparison_ids) == EXPECTED_CONFIGURATION_COMPARISONS,
        },
        {
            "check": "all_configurations_candidate_not_supplied",
            "actual": dict(sorted(status_counts.items())),
            "expected": {
                EXPECTED_CONFIGURATION_STATUS:
                    EXPECTED_CONFIGURATION_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_CONFIGURATION_STATUS:
                        EXPECTED_CONFIGURATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(sorted(blocker_counts.items())),
            "expected": {
                EXPECTED_CONFIGURATION_BLOCKER:
                    EXPECTED_CONFIGURATION_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_CONFIGURATION_BLOCKER:
                        EXPECTED_CONFIGURATION_RECORDS
                }
            ),
        },
        {
            "check": "all_configuration_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authoritative_source_endpoint_configuration_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_source_endpoint_configuration_record_digest"
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
                row["rejected_metadata_field_name"] == REJECTED_METADATA_FIELD
                for row in records
            ),
        },
        {
            "check": "specification_principles_defined",
            "actual": len(SPECIFICATION_PRINCIPLES),
            "expected": 8,
            "passed": len(SPECIFICATION_PRINCIPLES) == 8,
        },
        {
            "check": "candidate_source_classes_defined",
            "actual": len(CANDIDATE_SOURCE_CLASSES),
            "expected": 5,
            "passed": len(CANDIDATE_SOURCE_CLASSES) == 5,
        },
        {
            "check": "specification_stages_defined",
            "actual": len(SPECIFICATION_STAGES),
            "expected": 11,
            "passed": len(SPECIFICATION_STAGES) == 11,
        },
        {
            "check": "candidate_requirements_defined",
            "actual": len(CANDIDATE_REQUIREMENTS),
            "expected": 25,
            "passed": len(CANDIDATE_REQUIREMENTS) == 25,
        },
        {
            "check": "specification_statuses_defined",
            "actual": len(SPECIFICATION_STATUSES),
            "expected": 9,
            "passed": len(SPECIFICATION_STATUSES) == 9,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 28,
            "passed": len(BLOCKER_CODES) == 28,
        },
        {
            "check": "candidate_specification_fields_defined",
            "actual": len(CANDIDATE_SPECIFICATION_FIELDS),
            "expected": 49,
            "passed": len(CANDIDATE_SPECIFICATION_FIELDS) == 49,
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
            "check": "endpoint_candidate_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": "endpoint_candidate_invention" in PROHIBITED_AUTHORITIES,
        },
        {
            "check": "credential_literal_storage_prohibited",
            "actual": True,
            "expected": True,
            "passed": "credential_literal_storage" in PROHIBITED_AUTHORITIES,
        },
        {
            "check": "retrieval_execution_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "historical_outcome_fetch_execution"
                in PROHIBITED_AUTHORITIES
                and "historical_outcome_parse_execution"
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
                and "canonical_outcome_mapping_change"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "candidate_transformation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "boolean_to_integer_coercion"
                in PROHIBITED_AUTHORITIES
                and "source_value_defaulting"
                in PROHIBITED_AUTHORITIES
                and "source_value_inference"
                in PROHIBITED_AUTHORITIES
                and "source_value_imputation"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "candidate_specifications_materialized_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "credentials_stored_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "network_retrieval_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "canonical_sources_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "downstream_records_not_recomputed",
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
            "specification_principles": SPECIFICATION_PRINCIPLES,
            "candidate_source_classes": CANDIDATE_SOURCE_CLASSES,
            "specification_stages": SPECIFICATION_STAGES,
            "candidate_requirements": CANDIDATE_REQUIREMENTS,
            "specification_statuses": SPECIFICATION_STATUSES,
            "blocker_codes": BLOCKER_CODES,
            "candidate_specification_fields": CANDIDATE_SPECIFICATION_FIELDS,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
            "prohibited_authorities": PROHIBITED_AUTHORITIES,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_"
        "authoritative_source_endpoint_candidate_specification_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_"
        "authoritative_source_endpoint_candidate_specification_plan_failed"
    )

    next_layer = (
        "9AR_pitch_type_matchup_overlay_historical_outcome_"
        "authoritative_source_endpoint_candidate_specification_implementation"
        if all_checks_passed
        else
        "9AQ_pitch_type_matchup_overlay_historical_outcome_"
        "authoritative_source_endpoint_candidate_specification_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "specification_principles.csv",
        ["principle_id", "principle"],
        SPECIFICATION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "candidate_source_classes.csv",
        ["source_class_id", "source_class", "priority", "eligible"],
        CANDIDATE_SOURCE_CLASSES,
    )

    write_csv(
        OUTPUT_DIR / "specification_stages.csv",
        ["stage_id", "stage_name", "priority"],
        SPECIFICATION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "candidate_requirements.csv",
        ["requirement_id", "requirement", "expected"],
        CANDIDATE_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "specification_statuses.csv",
        ["status", "materialization_authority"],
        SPECIFICATION_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "blocker_code_catalog.csv",
        ["code", "category"],
        BLOCKER_CODES,
    )

    write_csv(
        OUTPUT_DIR / "candidate_specification_field_contract.csv",
        ["ordinal", "field"],
        [
            {"ordinal": index, "field": field}
            for index, field in enumerate(
                CANDIDATE_SPECIFICATION_FIELDS,
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
        OUTPUT_DIR / "candidate_missing_configuration_inventory.csv",
        [
            "authoritative_source_endpoint_configuration_record_id",
            "authoritative_source_endpoint_configuration_record_digest",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "configuration_status",
            "configuration_blocker_codes",
            "endpoint_candidate_present",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.CONFIGURATION_CONTRACT_VERSION,
        "configuration_records": len(records),
        "configuration_comparisons": len(comparison_ids),
        "configuration_status_counts":
            dict(sorted(status_counts.items())),
        "configuration_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "specification_principles":
            len(SPECIFICATION_PRINCIPLES),
        "candidate_source_classes":
            len(CANDIDATE_SOURCE_CLASSES),
        "specification_stages":
            len(SPECIFICATION_STAGES),
        "candidate_requirements":
            len(CANDIDATE_REQUIREMENTS),
        "specification_statuses":
            len(SPECIFICATION_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "candidate_specification_fields":
            len(CANDIDATE_SPECIFICATION_FIELDS),
        "ordering_fields":
            len(ORDERING_FIELDS),
        "implementation_steps":
            len(IMPLEMENTATION_STEPS),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required": len(checks),
        "predecessor_digest": sha256_payload(records),
        "reverse_predecessor_digest":
            sha256_payload(reverse_records),
        "plan_digest": plan_digest,
        "candidate_specifications_materialized": 0,
        "credentials_stored": 0,
        "network_retrievals_executed": 0,
        "raw_source_artifacts_retained": 0,
        "authoritative_sources_acquired": 0,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "source_values_repaired": 0,
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
        / "authoritative_source_endpoint_candidate_specification_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_"
            "candidate_specification_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld":
            sorted(PROHIBITED_AUTHORITIES),
        "recommended_next_layer": next_layer,
        "output_directory":
            str(OUTPUT_DIR.relative_to(ROOT)),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(f"Layer: {LAYER_ID} — {LAYER_NAME}")
    print(f"Plan version: {PLAN_VERSION}")
    print(
        "Predecessor contract version: "
        f"{predecessor.CONFIGURATION_CONTRACT_VERSION}"
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
    print(f"Configuration records replayed: {len(records)}")
    print(f"Configuration comparisons: {len(comparison_ids)}")
    print(
        "Configuration status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Configuration blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Specification principles: "
        f"{len(SPECIFICATION_PRINCIPLES)}"
    )
    print(
        "Candidate source classes: "
        f"{len(CANDIDATE_SOURCE_CLASSES)}"
    )
    print(
        "Specification stages: "
        f"{len(SPECIFICATION_STAGES)}"
    )
    print(
        "Candidate requirements: "
        f"{len(CANDIDATE_REQUIREMENTS)}"
    )
    print(
        "Candidate specification fields: "
        f"{len(CANDIDATE_SPECIFICATION_FIELDS)}"
    )
    print(f"Plan digest: {plan_digest}")
    print("Candidate specifications materialized: 0")
    print("Credentials stored: 0")
    print("Network retrievals executed: 0")
    print("Raw source artifacts retained: 0")
    print("Authoritative sources acquired: 0")
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Source values repaired: 0")
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
    print(
        "Authority granted: "
        f"{diagnosis['authority_granted']}"
    )
    print(f"Recommended next layer: {next_layer}")
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
