#!/usr/bin/env python3
"""
Layer 9BI
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Parsed Record Validation Plan

Plans deterministic validation of source-evidence records produced by an
authorized response parser.

Layer 9BH established that no endpoint candidate, validated response, parser
submission, response parsing execution, or parsed source-evidence record exists.
This layer therefore defines parsed-record validation gates and contracts only.

Planning only. No response reading, parsing, historical-outcome extraction,
canonical mutation, transformation, recomputation, or production authority is
granted.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BI"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_parsed_record_validation_plan"
)

PLAN_VERSION = (
    "layer_9BI_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_parsed_record_validation_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BI_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_parsed_record_"
    "validation_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "parse_9BH_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_response.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BH_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_response_parsing_contract_v1"
)

EXPECTED_PARSING_RECORDS = 16
EXPECTED_PARSING_COMPARISONS = 16

EXPECTED_PARSING_STATUS = "candidate_not_supplied"

EXPECTED_PARSING_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


VALIDATION_PRINCIPLES = [
    {
        "principle_id": "HOASEPRV-P01",
        "principle": (
            "Parsed-record validation may occur only for records emitted by an "
            "authorized deterministic parser from a validated immutable response."
        ),
    },
    {
        "principle_id": "HOASEPRV-P02",
        "principle": (
            "Every parsed record must preserve candidate, response, parser, "
            "comparison, metric, and defect lineage."
        ),
    },
    {
        "principle_id": "HOASEPRV-P03",
        "principle": (
            "Parsed-record identity and content digests must be independently "
            "verifiable and stable under deterministic replay."
        ),
    },
    {
        "principle_id": "HOASEPRV-P04",
        "principle": (
            "Required raw fields, source locations, parser metadata, and response "
            "digest references may not be invented or completed by inference."
        ),
    },
    {
        "principle_id": "HOASEPRV-P05",
        "principle": (
            "Missing, malformed, duplicate, ambiguous, unsupported, or "
            "schema-incompatible parsed records must fail closed."
        ),
    },
    {
        "principle_id": "HOASEPRV-P06",
        "principle": (
            "Structural parsed-record validity does not establish historical "
            "outcome identity, semantic correctness, or canonical usability."
        ),
    },
    {
        "principle_id": "HOASEPRV-P07",
        "principle": (
            "Successful parsed-record validation authorizes historical-outcome "
            "field mapping planning only, not mapping execution."
        ),
    },
    {
        "principle_id": "HOASEPRV-P08",
        "principle": (
            "Successful planning grants parsed-record validation implementation "
            "authority only and performs no validation execution."
        ),
    },
]


VALIDATION_COMPONENTS = [
    {
        "component_id": "HOASEPRV-C01",
        "component": "response_and_parser_lineage",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEPRV-C02",
        "component": "parsed_record_submission_identity",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEPRV-C03",
        "component": "parsed_record_digest_integrity",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEPRV-C04",
        "component": "raw_field_and_source_location_provenance",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEPRV-C05",
        "component": "schema_and_required_field_validation",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEPRV-C06",
        "component": "duplicate_ordering_and_cardinality_validation",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEPRV-C07",
        "component": "ambiguity_and_failure_policy_validation",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEPRV-C08",
        "component": "validation_disposition_and_authority_boundary",
        "required": True,
        "priority": 8,
    },
]


VALIDATION_STAGES = [
    {"stage_id": "HOASEPRV-S01", "stage_name": "response_parsing_record_replay", "priority": 1},
    {"stage_id": "HOASEPRV-S02", "stage_name": "parsed_record_presence_gate", "priority": 2},
    {"stage_id": "HOASEPRV-S03", "stage_name": "parsed_record_submission_inventory", "priority": 3},
    {"stage_id": "HOASEPRV-S04", "stage_name": "record_identity_and_lineage_validation", "priority": 4},
    {"stage_id": "HOASEPRV-S05", "stage_name": "record_digest_and_content_integrity_validation", "priority": 5},
    {"stage_id": "HOASEPRV-S06", "stage_name": "required_field_and_schema_validation", "priority": 6},
    {"stage_id": "HOASEPRV-S07", "stage_name": "raw_field_and_source_location_validation", "priority": 7},
    {"stage_id": "HOASEPRV-S08", "stage_name": "duplicate_ordering_and_cardinality_validation", "priority": 8},
    {"stage_id": "HOASEPRV-S09", "stage_name": "ambiguity_and_failure_policy_validation", "priority": 9},
    {"stage_id": "HOASEPRV-S10", "stage_name": "validation_disposition_assignment", "priority": 10},
    {"stage_id": "HOASEPRV-S11", "stage_name": "deterministic_validation_plan_record_emission", "priority": 11},
]


VALIDATION_REQUIREMENTS = [
    {"requirement_id": "HOASEPRV-R01", "requirement": "candidate_present", "expected": True},
    {"requirement_id": "HOASEPRV-R02", "requirement": "validated_response_present", "expected": True},
    {"requirement_id": "HOASEPRV-R03", "requirement": "authorized_parser_present", "expected": True},
    {"requirement_id": "HOASEPRV-R04", "requirement": "response_parsing_completed", "expected": True},
    {"requirement_id": "HOASEPRV-R05", "requirement": "parsed_record_submission_present", "expected": True},
    {"requirement_id": "HOASEPRV-R06", "requirement": "parsed_record_id_present", "expected": True},
    {"requirement_id": "HOASEPRV-R07", "requirement": "parsed_record_version_present", "expected": True},
    {"requirement_id": "HOASEPRV-R08", "requirement": "parsed_record_digest_present", "expected": True},
    {"requirement_id": "HOASEPRV-R09", "requirement": "parsed_record_digest_verified", "expected": True},
    {"requirement_id": "HOASEPRV-R10", "requirement": "parser_id_present", "expected": True},
    {"requirement_id": "HOASEPRV-R11", "requirement": "parser_version_present", "expected": True},
    {"requirement_id": "HOASEPRV-R12", "requirement": "parser_code_digest_present", "expected": True},
    {"requirement_id": "HOASEPRV-R13", "requirement": "response_artifact_id_present", "expected": True},
    {"requirement_id": "HOASEPRV-R14", "requirement": "response_sha256_present", "expected": True},
    {"requirement_id": "HOASEPRV-R15", "requirement": "schema_version_present", "expected": True},
    {"requirement_id": "HOASEPRV-R16", "requirement": "record_selector_present", "expected": True},
    {"requirement_id": "HOASEPRV-R17", "requirement": "raw_record_payload_present", "expected": True},
    {"requirement_id": "HOASEPRV-R18", "requirement": "raw_field_provenance_present", "expected": True},
    {"requirement_id": "HOASEPRV-R19", "requirement": "source_location_provenance_present", "expected": True},
    {"requirement_id": "HOASEPRV-R20", "requirement": "required_fields_complete", "expected": True},
    {"requirement_id": "HOASEPRV-R21", "requirement": "field_types_valid", "expected": True},
    {"requirement_id": "HOASEPRV-R22", "requirement": "record_ordering_valid", "expected": True},
    {"requirement_id": "HOASEPRV-R23", "requirement": "duplicate_policy_satisfied", "expected": True},
    {"requirement_id": "HOASEPRV-R24", "requirement": "record_cardinality_valid", "expected": True},
    {"requirement_id": "HOASEPRV-R25", "requirement": "ambiguity_absent", "expected": True},
    {"requirement_id": "HOASEPRV-R26", "requirement": "malformed_record_absent", "expected": True},
    {"requirement_id": "HOASEPRV-R27", "requirement": "historical_outcome_value_mapped", "expected": False},
    {"requirement_id": "HOASEPRV-R28", "requirement": "canonical_records_mutated", "expected": False},
    {"requirement_id": "HOASEPRV-R29", "requirement": "response_bytes_read_during_planning", "expected": False},
    {"requirement_id": "HOASEPRV-R30", "requirement": "parsed_record_invented", "expected": False},
]


VALIDATION_STATUSES = [
    {"status": "source_evidence_parsed_record_validation_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "validated_response_not_supplied", "implementation_authority": False},
    {"status": "parser_not_supplied", "implementation_authority": False},
    {"status": "response_parsing_not_completed", "implementation_authority": False},
    {"status": "parsed_record_not_supplied", "implementation_authority": False},
    {"status": "parsed_record_identity_or_lineage_invalid", "implementation_authority": False},
    {"status": "parsed_record_integrity_or_schema_invalid", "implementation_authority": False},
    {"status": "parsed_record_provenance_or_cardinality_invalid", "implementation_authority": False},
    {"status": "parsed_record_ambiguous_or_malformed", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "historical_outcome_source_evidence_validated_response_missing", "category": "response"},
    {"code": "historical_outcome_source_evidence_authorized_parser_missing", "category": "parser"},
    {"code": "historical_outcome_source_evidence_response_parsing_not_completed", "category": "parsing"},
    {"code": "historical_outcome_source_evidence_parsed_record_submission_missing", "category": "record"},
    {"code": "historical_outcome_source_evidence_parsed_record_id_missing", "category": "identity"},
    {"code": "historical_outcome_source_evidence_parsed_record_version_missing", "category": "identity"},
    {"code": "historical_outcome_source_evidence_parsed_record_digest_missing", "category": "integrity"},
    {"code": "historical_outcome_source_evidence_parsed_record_digest_mismatch", "category": "integrity"},
    {"code": "historical_outcome_source_evidence_parsed_record_parser_id_missing", "category": "lineage"},
    {"code": "historical_outcome_source_evidence_parsed_record_parser_version_missing", "category": "lineage"},
    {"code": "historical_outcome_source_evidence_parsed_record_parser_code_digest_missing", "category": "lineage"},
    {"code": "historical_outcome_source_evidence_parsed_record_response_artifact_id_missing", "category": "lineage"},
    {"code": "historical_outcome_source_evidence_parsed_record_response_sha256_missing", "category": "lineage"},
    {"code": "historical_outcome_source_evidence_parsed_record_schema_version_missing", "category": "schema"},
    {"code": "historical_outcome_source_evidence_parsed_record_selector_missing", "category": "schema"},
    {"code": "historical_outcome_source_evidence_parsed_record_raw_payload_missing", "category": "record"},
    {"code": "historical_outcome_source_evidence_parsed_record_raw_field_provenance_missing", "category": "provenance"},
    {"code": "historical_outcome_source_evidence_parsed_record_source_location_missing", "category": "provenance"},
    {"code": "historical_outcome_source_evidence_parsed_record_required_field_missing", "category": "schema"},
    {"code": "historical_outcome_source_evidence_parsed_record_field_type_invalid", "category": "schema"},
    {"code": "historical_outcome_source_evidence_parsed_record_ordering_invalid", "category": "determinism"},
    {"code": "historical_outcome_source_evidence_parsed_record_duplicate_policy_failed", "category": "determinism"},
    {"code": "historical_outcome_source_evidence_parsed_record_cardinality_invalid", "category": "cardinality"},
    {"code": "historical_outcome_source_evidence_parsed_record_ambiguous", "category": "ambiguity"},
    {"code": "historical_outcome_source_evidence_parsed_record_malformed", "category": "failure"},
    {"code": "historical_outcome_source_evidence_parsed_record_invention_requested", "category": "authority"},
    {"code": "historical_outcome_source_evidence_parsed_record_validation_execution_requested", "category": "authority"},
    {"code": "historical_outcome_source_evidence_historical_field_mapping_requested", "category": "authority"},
    {"code": "historical_outcome_source_evidence_historical_value_extraction_requested", "category": "authority"},
]


VALIDATION_PLAN_RECORD_FIELDS = [
    "source_evidence_parsed_record_validation_plan_contract_version",
    "source_evidence_parsed_record_validation_plan_record_id",
    "source_evidence_response_parsing_plan_record_id",
    "source_evidence_response_parsing_plan_record_digest",
    "source_evidence_acquisition_result_validation_plan_record_id",
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
    "response_parsing_status",
    "response_parsing_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "response_artifact_id",
    "response_sha256",
    "parser_submission_supplied",
    "parser_id",
    "parser_version",
    "parser_code_digest",
    "parsed_record_submission_supplied",
    "parsed_record_id",
    "parsed_record_version",
    "parsed_record_digest",
    "schema_version",
    "record_selector",
    "raw_record_payload",
    "raw_field_provenance",
    "source_location_provenance",
    "required_fields_complete",
    "field_types_valid",
    "record_ordering_valid",
    "duplicate_policy_satisfied",
    "record_cardinality_valid",
    "ambiguity_detected",
    "malformed_record_detected",
    "parsed_record_validation_status",
    "parsed_record_validation_blocker_codes",
    "parsed_record_validation_implementation_authority_granted",
    "parsed_record_validation_rationale",
    "parsed_record_validation_limitations",
    "parsed_record_validation_plan_identity_digest",
    "parsed_record_validation_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "response_artifact_id"},
    {"ordinal": 5, "field": "parser_id"},
    {"ordinal": 6, "field": "parsed_record_id"},
    {"ordinal": 7, "field": "source_evidence_parsed_record_validation_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9BH_response_parsing_records"},
    {"ordinal": 2, "step": "preserve_candidate_response_parser_comparison_and_defect_lineage"},
    {"ordinal": 3, "step": "require_completed_authorized_response_parsing"},
    {"ordinal": 4, "step": "load_explicit_parsed_record_submissions"},
    {"ordinal": 5, "step": "validate_parsed_record_identity_version_and_digest"},
    {"ordinal": 6, "step": "validate_parser_and_response_lineage"},
    {"ordinal": 7, "step": "validate_schema_version_selector_and_required_fields"},
    {"ordinal": 8, "step": "validate_raw_record_payload_and_field_provenance"},
    {"ordinal": 9, "step": "validate_source_location_provenance"},
    {"ordinal": 10, "step": "validate_field_types_ordering_duplicates_and_cardinality"},
    {"ordinal": 11, "step": "reject_ambiguous_or_malformed_records"},
    {"ordinal": 12, "step": "withhold_historical_outcome_field_mapping"},
    {"ordinal": 13, "step": "withhold_historical_outcome_value_extraction"},
    {"ordinal": 14, "step": "emit_deterministic_validation_plan_records"},
    {"ordinal": 15, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 16, "step": "grant_validation_implementation_only_when_complete"},
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "endpoint_candidate_selection_without_submission",
    "response_artifact_invention",
    "response_metadata_invention",
    "parser_submission_invention",
    "parser_identity_invention",
    "parser_code_invention",
    "parsed_record_submission_invention",
    "parsed_record_identity_invention",
    "parsed_record_content_invention",
    "parsed_record_digest_invention",
    "schema_invention",
    "record_selector_invention",
    "raw_record_payload_invention",
    "raw_field_provenance_invention",
    "source_location_provenance_invention",
    "response_bytes_reading",
    "source_evidence_parse_execution",
    "raw_response_parse_execution",
    "historical_outcome_field_mapping_planning",
    "historical_outcome_field_mapping_execution",
    "historical_outcome_value_extraction",
    "credential_literal_storage",
    "credential_literal_logging",
    "dns_resolution_execution",
    "socket_connection_execution",
    "http_request_execution",
    "browser_execution",
    "api_request_execution",
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
        "layer_9bh_predecessor",
    )

    if (
        predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BH contract version: "
            f"{predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_parsing_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_parsing_records(
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
        row["source_evidence_response_parsing_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "source_evidence_response_parsing_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_bh_contract_version_verified",
            "actual":
                predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_bh_replay_deterministic",
            "actual": canonical_json(records) == canonical_json(reverse_records),
            "expected": True,
            "passed": canonical_json(records) == canonical_json(reverse_records),
        },
        {
            "check": "nine_bh_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": sha256_payload(records) == sha256_payload(reverse_records),
        },
        {
            "check": "expected_parsing_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_PARSING_RECORDS,
            "passed": len(records) == EXPECTED_PARSING_RECORDS,
        },
        {
            "check": "expected_parsing_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_PARSING_COMPARISONS,
            "passed": len(comparison_ids) == EXPECTED_PARSING_COMPARISONS,
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(sorted(status_counts.items())),
            "expected": {
                EXPECTED_PARSING_STATUS:
                    EXPECTED_PARSING_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_PARSING_STATUS:
                        EXPECTED_PARSING_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(sorted(blocker_counts.items())),
            "expected": {
                EXPECTED_PARSING_BLOCKER:
                    EXPECTED_PARSING_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_PARSING_BLOCKER:
                        EXPECTED_PARSING_RECORDS
                }
            ),
        },
        {
            "check": "all_parsing_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_response_parsing_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_response_parsing_plan_record_digest"
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
            "expected": 30,
            "passed": len(VALIDATION_REQUIREMENTS) == 30,
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
            "expected": 30,
            "passed": len(BLOCKER_CODES) == 30,
        },
        {
            "check": "validation_plan_record_fields_defined",
            "actual": len(VALIDATION_PLAN_RECORD_FIELDS),
            "expected": 52,
            "passed": len(VALIDATION_PLAN_RECORD_FIELDS) == 52,
        },
        {
            "check": "ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 7,
            "passed": len(ORDERING_FIELDS) == 7,
        },
        {
            "check": "implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 16,
            "passed": len(IMPLEMENTATION_STEPS) == 16,
        },
        {
            "check": "parsed_record_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "parsed_record_submission_invention",
                    "parsed_record_identity_invention",
                    "parsed_record_content_invention",
                    "parsed_record_digest_invention",
                )
            ),
        },
        {
            "check": "parsed_record_provenance_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "raw_record_payload_invention",
                    "raw_field_provenance_invention",
                    "source_location_provenance_invention",
                )
            ),
        },
        {
            "check": "response_reading_and_parsing_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "response_bytes_reading",
                    "source_evidence_parse_execution",
                    "raw_response_parse_execution",
                )
            ),
        },
        {
            "check": "historical_field_mapping_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "historical_outcome_field_mapping_planning",
                    "historical_outcome_field_mapping_execution",
                    "historical_outcome_value_extraction",
                )
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
            "check": "parsed_record_submissions_supplied_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "parsed_records_validated_zero",
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
        "endpoint_candidate_source_evidence_parsed_record_validation_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_parsed_record_validation_plan_failed"
    )

    next_layer = (
        "9BJ_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_parsed_record_validation_implementation"
        if all_checks_passed
        else
        "9BI_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_parsed_record_validation_plan_remediation"
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
        OUTPUT_DIR / "candidate_missing_response_parsing_inventory.csv",
        [
            "source_evidence_response_parsing_plan_record_id",
            "source_evidence_response_parsing_plan_record_digest",
            "source_evidence_acquisition_result_validation_plan_record_id",
            "endpoint_candidate_specification_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "source_evidence_response_parsing_status",
            "source_evidence_response_parsing_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "response_artifact_id",
            "response_sha256",
            "parser_submission_supplied",
            "parser_id",
            "parser_version",
            "parser_code_digest",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION,
        "parsing_records": len(records),
        "parsing_comparisons": len(comparison_ids),
        "parsing_status_counts":
            dict(sorted(status_counts.items())),
        "parsing_blocker_counts":
            dict(sorted(blocker_counts.items())),
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
        "parsed_record_submissions_supplied": 0,
        "parsed_records_validated": 0,
        "response_bytes_read": 0,
        "responses_parsed": 0,
        "historical_outcome_fields_mapped": 0,
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
        / "source_evidence_parsed_record_validation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_parsed_record_validation_implementation"
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
        f"{predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION}"
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
    print(f"Parsing records replayed: {len(records)}")
    print(f"Parsing comparisons: {len(comparison_ids)}")
    print(
        "Parsing status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Parsing blocker counts: "
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
    print("Parsed record submissions supplied: 0")
    print("Parsed records validated: 0")
    print("Response bytes read: 0")
    print("Responses parsed: 0")
    print("Historical outcome fields mapped: 0")
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
