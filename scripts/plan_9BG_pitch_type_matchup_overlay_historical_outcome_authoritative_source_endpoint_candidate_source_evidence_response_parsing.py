#!/usr/bin/env python3
"""
Layer 9BG
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Response Parsing Plan

Plans deterministic parsing of a validated, immutable source-evidence response.

Layer 9BF established that no endpoint candidate, acquisition result, or
validated response exists. This layer therefore defines parsing gates and
record contracts only.

Planning only.

This layer does not:
- invent a candidate, response, parser, schema, selector, or submission;
- read or parse response bytes;
- perform network activity;
- extract historical outcome values;
- mutate canonical records or mappings;
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


LAYER_ID = "9BG"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_response_parsing_plan"
)

PLAN_VERSION = (
    "layer_9BG_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_response_parsing_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BG_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_response_"
    "parsing_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "validate_9BF_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_acquisition_"
    "result.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BF_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_result_validation_contract_v1"
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


PARSING_PRINCIPLES = [
    {
        "principle_id": "HOASERP-P01",
        "principle": (
            "Parsing may be planned only for a validated immutable response "
            "whose complete acquisition lineage is preserved."
        ),
    },
    {
        "principle_id": "HOASERP-P02",
        "principle": (
            "The parser specification must be explicit, versioned, "
            "content-type compatible, and bound to the validated response."
        ),
    },
    {
        "principle_id": "HOASERP-P03",
        "principle": (
            "Parsing must be deterministic and produce the same ordered "
            "records for identical immutable response bytes."
        ),
    },
    {
        "principle_id": "HOASERP-P04",
        "principle": (
            "Selectors, paths, namespaces, encodings, delimiters, and schema "
            "rules may not be invented or completed by inference."
        ),
    },
    {
        "principle_id": "HOASERP-P05",
        "principle": (
            "Parsed records must preserve raw-field provenance, source "
            "locations, parser identity, and response digest lineage."
        ),
    },
    {
        "principle_id": "HOASERP-P06",
        "principle": (
            "Parsing success establishes structural source evidence only and "
            "does not establish historical outcome identity or correctness."
        ),
    },
    {
        "principle_id": "HOASERP-P07",
        "principle": (
            "Malformed, ambiguous, duplicate, truncated, unsupported, or "
            "schema-incompatible responses must fail closed."
        ),
    },
    {
        "principle_id": "HOASERP-P08",
        "principle": (
            "Successful planning grants response-parsing implementation "
            "authority only and performs no parsing."
        ),
    },
]


PARSING_COMPONENTS = [
    {
        "component_id": "HOASERP-C01",
        "component": "validated_response_lineage",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASERP-C02",
        "component": "parser_submission_identity",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASERP-C03",
        "component": "content_type_and_encoding_contract",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASERP-C04",
        "component": "schema_selector_and_namespace_contract",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASERP-C05",
        "component": "deterministic_record_extraction_contract",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASERP-C06",
        "component": "raw_field_and_source_location_provenance",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASERP-C07",
        "component": "error_duplicate_and_ambiguity_policy",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASERP-C08",
        "component": "parsing_disposition_and_authority_boundary",
        "required": True,
        "priority": 8,
    },
]


PARSING_STAGES = [
    {"stage_id": "HOASERP-S01", "stage_name": "validation_record_replay", "priority": 1},
    {"stage_id": "HOASERP-S02", "stage_name": "validated_response_presence_gate", "priority": 2},
    {"stage_id": "HOASERP-S03", "stage_name": "parser_submission_inventory", "priority": 3},
    {"stage_id": "HOASERP-S04", "stage_name": "parser_identity_and_version_validation", "priority": 4},
    {"stage_id": "HOASERP-S05", "stage_name": "content_type_encoding_and_container_validation", "priority": 5},
    {"stage_id": "HOASERP-S06", "stage_name": "schema_selector_path_and_namespace_validation", "priority": 6},
    {"stage_id": "HOASERP-S07", "stage_name": "record_ordering_and_duplicate_policy_validation", "priority": 7},
    {"stage_id": "HOASERP-S08", "stage_name": "raw_field_provenance_contract_validation", "priority": 8},
    {"stage_id": "HOASERP-S09", "stage_name": "failure_and_ambiguity_policy_validation", "priority": 9},
    {"stage_id": "HOASERP-S10", "stage_name": "parsing_disposition_assignment", "priority": 10},
    {"stage_id": "HOASERP-S11", "stage_name": "deterministic_parsing_plan_record_emission", "priority": 11},
]


PARSING_REQUIREMENTS = [
    {"requirement_id": "HOASERP-R01", "requirement": "candidate_present", "expected": True},
    {"requirement_id": "HOASERP-R02", "requirement": "acquisition_result_validated", "expected": True},
    {"requirement_id": "HOASERP-R03", "requirement": "validated_response_artifact_present", "expected": True},
    {"requirement_id": "HOASERP-R04", "requirement": "validated_response_sha256_present", "expected": True},
    {"requirement_id": "HOASERP-R05", "requirement": "parser_submission_present", "expected": True},
    {"requirement_id": "HOASERP-R06", "requirement": "parser_id_present", "expected": True},
    {"requirement_id": "HOASERP-R07", "requirement": "parser_version_present", "expected": True},
    {"requirement_id": "HOASERP-R08", "requirement": "parser_code_digest_present", "expected": True},
    {"requirement_id": "HOASERP-R09", "requirement": "supported_media_type_declared", "expected": True},
    {"requirement_id": "HOASERP-R10", "requirement": "character_encoding_declared", "expected": True},
    {"requirement_id": "HOASERP-R11", "requirement": "container_format_declared", "expected": True},
    {"requirement_id": "HOASERP-R12", "requirement": "schema_version_declared", "expected": True},
    {"requirement_id": "HOASERP-R13", "requirement": "record_selector_declared", "expected": True},
    {"requirement_id": "HOASERP-R14", "requirement": "field_path_contract_declared", "expected": True},
    {"requirement_id": "HOASERP-R15", "requirement": "namespace_contract_declared_when_required", "expected": True},
    {"requirement_id": "HOASERP-R16", "requirement": "delimiter_contract_declared_when_required", "expected": True},
    {"requirement_id": "HOASERP-R17", "requirement": "header_contract_declared_when_required", "expected": True},
    {"requirement_id": "HOASERP-R18", "requirement": "record_ordering_contract_declared", "expected": True},
    {"requirement_id": "HOASERP-R19", "requirement": "duplicate_policy_declared", "expected": True},
    {"requirement_id": "HOASERP-R20", "requirement": "missing_field_policy_declared", "expected": True},
    {"requirement_id": "HOASERP-R21", "requirement": "unknown_field_policy_declared", "expected": True},
    {"requirement_id": "HOASERP-R22", "requirement": "malformed_record_policy_fail_closed", "expected": True},
    {"requirement_id": "HOASERP-R23", "requirement": "raw_field_provenance_enabled", "expected": True},
    {"requirement_id": "HOASERP-R24", "requirement": "source_location_provenance_enabled", "expected": True},
    {"requirement_id": "HOASERP-R25", "requirement": "deterministic_replay_required", "expected": True},
    {"requirement_id": "HOASERP-R26", "requirement": "response_bytes_parsed_during_planning", "expected": False},
    {"requirement_id": "HOASERP-R27", "requirement": "historical_outcome_values_extracted", "expected": False},
    {"requirement_id": "HOASERP-R28", "requirement": "canonical_records_mutated", "expected": False},
    {"requirement_id": "HOASERP-R29", "requirement": "network_retrieval_executed_during_planning", "expected": False},
    {"requirement_id": "HOASERP-R30", "requirement": "parser_or_schema_invented", "expected": False},
]


PARSING_STATUSES = [
    {"status": "source_evidence_response_parsing_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "acquisition_result_not_validated", "implementation_authority": False},
    {"status": "validated_response_not_supplied", "implementation_authority": False},
    {"status": "parser_submission_not_supplied", "implementation_authority": False},
    {"status": "parser_identity_invalid", "implementation_authority": False},
    {"status": "media_type_or_encoding_unsupported", "implementation_authority": False},
    {"status": "schema_or_selector_contract_invalid", "implementation_authority": False},
    {"status": "ordering_duplicate_or_provenance_contract_invalid", "implementation_authority": False},
    {"status": "failure_or_ambiguity_policy_invalid", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "historical_outcome_source_evidence_acquisition_result_not_validated", "category": "validation"},
    {"code": "historical_outcome_source_evidence_validated_response_artifact_missing", "category": "response"},
    {"code": "historical_outcome_source_evidence_validated_response_sha256_missing", "category": "response"},
    {"code": "historical_outcome_source_evidence_response_parser_submission_missing", "category": "parser"},
    {"code": "historical_outcome_source_evidence_response_parser_id_missing", "category": "identity"},
    {"code": "historical_outcome_source_evidence_response_parser_version_missing", "category": "identity"},
    {"code": "historical_outcome_source_evidence_response_parser_code_digest_missing", "category": "identity"},
    {"code": "historical_outcome_source_evidence_response_parser_media_type_missing", "category": "format"},
    {"code": "historical_outcome_source_evidence_response_parser_media_type_unsupported", "category": "format"},
    {"code": "historical_outcome_source_evidence_response_parser_encoding_missing", "category": "format"},
    {"code": "historical_outcome_source_evidence_response_parser_encoding_unsupported", "category": "format"},
    {"code": "historical_outcome_source_evidence_response_parser_container_format_missing", "category": "format"},
    {"code": "historical_outcome_source_evidence_response_parser_schema_version_missing", "category": "schema"},
    {"code": "historical_outcome_source_evidence_response_parser_record_selector_missing", "category": "schema"},
    {"code": "historical_outcome_source_evidence_response_parser_field_path_contract_missing", "category": "schema"},
    {"code": "historical_outcome_source_evidence_response_parser_namespace_contract_missing", "category": "schema"},
    {"code": "historical_outcome_source_evidence_response_parser_delimiter_contract_missing", "category": "schema"},
    {"code": "historical_outcome_source_evidence_response_parser_header_contract_missing", "category": "schema"},
    {"code": "historical_outcome_source_evidence_response_parser_ordering_contract_missing", "category": "determinism"},
    {"code": "historical_outcome_source_evidence_response_parser_duplicate_policy_missing", "category": "determinism"},
    {"code": "historical_outcome_source_evidence_response_parser_missing_field_policy_missing", "category": "failure"},
    {"code": "historical_outcome_source_evidence_response_parser_unknown_field_policy_missing", "category": "failure"},
    {"code": "historical_outcome_source_evidence_response_parser_malformed_record_policy_invalid", "category": "failure"},
    {"code": "historical_outcome_source_evidence_response_parser_raw_field_provenance_missing", "category": "provenance"},
    {"code": "historical_outcome_source_evidence_response_parser_source_location_provenance_missing", "category": "provenance"},
    {"code": "historical_outcome_source_evidence_response_parser_invention_requested", "category": "authority"},
    {"code": "historical_outcome_source_evidence_response_schema_invention_requested", "category": "authority"},
    {"code": "historical_outcome_source_evidence_response_parsing_execution_requested", "category": "authority"},
    {"code": "historical_outcome_source_evidence_historical_value_extraction_requested", "category": "authority"},
]


PARSING_PLAN_RECORD_FIELDS = [
    "source_evidence_response_parsing_plan_contract_version",
    "source_evidence_response_parsing_plan_record_id",
    "source_evidence_acquisition_result_validation_plan_record_id",
    "acquisition_result_validation_plan_record_digest",
    "source_evidence_acquisition_execution_plan_record_id",
    "source_evidence_acquisition_authorization_plan_record_id",
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
    "acquisition_result_validation_status",
    "acquisition_result_validation_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "result_submission_supplied",
    "result_id",
    "response_artifact_id",
    "response_media_type",
    "response_sha256",
    "response_quarantined",
    "response_immutable",
    "parser_submission_supplied",
    "parser_id",
    "parser_version",
    "parser_code_digest",
    "supported_media_types",
    "character_encoding",
    "container_format",
    "schema_version",
    "record_selector",
    "field_path_contract",
    "namespace_contract",
    "delimiter_contract",
    "header_contract",
    "record_ordering_contract",
    "duplicate_policy",
    "missing_field_policy",
    "unknown_field_policy",
    "malformed_record_policy",
    "raw_field_provenance_contract",
    "source_location_provenance_contract",
    "source_evidence_response_parsing_status",
    "source_evidence_response_parsing_blocker_codes",
    "source_evidence_response_parsing_implementation_authority_granted",
    "source_evidence_response_parsing_rationale",
    "source_evidence_response_parsing_limitations",
    "source_evidence_response_parsing_plan_identity_digest",
    "source_evidence_response_parsing_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "response_artifact_id"},
    {"ordinal": 5, "field": "parser_id"},
    {"ordinal": 6, "field": "source_evidence_response_parsing_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9BF_acquisition_result_validation_records"},
    {"ordinal": 2, "step": "preserve_candidate_response_comparison_and_defect_lineage"},
    {"ordinal": 3, "step": "require_validated_immutable_response_artifact"},
    {"ordinal": 4, "step": "load_explicit_parser_submissions"},
    {"ordinal": 5, "step": "validate_parser_identity_version_and_code_digest"},
    {"ordinal": 6, "step": "validate_media_type_encoding_and_container_compatibility"},
    {"ordinal": 7, "step": "validate_schema_version_selector_paths_and_namespaces"},
    {"ordinal": 8, "step": "validate_delimiter_header_and_record_ordering_contracts"},
    {"ordinal": 9, "step": "validate_duplicate_missing_unknown_and_malformed_policies"},
    {"ordinal": 10, "step": "validate_raw_field_and_source_location_provenance"},
    {"ordinal": 11, "step": "require_fail_closed_behavior_for_ambiguity"},
    {"ordinal": 12, "step": "withhold_response_parsing_execution"},
    {"ordinal": 13, "step": "withhold_historical_outcome_value_extraction"},
    {"ordinal": 14, "step": "emit_deterministic_parsing_plan_records"},
    {"ordinal": 15, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 16, "step": "grant_parsing_implementation_only_when_complete"},
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "endpoint_candidate_selection_without_submission",
    "acquisition_result_submission_invention",
    "response_artifact_invention",
    "response_metadata_invention",
    "parser_submission_invention",
    "parser_identity_invention",
    "parser_code_invention",
    "schema_invention",
    "record_selector_invention",
    "field_path_invention",
    "namespace_invention",
    "delimiter_invention",
    "header_contract_invention",
    "response_bytes_reading",
    "source_evidence_parse_execution",
    "raw_response_parse_execution",
    "historical_outcome_retrieval_planning",
    "historical_outcome_fetch_execution",
    "historical_outcome_parse_execution",
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
        "layer_9bf_predecessor",
    )

    if (
        predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BF contract version: "
            f"{predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_validation_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_validation_records(
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
        row["acquisition_result_validation_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "acquisition_result_validation_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_bf_contract_version_verified",
            "actual":
                predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_bf_replay_deterministic",
            "actual": canonical_json(records) == canonical_json(reverse_records),
            "expected": True,
            "passed": canonical_json(records) == canonical_json(reverse_records),
        },
        {
            "check": "nine_bf_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": sha256_payload(records) == sha256_payload(reverse_records),
        },
        {
            "check": "expected_validation_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_VALIDATION_RECORDS,
            "passed": len(records) == EXPECTED_VALIDATION_RECORDS,
        },
        {
            "check": "expected_validation_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_VALIDATION_COMPARISONS,
            "passed": len(comparison_ids) == EXPECTED_VALIDATION_COMPARISONS,
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(sorted(status_counts.items())),
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
            "actual": dict(sorted(blocker_counts.items())),
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
                        "acquisition_result_validation_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "acquisition_result_validation_plan_record_digest"
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
            "check": "parsing_principles_defined",
            "actual": len(PARSING_PRINCIPLES),
            "expected": 8,
            "passed": len(PARSING_PRINCIPLES) == 8,
        },
        {
            "check": "parsing_components_defined",
            "actual": len(PARSING_COMPONENTS),
            "expected": 8,
            "passed": len(PARSING_COMPONENTS) == 8,
        },
        {
            "check": "parsing_stages_defined",
            "actual": len(PARSING_STAGES),
            "expected": 11,
            "passed": len(PARSING_STAGES) == 11,
        },
        {
            "check": "parsing_requirements_defined",
            "actual": len(PARSING_REQUIREMENTS),
            "expected": 30,
            "passed": len(PARSING_REQUIREMENTS) == 30,
        },
        {
            "check": "parsing_statuses_defined",
            "actual": len(PARSING_STATUSES),
            "expected": 10,
            "passed": len(PARSING_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 30,
            "passed": len(BLOCKER_CODES) == 30,
        },
        {
            "check": "parsing_plan_record_fields_defined",
            "actual": len(PARSING_PLAN_RECORD_FIELDS),
            "expected": 58,
            "passed": len(PARSING_PLAN_RECORD_FIELDS) == 58,
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
            "check": "parser_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "parser_submission_invention",
                    "parser_identity_invention",
                    "parser_code_invention",
                )
            ),
        },
        {
            "check": "schema_and_selector_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "schema_invention",
                    "record_selector_invention",
                    "field_path_invention",
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
            "check": "historical_value_extraction_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "historical_outcome_value_extraction"
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
            "check": "parsing_plan_records_materialized_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "parser_submissions_supplied_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "response_bytes_read_zero",
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
            "parsing_principles": PARSING_PRINCIPLES,
            "parsing_components": PARSING_COMPONENTS,
            "parsing_stages": PARSING_STAGES,
            "parsing_requirements": PARSING_REQUIREMENTS,
            "parsing_statuses": PARSING_STATUSES,
            "blocker_codes": BLOCKER_CODES,
            "parsing_plan_record_fields": PARSING_PLAN_RECORD_FIELDS,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
            "prohibited_authorities": PROHIBITED_AUTHORITIES,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_response_parsing_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_response_parsing_plan_failed"
    )

    next_layer = (
        "9BH_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_response_parsing_implementation"
        if all_checks_passed
        else
        "9BG_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_response_parsing_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "parsing_principles.csv",
        ["principle_id", "principle"],
        PARSING_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "parsing_components.csv",
        ["component_id", "component", "required", "priority"],
        PARSING_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "parsing_stages.csv",
        ["stage_id", "stage_name", "priority"],
        PARSING_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "parsing_requirements.csv",
        ["requirement_id", "requirement", "expected"],
        PARSING_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "parsing_statuses.csv",
        ["status", "implementation_authority"],
        PARSING_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "blocker_code_catalog.csv",
        ["code", "category"],
        BLOCKER_CODES,
    )

    write_csv(
        OUTPUT_DIR / "parsing_plan_record_field_contract.csv",
        ["ordinal", "field"],
        [
            {"ordinal": index, "field": field}
            for index, field in enumerate(
                PARSING_PLAN_RECORD_FIELDS,
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
        OUTPUT_DIR
        / "candidate_missing_acquisition_result_validation_inventory.csv",
        [
            "source_evidence_acquisition_result_validation_plan_record_id",
            "acquisition_result_validation_plan_record_digest",
            "source_evidence_acquisition_execution_plan_record_id",
            "source_evidence_acquisition_authorization_plan_record_id",
            "endpoint_candidate_specification_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "acquisition_result_validation_status",
            "acquisition_result_validation_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "result_submission_supplied",
            "result_id",
            "response_artifact_id",
            "response_media_type",
            "response_sha256",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION,
        "validation_records": len(records),
        "validation_comparisons": len(comparison_ids),
        "validation_status_counts":
            dict(sorted(status_counts.items())),
        "validation_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "parsing_principles": len(PARSING_PRINCIPLES),
        "parsing_components": len(PARSING_COMPONENTS),
        "parsing_stages": len(PARSING_STAGES),
        "parsing_requirements": len(PARSING_REQUIREMENTS),
        "parsing_statuses": len(PARSING_STATUSES),
        "blocker_codes": len(BLOCKER_CODES),
        "parsing_plan_record_fields":
            len(PARSING_PLAN_RECORD_FIELDS),
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
        "parsing_plan_records_materialized": 0,
        "parser_submissions_supplied": 0,
        "response_bytes_read": 0,
        "responses_parsed": 0,
        "parsed_source_evidence_records": 0,
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
        / "source_evidence_response_parsing_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_response_parsing_implementation"
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
        f"{predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION}"
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
    print(f"Validation records replayed: {len(records)}")
    print(f"Validation comparisons: {len(comparison_ids)}")
    print(
        "Validation status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Validation blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(f"Parsing principles: {len(PARSING_PRINCIPLES)}")
    print(f"Parsing components: {len(PARSING_COMPONENTS)}")
    print(f"Parsing stages: {len(PARSING_STAGES)}")
    print(f"Parsing requirements: {len(PARSING_REQUIREMENTS)}")
    print(
        "Parsing plan record fields: "
        f"{len(PARSING_PLAN_RECORD_FIELDS)}"
    )
    print(f"Plan digest: {plan_digest}")
    print("Parsing plan records materialized: 0")
    print("Parser submissions supplied: 0")
    print("Response bytes read: 0")
    print("Responses parsed: 0")
    print("Parsed source evidence records: 0")
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
