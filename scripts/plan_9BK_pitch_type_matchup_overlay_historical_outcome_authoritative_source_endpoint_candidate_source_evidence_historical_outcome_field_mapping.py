#!/usr/bin/env python3
"""
Layer 9BK
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Historical Outcome Field Mapping Plan

Plans deterministic mapping from validated source-evidence parsed records to
the canonical historical outcome field.

Layer 9BJ established that no endpoint candidate, validated response, parser,
parsed-record submission, or validated parsed record exists. This layer
therefore defines mapping gates and contracts only.

Planning only.

This layer does not:
- invent a candidate, response, parser, parsed record, source field, mapping,
  conversion, normalization, provenance record, or submission;
- map or extract historical outcome values;
- read response bytes or perform parsing;
- mutate canonical records or mappings;
- transform, infer, default, impute, coerce, or substitute values;
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


LAYER_ID = "9BK"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_plan"
)

PLAN_VERSION = (
    "layer_9BK_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BK_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "validate_9BJ_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_parsed_record.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BJ_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_parsed_record_validation_contract_v1"
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


MAPPING_PRINCIPLES = [
    {
        "principle_id": "HOASEHOFM-P01",
        "principle": (
            "Historical outcome field mapping may be planned only for a "
            "validated parsed source-evidence record with complete lineage."
        ),
    },
    {
        "principle_id": "HOASEHOFM-P02",
        "principle": (
            "The source field, target field, source type, target type, and "
            "mapping rule must be explicit and versioned."
        ),
    },
    {
        "principle_id": "HOASEHOFM-P03",
        "principle": (
            "Mapping must preserve candidate, response, parser, parsed-record, "
            "comparison, metric, and defect provenance."
        ),
    },
    {
        "principle_id": "HOASEHOFM-P04",
        "principle": (
            "No source field, selector, conversion, normalization, or semantic "
            "interpretation may be invented or inferred."
        ),
    },
    {
        "principle_id": "HOASEHOFM-P05",
        "principle": (
            "The rejected metadata field outcome_available_at_utc may not be "
            "substituted for the canonical outcome_value field."
        ),
    },
    {
        "principle_id": "HOASEHOFM-P06",
        "principle": (
            "Boolean-to-integer coercion, defaulting, imputation, inversion, "
            "and truthiness interpretation are prohibited."
        ),
    },
    {
        "principle_id": "HOASEHOFM-P07",
        "principle": (
            "Mapping readiness does not authorize canonical mutation, "
            "historical evaluation recomputation, or metric recomputation."
        ),
    },
    {
        "principle_id": "HOASEHOFM-P08",
        "principle": (
            "Successful planning grants field-mapping implementation authority "
            "only and performs no mapping."
        ),
    },
]


MAPPING_COMPONENTS = [
    {
        "component_id": "HOASEHOFM-C01",
        "component": "validated_parsed_record_lineage",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEHOFM-C02",
        "component": "mapping_submission_identity",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEHOFM-C03",
        "component": "source_field_contract",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEHOFM-C04",
        "component": "canonical_target_field_contract",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEHOFM-C05",
        "component": "type_and_value_domain_contract",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEHOFM-C06",
        "component": "deterministic_mapping_rule_contract",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEHOFM-C07",
        "component": "provenance_and_failure_policy",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEHOFM-C08",
        "component": "mapping_disposition_and_authority_boundary",
        "required": True,
        "priority": 8,
    },
]


MAPPING_STAGES = [
    {"stage_id": "HOASEHOFM-S01", "stage_name": "parsed_record_validation_replay", "priority": 1},
    {"stage_id": "HOASEHOFM-S02", "stage_name": "validated_parsed_record_presence_gate", "priority": 2},
    {"stage_id": "HOASEHOFM-S03", "stage_name": "mapping_submission_inventory", "priority": 3},
    {"stage_id": "HOASEHOFM-S04", "stage_name": "mapping_identity_and_version_validation", "priority": 4},
    {"stage_id": "HOASEHOFM-S05", "stage_name": "source_field_and_source_type_validation", "priority": 5},
    {"stage_id": "HOASEHOFM-S06", "stage_name": "target_field_and_target_type_validation", "priority": 6},
    {"stage_id": "HOASEHOFM-S07", "stage_name": "value_domain_and_null_policy_validation", "priority": 7},
    {"stage_id": "HOASEHOFM-S08", "stage_name": "conversion_and_normalization_policy_validation", "priority": 8},
    {"stage_id": "HOASEHOFM-S09", "stage_name": "provenance_and_failure_policy_validation", "priority": 9},
    {"stage_id": "HOASEHOFM-S10", "stage_name": "mapping_disposition_assignment", "priority": 10},
    {"stage_id": "HOASEHOFM-S11", "stage_name": "deterministic_mapping_plan_record_emission", "priority": 11},
]


MAPPING_REQUIREMENTS = [
    {"requirement_id": "HOASEHOFM-R01", "requirement": "candidate_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R02", "requirement": "validated_response_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R03", "requirement": "authorized_parser_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R04", "requirement": "validated_parsed_record_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R05", "requirement": "mapping_submission_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R06", "requirement": "mapping_id_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R07", "requirement": "mapping_version_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R08", "requirement": "mapping_digest_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R09", "requirement": "source_field_name_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R10", "requirement": "source_field_path_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R11", "requirement": "source_field_type_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R12", "requirement": "source_value_domain_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R13", "requirement": "target_field_name_is_outcome_value", "expected": True},
    {"requirement_id": "HOASEHOFM-R14", "requirement": "target_field_path_is_canonical", "expected": True},
    {"requirement_id": "HOASEHOFM-R15", "requirement": "target_field_type_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R16", "requirement": "target_value_domain_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R17", "requirement": "mapping_rule_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R18", "requirement": "mapping_rule_deterministic", "expected": True},
    {"requirement_id": "HOASEHOFM-R19", "requirement": "null_policy_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R20", "requirement": "invalid_value_policy_fail_closed", "expected": True},
    {"requirement_id": "HOASEHOFM-R21", "requirement": "source_field_provenance_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R22", "requirement": "mapping_provenance_present", "expected": True},
    {"requirement_id": "HOASEHOFM-R23", "requirement": "rejected_metadata_substitution_prohibited", "expected": True},
    {"requirement_id": "HOASEHOFM-R24", "requirement": "boolean_to_integer_coercion_prohibited", "expected": True},
    {"requirement_id": "HOASEHOFM-R25", "requirement": "defaulting_inference_imputation_prohibited", "expected": True},
    {"requirement_id": "HOASEHOFM-R26", "requirement": "mapping_execution_during_planning", "expected": False},
    {"requirement_id": "HOASEHOFM-R27", "requirement": "historical_outcome_values_extracted", "expected": False},
    {"requirement_id": "HOASEHOFM-R28", "requirement": "canonical_records_mutated", "expected": False},
    {"requirement_id": "HOASEHOFM-R29", "requirement": "downstream_records_recomputed", "expected": False},
    {"requirement_id": "HOASEHOFM-R30", "requirement": "mapping_submission_invented", "expected": False},
]


MAPPING_STATUSES = [
    {"status": "historical_outcome_field_mapping_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "validated_response_not_supplied", "implementation_authority": False},
    {"status": "parser_not_supplied", "implementation_authority": False},
    {"status": "validated_parsed_record_not_supplied", "implementation_authority": False},
    {"status": "mapping_submission_not_supplied", "implementation_authority": False},
    {"status": "mapping_identity_invalid", "implementation_authority": False},
    {"status": "source_or_target_field_contract_invalid", "implementation_authority": False},
    {"status": "mapping_rule_or_value_domain_invalid", "implementation_authority": False},
    {"status": "mapping_provenance_or_failure_policy_invalid", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "historical_outcome_source_evidence_validated_response_missing", "category": "response"},
    {"code": "historical_outcome_source_evidence_authorized_parser_missing", "category": "parser"},
    {"code": "historical_outcome_source_evidence_validated_parsed_record_missing", "category": "record"},
    {"code": "historical_outcome_field_mapping_submission_missing", "category": "mapping"},
    {"code": "historical_outcome_field_mapping_id_missing", "category": "identity"},
    {"code": "historical_outcome_field_mapping_version_missing", "category": "identity"},
    {"code": "historical_outcome_field_mapping_digest_missing", "category": "integrity"},
    {"code": "historical_outcome_field_mapping_source_field_name_missing", "category": "source"},
    {"code": "historical_outcome_field_mapping_source_field_path_missing", "category": "source"},
    {"code": "historical_outcome_field_mapping_source_field_type_missing", "category": "source"},
    {"code": "historical_outcome_field_mapping_source_value_domain_missing", "category": "source"},
    {"code": "historical_outcome_field_mapping_target_field_name_invalid", "category": "target"},
    {"code": "historical_outcome_field_mapping_target_field_path_invalid", "category": "target"},
    {"code": "historical_outcome_field_mapping_target_field_type_missing", "category": "target"},
    {"code": "historical_outcome_field_mapping_target_value_domain_missing", "category": "target"},
    {"code": "historical_outcome_field_mapping_rule_missing", "category": "mapping"},
    {"code": "historical_outcome_field_mapping_rule_nondeterministic", "category": "mapping"},
    {"code": "historical_outcome_field_mapping_null_policy_missing", "category": "failure"},
    {"code": "historical_outcome_field_mapping_invalid_value_policy_not_fail_closed", "category": "failure"},
    {"code": "historical_outcome_field_mapping_source_field_provenance_missing", "category": "provenance"},
    {"code": "historical_outcome_field_mapping_provenance_missing", "category": "provenance"},
    {"code": "historical_outcome_field_mapping_rejected_metadata_substitution_requested", "category": "authority"},
    {"code": "historical_outcome_field_mapping_boolean_to_integer_coercion_requested", "category": "authority"},
    {"code": "historical_outcome_field_mapping_defaulting_requested", "category": "authority"},
    {"code": "historical_outcome_field_mapping_inference_requested", "category": "authority"},
    {"code": "historical_outcome_field_mapping_imputation_requested", "category": "authority"},
    {"code": "historical_outcome_field_mapping_submission_invention_requested", "category": "authority"},
    {"code": "historical_outcome_field_mapping_execution_requested", "category": "authority"},
    {"code": "historical_outcome_value_extraction_requested", "category": "authority"},
]


MAPPING_PLAN_RECORD_FIELDS = [
    "historical_outcome_field_mapping_plan_contract_version",
    "historical_outcome_field_mapping_plan_record_id",
    "source_evidence_parsed_record_validation_plan_record_id",
    "parsed_record_validation_plan_record_digest",
    "source_evidence_response_parsing_plan_record_id",
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
    "parsed_record_validation_status",
    "parsed_record_validation_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "response_artifact_id",
    "response_sha256",
    "parser_id",
    "parser_version",
    "parser_code_digest",
    "parsed_record_submission_supplied",
    "parsed_record_id",
    "parsed_record_version",
    "parsed_record_digest",
    "mapping_submission_supplied",
    "mapping_id",
    "mapping_version",
    "mapping_digest",
    "source_field_name",
    "source_field_path",
    "source_field_type",
    "source_value_domain",
    "target_field_name",
    "target_field_path",
    "target_field_type",
    "target_value_domain",
    "mapping_rule",
    "null_policy",
    "invalid_value_policy",
    "source_field_provenance",
    "mapping_provenance",
    "historical_outcome_field_mapping_status",
    "historical_outcome_field_mapping_blocker_codes",
    "historical_outcome_field_mapping_implementation_authority_granted",
    "historical_outcome_field_mapping_rationale",
    "historical_outcome_field_mapping_limitations",
    "historical_outcome_field_mapping_plan_identity_digest",
    "historical_outcome_field_mapping_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "response_artifact_id"},
    {"ordinal": 5, "field": "parser_id"},
    {"ordinal": 6, "field": "parsed_record_id"},
    {"ordinal": 7, "field": "mapping_id"},
    {"ordinal": 8, "field": "historical_outcome_field_mapping_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9BJ_parsed_record_validation_records"},
    {"ordinal": 2, "step": "preserve_candidate_response_parser_record_comparison_and_defect_lineage"},
    {"ordinal": 3, "step": "require_validated_parsed_record"},
    {"ordinal": 4, "step": "load_explicit_field_mapping_submissions"},
    {"ordinal": 5, "step": "validate_mapping_identity_version_and_digest"},
    {"ordinal": 6, "step": "validate_source_field_name_path_type_and_domain"},
    {"ordinal": 7, "step": "validate_canonical_target_field_name_path_type_and_domain"},
    {"ordinal": 8, "step": "validate_deterministic_mapping_rule"},
    {"ordinal": 9, "step": "validate_null_and_invalid_value_fail_closed_policies"},
    {"ordinal": 10, "step": "validate_source_field_and_mapping_provenance"},
    {"ordinal": 11, "step": "reject_rejected_metadata_substitution"},
    {"ordinal": 12, "step": "reject_coercion_defaulting_inference_and_imputation"},
    {"ordinal": 13, "step": "withhold_mapping_execution_and_value_extraction"},
    {"ordinal": 14, "step": "emit_deterministic_mapping_plan_records"},
    {"ordinal": 15, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 16, "step": "grant_mapping_implementation_only_when_complete"},
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
    "mapping_submission_invention",
    "mapping_identity_invention",
    "mapping_rule_invention",
    "source_field_name_invention",
    "source_field_path_invention",
    "source_field_type_invention",
    "source_value_domain_invention",
    "target_field_type_invention",
    "target_value_domain_invention",
    "source_field_provenance_invention",
    "mapping_provenance_invention",
    "rejected_metadata_field_substitution",
    "boolean_to_integer_coercion",
    "source_value_defaulting",
    "source_value_inference",
    "source_value_imputation",
    "historical_outcome_field_mapping_execution",
    "historical_outcome_value_extraction",
    "response_bytes_reading",
    "source_evidence_parse_execution",
    "raw_response_parse_execution",
    "credential_literal_storage",
    "credential_literal_logging",
    "dns_resolution_execution",
    "socket_connection_execution",
    "http_request_execution",
    "browser_execution",
    "api_request_execution",
    "canonical_source_value_mutation",
    "canonical_outcome_mapping_change",
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
        "layer_9bj_predecessor",
    )

    if (
        predecessor.PARSED_RECORD_VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BJ contract version: "
            f"{predecessor.PARSED_RECORD_VALIDATION_CONTRACT_VERSION}"
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
        row["parsed_record_validation_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "parsed_record_validation_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_bj_contract_version_verified",
            "actual":
                predecessor.PARSED_RECORD_VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.PARSED_RECORD_VALIDATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_bj_replay_deterministic",
            "actual": canonical_json(records) == canonical_json(reverse_records),
            "expected": True,
            "passed": canonical_json(records) == canonical_json(reverse_records),
        },
        {
            "check": "nine_bj_digest_replay_deterministic",
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
                        "parsed_record_validation_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "parsed_record_validation_plan_record_digest"
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
                row["authoritative_field_name"]
                == AUTHORITATIVE_FIELD_NAME
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
                row["authoritative_field_path"]
                == AUTHORITATIVE_FIELD_PATH
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
            "check": "mapping_principles_defined",
            "actual": len(MAPPING_PRINCIPLES),
            "expected": 8,
            "passed": len(MAPPING_PRINCIPLES) == 8,
        },
        {
            "check": "mapping_components_defined",
            "actual": len(MAPPING_COMPONENTS),
            "expected": 8,
            "passed": len(MAPPING_COMPONENTS) == 8,
        },
        {
            "check": "mapping_stages_defined",
            "actual": len(MAPPING_STAGES),
            "expected": 11,
            "passed": len(MAPPING_STAGES) == 11,
        },
        {
            "check": "mapping_requirements_defined",
            "actual": len(MAPPING_REQUIREMENTS),
            "expected": 30,
            "passed": len(MAPPING_REQUIREMENTS) == 30,
        },
        {
            "check": "mapping_statuses_defined",
            "actual": len(MAPPING_STATUSES),
            "expected": 10,
            "passed": len(MAPPING_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 30,
            "passed": len(BLOCKER_CODES) == 30,
        },
        {
            "check": "mapping_plan_record_fields_defined",
            "actual": len(MAPPING_PLAN_RECORD_FIELDS),
            "expected": 56,
            "passed": len(MAPPING_PLAN_RECORD_FIELDS) == 56,
        },
        {
            "check": "ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 8,
            "passed": len(ORDERING_FIELDS) == 8,
        },
        {
            "check": "implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 16,
            "passed": len(IMPLEMENTATION_STEPS) == 16,
        },
        {
            "check": "mapping_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "mapping_submission_invention",
                    "mapping_identity_invention",
                    "mapping_rule_invention",
                )
            ),
        },
        {
            "check": "source_contract_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "source_field_name_invention",
                    "source_field_path_invention",
                    "source_field_type_invention",
                    "source_value_domain_invention",
                )
            ),
        },
        {
            "check": "rejected_metadata_substitution_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "rejected_metadata_field_substitution"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "coercion_defaulting_inference_imputation_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "boolean_to_integer_coercion",
                    "source_value_defaulting",
                    "source_value_inference",
                    "source_value_imputation",
                )
            ),
        },
        {
            "check": "mapping_execution_and_value_extraction_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
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
            "check": "mapping_plan_records_materialized_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "mapping_submissions_supplied_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "historical_outcome_values_mapped_zero",
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
            "mapping_principles": MAPPING_PRINCIPLES,
            "mapping_components": MAPPING_COMPONENTS,
            "mapping_stages": MAPPING_STAGES,
            "mapping_requirements": MAPPING_REQUIREMENTS,
            "mapping_statuses": MAPPING_STATUSES,
            "blocker_codes": BLOCKER_CODES,
            "mapping_plan_record_fields":
                MAPPING_PLAN_RECORD_FIELDS,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
            "prohibited_authorities": PROHIBITED_AUTHORITIES,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "plan_failed"
    )

    next_layer = (
        "9BL_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "implementation"
        if all_checks_passed
        else
        "9BK_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "mapping_principles.csv",
        ["principle_id", "principle"],
        MAPPING_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "mapping_components.csv",
        ["component_id", "component", "required", "priority"],
        MAPPING_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "mapping_stages.csv",
        ["stage_id", "stage_name", "priority"],
        MAPPING_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "mapping_requirements.csv",
        ["requirement_id", "requirement", "expected"],
        MAPPING_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "mapping_statuses.csv",
        ["status", "implementation_authority"],
        MAPPING_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "blocker_code_catalog.csv",
        ["code", "category"],
        BLOCKER_CODES,
    )

    write_csv(
        OUTPUT_DIR / "mapping_plan_record_field_contract.csv",
        ["ordinal", "field"],
        [
            {"ordinal": index, "field": field}
            for index, field in enumerate(
                MAPPING_PLAN_RECORD_FIELDS,
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
        / "candidate_missing_parsed_record_validation_inventory.csv",
        [
            "source_evidence_parsed_record_validation_plan_record_id",
            "parsed_record_validation_plan_record_digest",
            "source_evidence_response_parsing_plan_record_id",
            "endpoint_candidate_specification_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "parsed_record_validation_status",
            "parsed_record_validation_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "response_artifact_id",
            "response_sha256",
            "parser_id",
            "parser_version",
            "parser_code_digest",
            "parsed_record_submission_supplied",
            "parsed_record_id",
            "parsed_record_version",
            "parsed_record_digest",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.PARSED_RECORD_VALIDATION_CONTRACT_VERSION,
        "validation_records": len(records),
        "validation_comparisons": len(comparison_ids),
        "validation_status_counts":
            dict(sorted(status_counts.items())),
        "validation_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "mapping_principles": len(MAPPING_PRINCIPLES),
        "mapping_components": len(MAPPING_COMPONENTS),
        "mapping_stages": len(MAPPING_STAGES),
        "mapping_requirements": len(MAPPING_REQUIREMENTS),
        "mapping_statuses": len(MAPPING_STATUSES),
        "blocker_codes": len(BLOCKER_CODES),
        "mapping_plan_record_fields":
            len(MAPPING_PLAN_RECORD_FIELDS),
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
        "mapping_plan_records_materialized": 0,
        "mapping_submissions_supplied": 0,
        "historical_outcome_fields_mapped": 0,
        "historical_outcome_values_extracted": 0,
        "response_bytes_read": 0,
        "responses_parsed": 0,
        "parsed_records_validated": 0,
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
        / "historical_outcome_field_mapping_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_historical_outcome_field_mapping_implementation"
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
        f"{predecessor.PARSED_RECORD_VALIDATION_CONTRACT_VERSION}"
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
    print(f"Mapping principles: {len(MAPPING_PRINCIPLES)}")
    print(f"Mapping components: {len(MAPPING_COMPONENTS)}")
    print(f"Mapping stages: {len(MAPPING_STAGES)}")
    print(f"Mapping requirements: {len(MAPPING_REQUIREMENTS)}")
    print(
        "Mapping plan record fields: "
        f"{len(MAPPING_PLAN_RECORD_FIELDS)}"
    )
    print(f"Plan digest: {plan_digest}")
    print("Mapping plan records materialized: 0")
    print("Mapping submissions supplied: 0")
    print("Historical outcome fields mapped: 0")
    print("Historical outcome values extracted: 0")
    print("Response bytes read: 0")
    print("Responses parsed: 0")
    print("Parsed records validated: 0")
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
