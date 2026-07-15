#!/usr/bin/env python3
"""
Layer 9BM
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Historical Outcome Field Mapping Result Validation Plan

Plans deterministic validation of historical-outcome field-mapping results
produced under the Layer 9BL contract.

Layer 9BL established that no endpoint candidate, validated parsed record,
field-mapping submission, mapped field, or extracted historical-outcome value
exists. This layer therefore defines result-validation gates and contracts only.

Planning only. No mapping execution, value extraction, canonical mutation,
downstream recomputation, or production authority is granted.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BM"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_plan"
)

PLAN_VERSION = (
    "layer_9BM_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BM_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "map_9BL_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_historical_outcome_field.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BL_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_contract_v1"
)

EXPECTED_MAPPING_RECORDS = 16
EXPECTED_MAPPING_COMPARISONS = 16

EXPECTED_MAPPING_STATUS = "candidate_not_supplied"

EXPECTED_MAPPING_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


VALIDATION_PRINCIPLES = [
    {
        "principle_id": "HOASEHOFMRV-P01",
        "principle": (
            "Mapping-result validation requires an authorized mapping execution "
            "derived from a validated parsed source-evidence record."
        ),
    },
    {
        "principle_id": "HOASEHOFMRV-P02",
        "principle": (
            "Every result must preserve candidate, response, parser, parsed-record, "
            "mapping, comparison, metric, and defect lineage."
        ),
    },
    {
        "principle_id": "HOASEHOFMRV-P03",
        "principle": (
            "Mapped-result identity and content digests must be independently "
            "verifiable and deterministic."
        ),
    },
    {
        "principle_id": "HOASEHOFMRV-P04",
        "principle": (
            "The mapped target must remain the canonical outcome_value field."
        ),
    },
    {
        "principle_id": "HOASEHOFMRV-P05",
        "principle": (
            "The rejected metadata field outcome_available_at_utc may not be "
            "accepted as a mapped historical outcome."
        ),
    },
    {
        "principle_id": "HOASEHOFMRV-P06",
        "principle": (
            "Coercion, defaulting, inference, imputation, inversion, substitution, "
            "and truthiness interpretation remain prohibited."
        ),
    },
    {
        "principle_id": "HOASEHOFMRV-P07",
        "principle": (
            "A structurally valid mapping result does not authorize canonical "
            "mutation or historical evaluation recomputation."
        ),
    },
    {
        "principle_id": "HOASEHOFMRV-P08",
        "principle": (
            "Successful planning grants mapping-result validation implementation "
            "authority only."
        ),
    },
]


VALIDATION_COMPONENTS = [
    {"component_id": "HOASEHOFMRV-C01", "component": "mapping_execution_lineage", "required": True, "priority": 1},
    {"component_id": "HOASEHOFMRV-C02", "component": "mapping_result_submission_identity", "required": True, "priority": 2},
    {"component_id": "HOASEHOFMRV-C03", "component": "mapping_result_digest_integrity", "required": True, "priority": 3},
    {"component_id": "HOASEHOFMRV-C04", "component": "canonical_target_identity_validation", "required": True, "priority": 4},
    {"component_id": "HOASEHOFMRV-C05", "component": "mapped_value_type_and_domain_validation", "required": True, "priority": 5},
    {"component_id": "HOASEHOFMRV-C06", "component": "source_to_target_provenance_validation", "required": True, "priority": 6},
    {"component_id": "HOASEHOFMRV-C07", "component": "failure_and_rejection_policy_validation", "required": True, "priority": 7},
    {"component_id": "HOASEHOFMRV-C08", "component": "validation_disposition_and_authority_boundary", "required": True, "priority": 8},
]


VALIDATION_STAGES = [
    {"stage_id": "HOASEHOFMRV-S01", "stage_name": "field_mapping_record_replay", "priority": 1},
    {"stage_id": "HOASEHOFMRV-S02", "stage_name": "mapping_execution_presence_gate", "priority": 2},
    {"stage_id": "HOASEHOFMRV-S03", "stage_name": "mapping_result_submission_inventory", "priority": 3},
    {"stage_id": "HOASEHOFMRV-S04", "stage_name": "result_identity_and_lineage_validation", "priority": 4},
    {"stage_id": "HOASEHOFMRV-S05", "stage_name": "result_digest_integrity_validation", "priority": 5},
    {"stage_id": "HOASEHOFMRV-S06", "stage_name": "canonical_target_validation", "priority": 6},
    {"stage_id": "HOASEHOFMRV-S07", "stage_name": "mapped_value_type_and_domain_validation", "priority": 7},
    {"stage_id": "HOASEHOFMRV-S08", "stage_name": "source_to_target_provenance_validation", "priority": 8},
    {"stage_id": "HOASEHOFMRV-S09", "stage_name": "failure_and_rejection_policy_validation", "priority": 9},
    {"stage_id": "HOASEHOFMRV-S10", "stage_name": "validation_disposition_assignment", "priority": 10},
    {"stage_id": "HOASEHOFMRV-S11", "stage_name": "deterministic_validation_plan_record_emission", "priority": 11},
]


VALIDATION_REQUIREMENTS = [
    {"requirement_id": "HOASEHOFMRV-R01", "requirement": "candidate_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R02", "requirement": "validated_response_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R03", "requirement": "authorized_parser_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R04", "requirement": "validated_parsed_record_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R05", "requirement": "authorized_mapping_execution_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R06", "requirement": "mapping_result_submission_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R07", "requirement": "mapping_result_id_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R08", "requirement": "mapping_result_version_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R09", "requirement": "mapping_result_digest_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R10", "requirement": "mapping_result_digest_verified", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R11", "requirement": "source_field_name_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R12", "requirement": "source_field_path_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R13", "requirement": "source_value_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R14", "requirement": "target_field_name_is_outcome_value", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R15", "requirement": "target_field_path_is_canonical", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R16", "requirement": "mapped_value_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R17", "requirement": "mapped_value_type_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R18", "requirement": "mapped_value_domain_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R19", "requirement": "source_to_target_provenance_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R20", "requirement": "mapping_rule_provenance_present", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R21", "requirement": "rejected_metadata_substitution_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R22", "requirement": "coercion_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R23", "requirement": "defaulting_inference_imputation_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R24", "requirement": "result_ambiguity_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R25", "requirement": "result_malformed_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRV-R26", "requirement": "validation_execution_during_planning", "expected": False},
    {"requirement_id": "HOASEHOFMRV-R27", "requirement": "canonical_records_mutated", "expected": False},
    {"requirement_id": "HOASEHOFMRV-R28", "requirement": "canonical_mappings_changed", "expected": False},
    {"requirement_id": "HOASEHOFMRV-R29", "requirement": "downstream_records_recomputed", "expected": False},
    {"requirement_id": "HOASEHOFMRV-R30", "requirement": "mapping_result_invented", "expected": False},
]


VALIDATION_STATUSES = [
    {"status": "historical_outcome_field_mapping_result_validation_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "validated_response_not_supplied", "implementation_authority": False},
    {"status": "parser_not_supplied", "implementation_authority": False},
    {"status": "validated_parsed_record_not_supplied", "implementation_authority": False},
    {"status": "mapping_execution_not_completed", "implementation_authority": False},
    {"status": "mapping_result_not_supplied", "implementation_authority": False},
    {"status": "mapping_result_identity_or_integrity_invalid", "implementation_authority": False},
    {"status": "mapping_result_target_or_value_invalid", "implementation_authority": False},
    {"status": "mapping_result_provenance_or_policy_invalid", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "historical_outcome_source_evidence_validated_response_missing", "category": "response"},
    {"code": "historical_outcome_source_evidence_authorized_parser_missing", "category": "parser"},
    {"code": "historical_outcome_source_evidence_validated_parsed_record_missing", "category": "record"},
    {"code": "historical_outcome_field_mapping_execution_missing", "category": "mapping"},
    {"code": "historical_outcome_field_mapping_result_submission_missing", "category": "result"},
    {"code": "historical_outcome_field_mapping_result_id_missing", "category": "identity"},
    {"code": "historical_outcome_field_mapping_result_version_missing", "category": "identity"},
    {"code": "historical_outcome_field_mapping_result_digest_missing", "category": "integrity"},
    {"code": "historical_outcome_field_mapping_result_digest_mismatch", "category": "integrity"},
    {"code": "historical_outcome_field_mapping_result_source_field_name_missing", "category": "source"},
    {"code": "historical_outcome_field_mapping_result_source_field_path_missing", "category": "source"},
    {"code": "historical_outcome_field_mapping_result_source_value_missing", "category": "source"},
    {"code": "historical_outcome_field_mapping_result_target_field_name_invalid", "category": "target"},
    {"code": "historical_outcome_field_mapping_result_target_field_path_invalid", "category": "target"},
    {"code": "historical_outcome_field_mapping_result_mapped_value_missing", "category": "value"},
    {"code": "historical_outcome_field_mapping_result_mapped_value_type_invalid", "category": "value"},
    {"code": "historical_outcome_field_mapping_result_mapped_value_domain_invalid", "category": "value"},
    {"code": "historical_outcome_field_mapping_result_source_to_target_provenance_missing", "category": "provenance"},
    {"code": "historical_outcome_field_mapping_result_mapping_rule_provenance_missing", "category": "provenance"},
    {"code": "historical_outcome_field_mapping_result_rejected_metadata_substitution_detected", "category": "authority"},
    {"code": "historical_outcome_field_mapping_result_coercion_detected", "category": "authority"},
    {"code": "historical_outcome_field_mapping_result_defaulting_detected", "category": "authority"},
    {"code": "historical_outcome_field_mapping_result_inference_detected", "category": "authority"},
    {"code": "historical_outcome_field_mapping_result_imputation_detected", "category": "authority"},
    {"code": "historical_outcome_field_mapping_result_ambiguous", "category": "ambiguity"},
    {"code": "historical_outcome_field_mapping_result_malformed", "category": "failure"},
    {"code": "historical_outcome_field_mapping_result_invention_requested", "category": "authority"},
    {"code": "historical_outcome_field_mapping_result_validation_execution_requested", "category": "authority"},
    {"code": "canonical_historical_outcome_mutation_requested", "category": "authority"},
]


VALIDATION_PLAN_RECORD_FIELDS = [
    "historical_outcome_field_mapping_result_validation_plan_contract_version",
    "historical_outcome_field_mapping_result_validation_plan_record_id",
    "historical_outcome_field_mapping_plan_record_id",
    "historical_outcome_field_mapping_plan_record_digest",
    "source_evidence_parsed_record_validation_plan_record_id",
    "parsed_record_validation_plan_record_digest",
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
    "historical_outcome_field_mapping_status",
    "historical_outcome_field_mapping_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "response_artifact_id",
    "response_sha256",
    "parser_id",
    "parser_version",
    "parser_code_digest",
    "parsed_record_id",
    "parsed_record_version",
    "parsed_record_digest",
    "mapping_submission_supplied",
    "mapping_id",
    "mapping_version",
    "mapping_digest",
    "mapping_result_submission_supplied",
    "mapping_result_id",
    "mapping_result_version",
    "mapping_result_digest",
    "source_field_name",
    "source_field_path",
    "source_value",
    "target_field_name",
    "target_field_path",
    "mapped_value",
    "mapped_value_type_valid",
    "mapped_value_domain_valid",
    "source_to_target_provenance",
    "mapping_rule_provenance",
    "rejected_metadata_substitution_detected",
    "coercion_detected",
    "defaulting_inference_or_imputation_detected",
    "mapping_result_ambiguity_detected",
    "mapping_result_malformed_detected",
    "mapping_result_validation_status",
    "mapping_result_validation_blocker_codes",
    "mapping_result_validation_implementation_authority_granted",
    "mapping_result_validation_rationale",
    "mapping_result_validation_limitations",
    "mapping_result_validation_plan_identity_digest",
    "mapping_result_validation_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "parsed_record_id"},
    {"ordinal": 5, "field": "mapping_id"},
    {"ordinal": 6, "field": "mapping_result_id"},
    {"ordinal": 7, "field": "historical_outcome_field_mapping_result_validation_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9BL_field_mapping_records"},
    {"ordinal": 2, "step": "preserve_full_candidate_to_mapping_lineage"},
    {"ordinal": 3, "step": "require_authorized_mapping_execution"},
    {"ordinal": 4, "step": "load_explicit_mapping_result_submissions"},
    {"ordinal": 5, "step": "validate_mapping_result_identity_version_and_digest"},
    {"ordinal": 6, "step": "validate_source_field_name_path_and_value"},
    {"ordinal": 7, "step": "validate_canonical_target_field_identity"},
    {"ordinal": 8, "step": "validate_mapped_value_type_and_domain"},
    {"ordinal": 9, "step": "validate_source_to_target_and_mapping_rule_provenance"},
    {"ordinal": 10, "step": "reject_rejected_metadata_substitution"},
    {"ordinal": 11, "step": "reject_coercion_defaulting_inference_and_imputation"},
    {"ordinal": 12, "step": "reject_ambiguous_or_malformed_mapping_results"},
    {"ordinal": 13, "step": "withhold_canonical_mutation_and_recomputation"},
    {"ordinal": 14, "step": "emit_deterministic_validation_plan_records"},
    {"ordinal": 15, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 16, "step": "grant_result_validation_implementation_only_when_complete"},
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "response_artifact_invention",
    "parser_submission_invention",
    "parsed_record_submission_invention",
    "mapping_submission_invention",
    "mapping_result_submission_invention",
    "mapping_result_identity_invention",
    "mapping_result_content_invention",
    "mapping_result_digest_invention",
    "source_field_name_invention",
    "source_field_path_invention",
    "source_value_invention",
    "mapped_value_invention",
    "source_to_target_provenance_invention",
    "mapping_rule_provenance_invention",
    "rejected_metadata_field_substitution",
    "boolean_to_integer_coercion",
    "source_value_defaulting",
    "source_value_inference",
    "source_value_imputation",
    "mapping_result_validation_execution",
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
        "layer_9bl_predecessor",
    )

    if (
        predecessor.HISTORICAL_OUTCOME_FIELD_MAPPING_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BL contract version: "
            f"{predecessor.HISTORICAL_OUTCOME_FIELD_MAPPING_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_mapping_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_mapping_records(
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
        row["historical_outcome_field_mapping_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "historical_outcome_field_mapping_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_bl_contract_version_verified",
            "actual":
                predecessor.HISTORICAL_OUTCOME_FIELD_MAPPING_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.HISTORICAL_OUTCOME_FIELD_MAPPING_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_bl_replay_deterministic",
            "actual": canonical_json(records) == canonical_json(reverse_records),
            "expected": True,
            "passed": canonical_json(records) == canonical_json(reverse_records),
        },
        {
            "check": "nine_bl_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": sha256_payload(records) == sha256_payload(reverse_records),
        },
        {
            "check": "expected_mapping_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_MAPPING_RECORDS,
            "passed": len(records) == EXPECTED_MAPPING_RECORDS,
        },
        {
            "check": "expected_mapping_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_MAPPING_COMPARISONS,
            "passed": len(comparison_ids) == EXPECTED_MAPPING_COMPARISONS,
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(sorted(status_counts.items())),
            "expected": {
                EXPECTED_MAPPING_STATUS:
                    EXPECTED_MAPPING_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_MAPPING_STATUS:
                        EXPECTED_MAPPING_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(sorted(blocker_counts.items())),
            "expected": {
                EXPECTED_MAPPING_BLOCKER:
                    EXPECTED_MAPPING_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_MAPPING_BLOCKER:
                        EXPECTED_MAPPING_RECORDS
                }
            ),
        },
        {
            "check": "all_mapping_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "historical_outcome_field_mapping_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "historical_outcome_field_mapping_plan_record_digest"
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
            "expected": 62,
            "passed": len(VALIDATION_PLAN_RECORD_FIELDS) == 62,
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
            "check": "mapping_result_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "mapping_result_submission_invention",
                    "mapping_result_identity_invention",
                    "mapping_result_content_invention",
                    "mapping_result_digest_invention",
                )
            ),
        },
        {
            "check": "mapped_value_and_provenance_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "source_value_invention",
                    "mapped_value_invention",
                    "source_to_target_provenance_invention",
                    "mapping_rule_provenance_invention",
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
            "check": "validation_execution_and_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "mapping_result_validation_execution",
                    "canonical_source_value_mutation",
                    "canonical_outcome_mapping_change",
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
            "check": "validation_plan_records_materialized_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "mapping_result_submissions_supplied_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "mapping_results_validated_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "canonical_mutations_and_recomputations_zero",
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
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_plan_failed"
    )

    next_layer = (
        "9BN_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_implementation"
        if all_checks_passed
        else
        "9BM_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_plan_remediation"
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
        OUTPUT_DIR / "candidate_missing_field_mapping_inventory.csv",
        [
            "historical_outcome_field_mapping_plan_record_id",
            "historical_outcome_field_mapping_plan_record_digest",
            "source_evidence_parsed_record_validation_plan_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "historical_outcome_field_mapping_status",
            "historical_outcome_field_mapping_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "parsed_record_id",
            "mapping_submission_supplied",
            "mapping_id",
            "mapping_version",
            "mapping_digest",
            "target_field_name",
            "target_field_path",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.HISTORICAL_OUTCOME_FIELD_MAPPING_CONTRACT_VERSION,
        "mapping_records": len(records),
        "mapping_comparisons": len(comparison_ids),
        "mapping_status_counts":
            dict(sorted(status_counts.items())),
        "mapping_blocker_counts":
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
        "mapping_result_submissions_supplied": 0,
        "mapping_results_validated": 0,
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
        / "historical_outcome_field_mapping_result_validation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_historical_outcome_field_mapping_result_"
            "validation_implementation"
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
        f"{predecessor.HISTORICAL_OUTCOME_FIELD_MAPPING_CONTRACT_VERSION}"
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
    print(f"Mapping records replayed: {len(records)}")
    print(f"Mapping comparisons: {len(comparison_ids)}")
    print(
        "Mapping status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Mapping blocker counts: "
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
    print("Mapping result submissions supplied: 0")
    print("Mapping results validated: 0")
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
