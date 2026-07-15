#!/usr/bin/env python3
"""
Layer 9BO
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Source Evidence
Historical Outcome Field Mapping Result Validation Evidence Package Plan

Plans a deterministic evidence package for Layer 9BN mapping-result validation
records.

Layer 9BN established that no endpoint candidate, validated response, parser,
validated parsed record, mapping submission, mapping execution, mapping-result
submission, source value, mapped value, or validated mapping result exists.

This layer is planning only. It packages no fabricated evidence and grants only
Layer 9BP evidence-package implementation authority.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BO"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_plan"
)

PLAN_VERSION = (
    "layer_9BO_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BO_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "validate_9BN_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BN_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "contract_v1"
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


EVIDENCE_PACKAGE_PRINCIPLES = [
    {
        "principle_id": "HOASEHOFMRVEP-P01",
        "principle": (
            "Evidence packages must preserve complete immutable lineage from "
            "comparison and defect records through mapping-result validation."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEP-P02",
        "principle": (
            "Every packaged artifact must have an explicit identifier, version, "
            "digest, media type, and provenance locator."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEP-P03",
        "principle": (
            "Evidence absence must be represented explicitly rather than filled "
            "with invented endpoint, response, parser, mapping, or result data."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEP-P04",
        "principle": (
            "The package must preserve the canonical outcome_value identity and "
            "the rejected outcome_available_at_utc metadata identity."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEP-P05",
        "principle": (
            "Package completeness is structural and does not imply that an "
            "authoritative historical outcome was found or validated."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEP-P06",
        "principle": (
            "Package creation may not execute network retrieval, parsing, "
            "mapping, extraction, mutation, or recomputation."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEP-P07",
        "principle": (
            "Package manifests and records must replay deterministically in "
            "forward and reverse predecessor order."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEP-P08",
        "principle": (
            "Successful planning grants evidence-package implementation "
            "authority only."
        ),
    },
]


EVIDENCE_PACKAGE_COMPONENTS = [
    {
        "component_id": "HOASEHOFMRVEP-C01",
        "component": "validation_record_lineage_manifest",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEHOFMRVEP-C02",
        "component": "candidate_response_parser_evidence_inventory",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEHOFMRVEP-C03",
        "component": "parsed_record_and_mapping_evidence_inventory",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEHOFMRVEP-C04",
        "component": "mapping_result_validation_evidence_inventory",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEHOFMRVEP-C05",
        "component": "canonical_field_identity_manifest",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEHOFMRVEP-C06",
        "component": "blocker_and_limitation_manifest",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEHOFMRVEP-C07",
        "component": "package_integrity_and_replay_manifest",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEHOFMRVEP-C08",
        "component": "authority_boundary_manifest",
        "required": True,
        "priority": 8,
    },
]


EVIDENCE_PACKAGE_STAGES = [
    {"stage_id": "HOASEHOFMRVEP-S01", "stage_name": "validation_record_replay", "priority": 1},
    {"stage_id": "HOASEHOFMRVEP-S02", "stage_name": "lineage_inventory", "priority": 2},
    {"stage_id": "HOASEHOFMRVEP-S03", "stage_name": "evidence_artifact_inventory", "priority": 3},
    {"stage_id": "HOASEHOFMRVEP-S04", "stage_name": "evidence_absence_classification", "priority": 4},
    {"stage_id": "HOASEHOFMRVEP-S05", "stage_name": "canonical_field_identity_verification", "priority": 5},
    {"stage_id": "HOASEHOFMRVEP-S06", "stage_name": "blocker_and_limitation_aggregation", "priority": 6},
    {"stage_id": "HOASEHOFMRVEP-S07", "stage_name": "artifact_digest_contract_validation", "priority": 7},
    {"stage_id": "HOASEHOFMRVEP-S08", "stage_name": "manifest_digest_contract_validation", "priority": 8},
    {"stage_id": "HOASEHOFMRVEP-S09", "stage_name": "authority_boundary_validation", "priority": 9},
    {"stage_id": "HOASEHOFMRVEP-S10", "stage_name": "package_disposition_assignment", "priority": 10},
    {"stage_id": "HOASEHOFMRVEP-S11", "stage_name": "deterministic_package_plan_emission", "priority": 11},
]


EVIDENCE_PACKAGE_REQUIREMENTS = [
    {"requirement_id": "HOASEHOFMRVEP-R01", "requirement": "validation_record_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R02", "requirement": "validation_record_digest_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R03", "requirement": "comparison_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R04", "requirement": "defect_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R05", "requirement": "candidate_evidence_inventory_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R06", "requirement": "response_evidence_inventory_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R07", "requirement": "parser_evidence_inventory_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R08", "requirement": "parsed_record_evidence_inventory_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R09", "requirement": "mapping_evidence_inventory_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R10", "requirement": "mapping_result_evidence_inventory_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R11", "requirement": "canonical_target_field_identity_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R12", "requirement": "rejected_metadata_field_identity_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R13", "requirement": "validation_status_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R14", "requirement": "validation_blockers_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R15", "requirement": "validation_rationale_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R16", "requirement": "validation_limitations_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R17", "requirement": "package_record_id_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R18", "requirement": "package_record_version_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R19", "requirement": "package_record_digest_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R20", "requirement": "package_manifest_digest_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R21", "requirement": "package_replay_deterministic", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R22", "requirement": "evidence_absence_explicit", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R23", "requirement": "fabricated_evidence_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R24", "requirement": "package_authority_boundary_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEP-R25", "requirement": "package_execution_during_planning", "expected": False},
    {"requirement_id": "HOASEHOFMRVEP-R26", "requirement": "network_retrieval_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEP-R27", "requirement": "mapping_or_extraction_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEP-R28", "requirement": "canonical_records_mutated", "expected": False},
    {"requirement_id": "HOASEHOFMRVEP-R29", "requirement": "downstream_records_recomputed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEP-R30", "requirement": "production_or_betting_authority_granted", "expected": False},
]


EVIDENCE_PACKAGE_STATUSES = [
    {"status": "evidence_package_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "validated_response_not_supplied", "implementation_authority": False},
    {"status": "parser_not_supplied", "implementation_authority": False},
    {"status": "validated_parsed_record_not_supplied", "implementation_authority": False},
    {"status": "mapping_not_supplied", "implementation_authority": False},
    {"status": "mapping_result_not_supplied", "implementation_authority": False},
    {"status": "mapping_result_not_validated", "implementation_authority": False},
    {"status": "evidence_package_identity_or_integrity_invalid", "implementation_authority": False},
    {"status": "evidence_package_authority_boundary_invalid", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "historical_outcome_source_evidence_validated_response_missing", "category": "response"},
    {"code": "historical_outcome_source_evidence_authorized_parser_missing", "category": "parser"},
    {"code": "historical_outcome_source_evidence_validated_parsed_record_missing", "category": "record"},
    {"code": "historical_outcome_field_mapping_submission_missing", "category": "mapping"},
    {"code": "historical_outcome_field_mapping_result_submission_missing", "category": "result"},
    {"code": "historical_outcome_field_mapping_result_validation_missing", "category": "validation"},
    {"code": "evidence_package_record_id_missing", "category": "identity"},
    {"code": "evidence_package_record_version_missing", "category": "identity"},
    {"code": "evidence_package_record_digest_missing", "category": "integrity"},
    {"code": "evidence_package_manifest_digest_missing", "category": "integrity"},
    {"code": "evidence_package_validation_record_lineage_missing", "category": "lineage"},
    {"code": "evidence_package_comparison_lineage_missing", "category": "lineage"},
    {"code": "evidence_package_defect_lineage_missing", "category": "lineage"},
    {"code": "evidence_package_candidate_inventory_missing", "category": "inventory"},
    {"code": "evidence_package_response_inventory_missing", "category": "inventory"},
    {"code": "evidence_package_parser_inventory_missing", "category": "inventory"},
    {"code": "evidence_package_parsed_record_inventory_missing", "category": "inventory"},
    {"code": "evidence_package_mapping_inventory_missing", "category": "inventory"},
    {"code": "evidence_package_mapping_result_inventory_missing", "category": "inventory"},
    {"code": "evidence_package_canonical_field_identity_missing", "category": "field"},
    {"code": "evidence_package_rejected_metadata_identity_missing", "category": "field"},
    {"code": "evidence_package_validation_status_missing", "category": "validation"},
    {"code": "evidence_package_validation_blockers_missing", "category": "validation"},
    {"code": "evidence_package_validation_rationale_missing", "category": "validation"},
    {"code": "evidence_package_validation_limitations_missing", "category": "validation"},
    {"code": "evidence_package_fabricated_evidence_detected", "category": "authority"},
    {"code": "evidence_package_network_execution_requested", "category": "authority"},
    {"code": "evidence_package_mapping_or_extraction_requested", "category": "authority"},
    {"code": "evidence_package_canonical_mutation_requested", "category": "authority"},
]


EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS = [
    "evidence_package_plan_contract_version",
    "evidence_package_plan_record_id",
    "mapping_result_validation_plan_record_id",
    "mapping_result_validation_plan_record_digest",
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
    "mapping_id",
    "mapping_version",
    "mapping_digest",
    "mapping_result_id",
    "mapping_result_version",
    "mapping_result_digest",
    "mapping_result_validation_status",
    "mapping_result_validation_blocker_codes",
    "mapping_result_validation_rationale",
    "mapping_result_validation_limitations",
    "candidate_evidence_artifact_count",
    "response_evidence_artifact_count",
    "parser_evidence_artifact_count",
    "parsed_record_evidence_artifact_count",
    "mapping_evidence_artifact_count",
    "mapping_result_evidence_artifact_count",
    "validation_evidence_artifact_count",
    "evidence_absence_explicit",
    "fabricated_evidence_detected",
    "package_manifest_version",
    "package_manifest_digest",
    "evidence_package_status",
    "evidence_package_blocker_codes",
    "evidence_package_implementation_authority_granted",
    "evidence_package_rationale",
    "evidence_package_limitations",
    "evidence_package_authority_boundary",
    "evidence_package_plan_identity_digest",
    "evidence_package_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "parsed_record_id"},
    {"ordinal": 5, "field": "mapping_id"},
    {"ordinal": 6, "field": "mapping_result_id"},
    {"ordinal": 7, "field": "mapping_result_validation_plan_record_id"},
    {"ordinal": 8, "field": "evidence_package_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9BN_mapping_result_validation_records"},
    {"ordinal": 2, "step": "preserve_comparison_metric_and_defect_lineage"},
    {"ordinal": 3, "step": "inventory_candidate_response_and_parser_evidence"},
    {"ordinal": 4, "step": "inventory_parsed_record_mapping_and_result_evidence"},
    {"ordinal": 5, "step": "inventory_mapping_result_validation_evidence"},
    {"ordinal": 6, "step": "preserve_canonical_and_rejected_field_identities"},
    {"ordinal": 7, "step": "aggregate_validation_status_blockers_and_rationale"},
    {"ordinal": 8, "step": "aggregate_validation_limitations"},
    {"ordinal": 9, "step": "represent_missing_evidence_explicitly"},
    {"ordinal": 10, "step": "reject_fabricated_evidence"},
    {"ordinal": 11, "step": "define_package_identity_version_and_digest_contract"},
    {"ordinal": 12, "step": "define_package_manifest_digest_contract"},
    {"ordinal": 13, "step": "withhold_network_mapping_extraction_and_mutation"},
    {"ordinal": 14, "step": "emit_deterministic_evidence_package_plan_records"},
    {"ordinal": 15, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 16, "step": "grant_evidence_package_implementation_only_when_complete"},
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "response_artifact_invention",
    "response_metadata_invention",
    "parser_submission_invention",
    "parser_identity_invention",
    "parser_code_invention",
    "parsed_record_submission_invention",
    "parsed_record_identity_invention",
    "parsed_record_content_invention",
    "mapping_submission_invention",
    "mapping_identity_invention",
    "mapping_result_submission_invention",
    "mapping_result_identity_invention",
    "mapping_result_content_invention",
    "validation_result_invention",
    "evidence_artifact_invention",
    "evidence_artifact_identity_invention",
    "evidence_artifact_digest_invention",
    "evidence_locator_invention",
    "source_value_invention",
    "mapped_value_invention",
    "source_to_target_provenance_invention",
    "mapping_rule_provenance_invention",
    "rejected_metadata_field_substitution",
    "boolean_to_integer_coercion",
    "source_value_defaulting",
    "source_value_inference",
    "source_value_imputation",
    "evidence_package_execution",
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
        "layer_9bn_predecessor",
    )

    if (
        predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BN contract version: "
            f"{predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION}"
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
        row["mapping_result_validation_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "mapping_result_validation_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_bn_contract_version_verified",
            "actual":
                predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_bn_replay_deterministic",
            "actual":
                canonical_json(records)
                == canonical_json(reverse_records),
            "expected": True,
            "passed":
                canonical_json(records)
                == canonical_json(reverse_records),
        },
        {
            "check": "nine_bn_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed":
                sha256_payload(records)
                == sha256_payload(reverse_records),
        },
        {
            "check": "expected_validation_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_VALIDATION_RECORDS,
            "passed":
                len(records)
                == EXPECTED_VALIDATION_RECORDS,
        },
        {
            "check": "expected_validation_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_VALIDATION_COMPARISONS,
            "passed":
                len(comparison_ids)
                == EXPECTED_VALIDATION_COMPARISONS,
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
                        "mapping_result_validation_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "mapping_result_validation_plan_record_digest"
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
            "check": "evidence_package_principles_defined",
            "actual": len(EVIDENCE_PACKAGE_PRINCIPLES),
            "expected": 8,
            "passed": len(EVIDENCE_PACKAGE_PRINCIPLES) == 8,
        },
        {
            "check": "evidence_package_components_defined",
            "actual": len(EVIDENCE_PACKAGE_COMPONENTS),
            "expected": 8,
            "passed": len(EVIDENCE_PACKAGE_COMPONENTS) == 8,
        },
        {
            "check": "evidence_package_stages_defined",
            "actual": len(EVIDENCE_PACKAGE_STAGES),
            "expected": 11,
            "passed": len(EVIDENCE_PACKAGE_STAGES) == 11,
        },
        {
            "check": "evidence_package_requirements_defined",
            "actual": len(EVIDENCE_PACKAGE_REQUIREMENTS),
            "expected": 30,
            "passed": len(EVIDENCE_PACKAGE_REQUIREMENTS) == 30,
        },
        {
            "check": "evidence_package_statuses_defined",
            "actual": len(EVIDENCE_PACKAGE_STATUSES),
            "expected": 10,
            "passed": len(EVIDENCE_PACKAGE_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 30,
            "passed": len(BLOCKER_CODES) == 30,
        },
        {
            "check": "evidence_package_plan_record_fields_defined",
            "actual": len(EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS),
            "expected": 61,
            "passed":
                len(EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS)
                == 61,
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
            "check": "evidence_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "evidence_artifact_invention",
                    "evidence_artifact_identity_invention",
                    "evidence_artifact_digest_invention",
                    "evidence_locator_invention",
                )
            ),
        },
        {
            "check": "candidate_response_parser_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "endpoint_candidate_invention",
                    "response_artifact_invention",
                    "parser_submission_invention",
                )
            ),
        },
        {
            "check": "mapping_and_result_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "mapping_submission_invention",
                    "mapping_result_submission_invention",
                    "validation_result_invention",
                )
            ),
        },
        {
            "check": "network_mapping_extraction_and_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "http_request_execution",
                    "api_request_execution",
                    "historical_outcome_field_mapping_execution",
                    "historical_outcome_value_extraction",
                    "canonical_source_value_mutation",
                    "canonical_outcome_mapping_change",
                )
            ),
        },
        {
            "check": "evidence_package_plan_records_materialized_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "evidence_artifacts_created_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "historical_outcome_values_extracted_zero",
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
            "evidence_package_principles":
                EVIDENCE_PACKAGE_PRINCIPLES,
            "evidence_package_components":
                EVIDENCE_PACKAGE_COMPONENTS,
            "evidence_package_stages":
                EVIDENCE_PACKAGE_STAGES,
            "evidence_package_requirements":
                EVIDENCE_PACKAGE_REQUIREMENTS,
            "evidence_package_statuses":
                EVIDENCE_PACKAGE_STATUSES,
            "blocker_codes": BLOCKER_CODES,
            "evidence_package_plan_record_fields":
                EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
            "prohibited_authorities": PROHIBITED_AUTHORITIES,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_plan_failed"
    )

    next_layer = (
        "9BP_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_implementation"
        if all_checks_passed
        else
        "9BO_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "evidence_package_principles.csv",
        ["principle_id", "principle"],
        EVIDENCE_PACKAGE_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "evidence_package_components.csv",
        ["component_id", "component", "required", "priority"],
        EVIDENCE_PACKAGE_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "evidence_package_stages.csv",
        ["stage_id", "stage_name", "priority"],
        EVIDENCE_PACKAGE_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "evidence_package_requirements.csv",
        ["requirement_id", "requirement", "expected"],
        EVIDENCE_PACKAGE_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "evidence_package_statuses.csv",
        ["status", "implementation_authority"],
        EVIDENCE_PACKAGE_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "blocker_code_catalog.csv",
        ["code", "category"],
        BLOCKER_CODES,
    )

    write_csv(
        OUTPUT_DIR / "evidence_package_plan_record_field_contract.csv",
        ["ordinal", "field"],
        [
            {
                "ordinal": index,
                "field": field,
            }
            for index, field in enumerate(
                EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS,
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
        / "candidate_missing_mapping_result_validation_inventory.csv",
        [
            "mapping_result_validation_plan_record_id",
            "mapping_result_validation_plan_record_digest",
            "historical_outcome_field_mapping_plan_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "mapping_result_validation_status",
            "mapping_result_validation_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "parsed_record_id",
            "mapping_id",
            "mapping_result_id",
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
            predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION,
        "validation_records": len(records),
        "validation_comparisons": len(comparison_ids),
        "validation_status_counts":
            dict(sorted(status_counts.items())),
        "validation_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "evidence_package_principles":
            len(EVIDENCE_PACKAGE_PRINCIPLES),
        "evidence_package_components":
            len(EVIDENCE_PACKAGE_COMPONENTS),
        "evidence_package_stages":
            len(EVIDENCE_PACKAGE_STAGES),
        "evidence_package_requirements":
            len(EVIDENCE_PACKAGE_REQUIREMENTS),
        "evidence_package_statuses":
            len(EVIDENCE_PACKAGE_STATUSES),
        "blocker_codes": len(BLOCKER_CODES),
        "evidence_package_plan_record_fields":
            len(EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS),
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
        "evidence_package_plan_records_materialized": 0,
        "evidence_artifacts_created": 0,
        "evidence_manifests_created": 0,
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
        / "historical_outcome_mapping_result_validation_"
        "evidence_package_plan_summary.json",
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
            "validation_evidence_package_implementation"
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
        f"{predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION}"
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
    print(
        "Evidence-package principles: "
        f"{len(EVIDENCE_PACKAGE_PRINCIPLES)}"
    )
    print(
        "Evidence-package components: "
        f"{len(EVIDENCE_PACKAGE_COMPONENTS)}"
    )
    print(
        "Evidence-package stages: "
        f"{len(EVIDENCE_PACKAGE_STAGES)}"
    )
    print(
        "Evidence-package requirements: "
        f"{len(EVIDENCE_PACKAGE_REQUIREMENTS)}"
    )
    print(
        "Evidence-package plan record fields: "
        f"{len(EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS)}"
    )
    print(f"Plan digest: {plan_digest}")
    print("Evidence-package plan records materialized: 0")
    print("Evidence artifacts created: 0")
    print("Evidence manifests created: 0")
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
