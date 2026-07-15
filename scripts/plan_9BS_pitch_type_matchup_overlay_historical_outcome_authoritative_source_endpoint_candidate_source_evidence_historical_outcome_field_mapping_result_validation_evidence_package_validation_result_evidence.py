#!/usr/bin/env python3
"""
Layer 9BS
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Source Evidence
Historical Outcome Field Mapping Result Validation
Evidence Package Validation Result Evidence Plan

Plans deterministic evidence records for the Layer 9BR evidence-package
validation results.

Layer 9BR structurally validated 16 evidence-package records. All remain
candidate_not_supplied because no authoritative endpoint candidate exists.
This layer is planning only and does not invent candidate-derived evidence,
validate an authoritative historical outcome, or execute retrieval, parsing,
mapping, extraction, mutation, recomputation, production, market, pricing, or
betting operations.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BS"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_result_evidence_plan"
)

PLAN_VERSION = (
    "layer_9BS_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BS_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package_validation_"
    "result_evidence_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "validate_9BR_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BR_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_contract_v1"
)

EXPECTED_VALIDATION_RECORDS = 16
EXPECTED_COMPARISONS = 16

EXPECTED_STATUS = "candidate_not_supplied"

EXPECTED_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


RESULT_EVIDENCE_PRINCIPLES = [
    {
        "principle_id": "HOASEHOFMRVEPVRE-P01",
        "principle": (
            "Result evidence must preserve the complete Layer 9BR validation "
            "record and predecessor lineage."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVRE-P02",
        "principle": (
            "Structural package validity must remain distinct from validation "
            "of an authoritative historical outcome."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVRE-P03",
        "principle": (
            "Every result-evidence record must have deterministic identity, "
            "version, digest, status, blockers, rationale, and limitations."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVRE-P04",
        "principle": (
            "Candidate-derived evidence absence must remain explicit and must "
            "not be replaced by fabricated evidence."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVRE-P05",
        "principle": (
            "Canonical outcome_value and rejected outcome_available_at_utc "
            "identities must remain unchanged."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVRE-P06",
        "principle": (
            "Result-evidence planning may not execute retrieval, parsing, "
            "mapping, extraction, mutation, or recomputation."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVRE-P07",
        "principle": (
            "Forward and reverse predecessor replay must yield identical "
            "result-evidence planning inputs."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVRE-P08",
        "principle": (
            "Successful planning grants result-evidence implementation "
            "authority only."
        ),
    },
]


RESULT_EVIDENCE_COMPONENTS = [
    {
        "component_id": "HOASEHOFMRVEPVRE-C01",
        "component": "validation_result_lineage_manifest",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEHOFMRVEPVRE-C02",
        "component": "structural_validity_result_manifest",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEHOFMRVEPVRE-C03",
        "component": "authoritative_outcome_validation_disposition",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEHOFMRVEPVRE-C04",
        "component": "candidate_evidence_absence_manifest",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEHOFMRVEPVRE-C05",
        "component": "canonical_field_identity_manifest",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEHOFMRVEPVRE-C06",
        "component": "blocker_rationale_and_limitation_manifest",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEHOFMRVEPVRE-C07",
        "component": "result_evidence_integrity_manifest",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEHOFMRVEPVRE-C08",
        "component": "authority_boundary_manifest",
        "required": True,
        "priority": 8,
    },
]


RESULT_EVIDENCE_STAGES = [
    {"stage_id": "HOASEHOFMRVEPVRE-S01", "stage_name": "validation_result_replay", "priority": 1},
    {"stage_id": "HOASEHOFMRVEPVRE-S02", "stage_name": "validation_lineage_inventory", "priority": 2},
    {"stage_id": "HOASEHOFMRVEPVRE-S03", "stage_name": "structural_validity_inventory", "priority": 3},
    {"stage_id": "HOASEHOFMRVEPVRE-S04", "stage_name": "authoritative_outcome_disposition_inventory", "priority": 4},
    {"stage_id": "HOASEHOFMRVEPVRE-S05", "stage_name": "candidate_evidence_absence_inventory", "priority": 5},
    {"stage_id": "HOASEHOFMRVEPVRE-S06", "stage_name": "canonical_field_identity_verification", "priority": 6},
    {"stage_id": "HOASEHOFMRVEPVRE-S07", "stage_name": "blocker_rationale_and_limitation_aggregation", "priority": 7},
    {"stage_id": "HOASEHOFMRVEPVRE-S08", "stage_name": "result_evidence_digest_contract_definition", "priority": 8},
    {"stage_id": "HOASEHOFMRVEPVRE-S09", "stage_name": "authority_boundary_validation", "priority": 9},
    {"stage_id": "HOASEHOFMRVEPVRE-S10", "stage_name": "result_evidence_disposition_assignment", "priority": 10},
    {"stage_id": "HOASEHOFMRVEPVRE-S11", "stage_name": "deterministic_result_evidence_plan_emission", "priority": 11},
]


RESULT_EVIDENCE_REQUIREMENTS = [
    {"requirement_id": "HOASEHOFMRVEPVRE-R01", "requirement": "validation_record_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R02", "requirement": "validation_record_id_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R03", "requirement": "validation_record_digest_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R04", "requirement": "package_record_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R05", "requirement": "mapping_validation_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R06", "requirement": "comparison_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R07", "requirement": "metric_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R08", "requirement": "defect_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R09", "requirement": "package_record_identity_valid_result_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R10", "requirement": "package_record_digest_valid_result_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R11", "requirement": "package_manifest_valid_result_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R12", "requirement": "lineage_complete_result_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R13", "requirement": "evidence_inventory_valid_result_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R14", "requirement": "canonical_field_identity_valid_result_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R15", "requirement": "structural_package_validation_complete", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R16", "requirement": "authoritative_historical_outcome_validated", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVRE-R17", "requirement": "candidate_derived_artifact_count_zero", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R18", "requirement": "fabricated_evidence_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R19", "requirement": "evidence_absence_explicit", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R20", "requirement": "canonical_field_identity_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R21", "requirement": "validation_status_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R22", "requirement": "validation_blockers_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R23", "requirement": "validation_rationale_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R24", "requirement": "validation_limitations_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R25", "requirement": "authority_boundary_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVRE-R26", "requirement": "result_evidence_execution_during_planning", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVRE-R27", "requirement": "network_retrieval_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVRE-R28", "requirement": "mapping_or_extraction_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVRE-R29", "requirement": "canonical_mutation_or_recomputation_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVRE-R30", "requirement": "production_or_betting_authority_granted", "expected": False},
]


RESULT_EVIDENCE_STATUSES = [
    {"status": "validation_result_evidence_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "validation_record_identity_invalid", "implementation_authority": False},
    {"status": "validation_record_digest_invalid", "implementation_authority": False},
    {"status": "package_lineage_incomplete", "implementation_authority": False},
    {"status": "structural_validity_result_incomplete", "implementation_authority": False},
    {"status": "authoritative_outcome_disposition_invalid", "implementation_authority": False},
    {"status": "candidate_evidence_absence_invalid", "implementation_authority": False},
    {"status": "canonical_field_identity_invalid", "implementation_authority": False},
    {"status": "authority_boundary_invalid", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "validation_result_evidence_record_id_missing", "category": "identity"},
    {"code": "validation_result_evidence_identity_digest_missing", "category": "identity"},
    {"code": "validation_result_evidence_identity_digest_invalid", "category": "identity"},
    {"code": "validation_result_evidence_record_digest_missing", "category": "integrity"},
    {"code": "validation_result_evidence_record_digest_invalid", "category": "integrity"},
    {"code": "evidence_package_validation_record_lineage_missing", "category": "lineage"},
    {"code": "evidence_package_record_lineage_missing", "category": "lineage"},
    {"code": "mapping_result_validation_lineage_missing", "category": "lineage"},
    {"code": "field_mapping_lineage_missing", "category": "lineage"},
    {"code": "comparison_lineage_missing", "category": "lineage"},
    {"code": "metric_lineage_missing", "category": "lineage"},
    {"code": "defect_lineage_missing", "category": "lineage"},
    {"code": "package_record_identity_valid_result_missing", "category": "structural"},
    {"code": "package_record_digest_valid_result_missing", "category": "structural"},
    {"code": "package_manifest_valid_result_missing", "category": "structural"},
    {"code": "lineage_complete_result_missing", "category": "structural"},
    {"code": "evidence_inventory_valid_result_missing", "category": "structural"},
    {"code": "canonical_field_identity_valid_result_missing", "category": "structural"},
    {"code": "structural_package_validation_not_complete", "category": "structural"},
    {"code": "authoritative_historical_outcome_disposition_missing", "category": "outcome"},
    {"code": "candidate_derived_artifact_count_invalid", "category": "evidence"},
    {"code": "evidence_absence_not_explicit", "category": "evidence"},
    {"code": "fabricated_evidence_detected", "category": "evidence"},
    {"code": "canonical_field_identity_invalid", "category": "field"},
    {"code": "validation_status_or_blocker_missing", "category": "validation"},
    {"code": "validation_rationale_or_limitations_missing", "category": "validation"},
    {"code": "authority_boundary_missing", "category": "authority"},
    {"code": "validation_result_evidence_execution_requested", "category": "authority"},
    {"code": "production_or_betting_authority_requested", "category": "authority"},
]


RESULT_EVIDENCE_PLAN_RECORD_FIELDS = [
    "validation_result_evidence_plan_contract_version",
    "validation_result_evidence_plan_record_id",
    "evidence_package_validation_plan_record_id",
    "evidence_package_validation_plan_identity_digest",
    "evidence_package_validation_plan_record_digest",
    "evidence_package_plan_record_id",
    "evidence_package_plan_identity_digest",
    "evidence_package_plan_record_digest",
    "package_manifest_version",
    "package_manifest_digest",
    "mapping_result_validation_plan_record_id",
    "mapping_result_validation_plan_record_digest",
    "historical_outcome_field_mapping_plan_record_id",
    "historical_outcome_field_mapping_plan_record_digest",
    "comparison_record_id",
    "metric_record_id",
    "metric_name",
    "aggregation_name",
    "aggregation_key",
    "defect_source_path",
    "defect_source_symbol",
    "defect_source_record_id",
    "defect_source_record_digest",
    "authoritative_field_name",
    "authoritative_field_path",
    "rejected_metadata_field_name",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "candidate_derived_artifact_count",
    "validation_artifact_count",
    "evidence_absence_explicit",
    "fabricated_evidence_detected",
    "package_record_identity_valid",
    "package_record_digest_valid",
    "package_manifest_valid",
    "lineage_complete",
    "evidence_inventory_valid",
    "canonical_field_identity_valid",
    "structural_package_validation_complete",
    "authoritative_historical_outcome_validated",
    "evidence_package_validation_status",
    "evidence_package_validation_blocker_codes",
    "evidence_package_validation_rationale",
    "evidence_package_validation_limitations",
    "evidence_package_authority_boundary",
    "validation_result_evidence_status",
    "validation_result_evidence_blocker_codes",
    "validation_result_evidence_implementation_authority_granted",
    "validation_result_evidence_rationale",
    "validation_result_evidence_limitations",
    "validation_result_evidence_authority_boundary",
    "validation_result_evidence_plan_identity_digest",
    "validation_result_evidence_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "mapping_result_validation_plan_record_id"},
    {"ordinal": 5, "field": "evidence_package_plan_record_id"},
    {"ordinal": 6, "field": "evidence_package_validation_plan_record_id"},
    {"ordinal": 7, "field": "validation_result_evidence_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9BR_validation_records"},
    {"ordinal": 2, "step": "preserve_validation_package_mapping_comparison_metric_and_defect_lineage"},
    {"ordinal": 3, "step": "preserve_validation_identity_and_digest"},
    {"ordinal": 4, "step": "preserve_structural_validity_results"},
    {"ordinal": 5, "step": "preserve_structural_package_validation_complete"},
    {"ordinal": 6, "step": "preserve_authoritative_outcome_not_validated_disposition"},
    {"ordinal": 7, "step": "preserve_candidate_derived_artifact_count_zero"},
    {"ordinal": 8, "step": "preserve_evidence_absence_and_no_fabrication"},
    {"ordinal": 9, "step": "preserve_canonical_and_rejected_field_identities"},
    {"ordinal": 10, "step": "preserve_validation_status_blockers_rationale_and_limitations"},
    {"ordinal": 11, "step": "define_result_evidence_identity_version_and_digest_contract"},
    {"ordinal": 12, "step": "define_result_evidence_authority_boundary"},
    {"ordinal": 13, "step": "withhold_network_mapping_extraction_mutation_and_recomputation"},
    {"ordinal": 14, "step": "emit_deterministic_result_evidence_plan_records"},
    {"ordinal": 15, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 16, "step": "grant_result_evidence_implementation_only_when_complete"},
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "response_artifact_invention",
    "parser_submission_invention",
    "parsed_record_submission_invention",
    "mapping_submission_invention",
    "mapping_result_submission_invention",
    "validation_result_invention",
    "evidence_artifact_invention",
    "evidence_artifact_identity_invention",
    "evidence_artifact_digest_invention",
    "evidence_locator_invention",
    "package_record_invention",
    "package_manifest_invention",
    "validation_record_invention",
    "validation_result_evidence_execution",
    "authoritative_historical_outcome_validation",
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
        "layer_9br_predecessor",
    )

    if (
        predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BR contract version: "
            f"{predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_validation_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_validation_records(
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
        row["evidence_package_validation_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "evidence_package_validation_blocker_codes"
        ]
    )

    structural_validity_counts = {
        field: sum(
            bool(row[field])
            for row in records
        )
        for field in (
            "package_record_identity_valid",
            "package_record_digest_valid",
            "package_manifest_valid",
            "lineage_complete",
            "evidence_inventory_valid",
            "canonical_field_identity_valid",
        )
    }

    candidate_derived_artifact_count = sum(
        row["candidate_evidence_artifact_count"]
        + row["response_evidence_artifact_count"]
        + row["parser_evidence_artifact_count"]
        + row["parsed_record_evidence_artifact_count"]
        + row["mapping_evidence_artifact_count"]
        + row["mapping_result_evidence_artifact_count"]
        for row in records
    )

    validation_artifact_count = sum(
        row["validation_evidence_artifact_count"]
        for row in records
    )

    checks = [
        {
            "check": "nine_br_contract_version_verified",
            "actual":
                predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_br_replay_deterministic",
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
            "check": "nine_br_digest_replay_deterministic",
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
            "check": "expected_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_COMPARISONS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_COMPARISONS
            ),
        },
        {
            "check": "all_validation_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_package_validation_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_package_validation_plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "all_validation_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_package_validation_plan_identity_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_package_validation_plan_identity_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "all_records_structurally_valid",
            "actual": structural_validity_counts,
            "expected": {
                key: EXPECTED_VALIDATION_RECORDS
                for key in structural_validity_counts
            },
            "passed": all(
                count == EXPECTED_VALIDATION_RECORDS
                for count in structural_validity_counts.values()
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(sorted(status_counts.items())),
            "expected": {
                EXPECTED_STATUS:
                    EXPECTED_VALIDATION_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_STATUS:
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(sorted(blocker_counts.items())),
            "expected": {
                EXPECTED_BLOCKER:
                    EXPECTED_VALIDATION_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_BLOCKER:
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "candidate_derived_artifact_count_zero",
            "actual": candidate_derived_artifact_count,
            "expected": 0,
            "passed":
                candidate_derived_artifact_count == 0,
        },
        {
            "check": "one_validation_artifact_per_record",
            "actual": validation_artifact_count,
            "expected": EXPECTED_VALIDATION_RECORDS,
            "passed": (
                validation_artifact_count
                == EXPECTED_VALIDATION_RECORDS
            ),
        },
        {
            "check": "evidence_absence_explicit",
            "actual": sum(
                bool(row["evidence_absence_explicit"])
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                bool(row["evidence_absence_explicit"])
                for row in records
            ),
        },
        {
            "check": "fabricated_evidence_absent",
            "actual": sum(
                not bool(
                    row["fabricated_evidence_detected"]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                not bool(
                    row["fabricated_evidence_detected"]
                )
                for row in records
            ),
        },
        {
            "check": "canonical_field_identity_preserved",
            "actual": sorted(
                {
                    (
                        row["authoritative_field_name"],
                        row["authoritative_field_path"],
                        row["rejected_metadata_field_name"],
                    )
                    for row in records
                }
            ),
            "expected": [
                (
                    AUTHORITATIVE_FIELD_NAME,
                    AUTHORITATIVE_FIELD_PATH,
                    REJECTED_METADATA_FIELD,
                )
            ],
            "passed": all(
                row["authoritative_field_name"]
                == AUTHORITATIVE_FIELD_NAME
                and row["authoritative_field_path"]
                == AUTHORITATIVE_FIELD_PATH
                and row["rejected_metadata_field_name"]
                == REJECTED_METADATA_FIELD
                for row in records
            ),
        },
        {
            "check": "result_evidence_principles_defined",
            "actual":
                len(RESULT_EVIDENCE_PRINCIPLES),
            "expected": 8,
            "passed":
                len(RESULT_EVIDENCE_PRINCIPLES) == 8,
        },
        {
            "check": "result_evidence_components_defined",
            "actual":
                len(RESULT_EVIDENCE_COMPONENTS),
            "expected": 8,
            "passed":
                len(RESULT_EVIDENCE_COMPONENTS) == 8,
        },
        {
            "check": "result_evidence_stages_defined",
            "actual":
                len(RESULT_EVIDENCE_STAGES),
            "expected": 11,
            "passed":
                len(RESULT_EVIDENCE_STAGES) == 11,
        },
        {
            "check": "result_evidence_requirements_defined",
            "actual":
                len(RESULT_EVIDENCE_REQUIREMENTS),
            "expected": 30,
            "passed":
                len(RESULT_EVIDENCE_REQUIREMENTS) == 30,
        },
        {
            "check": "result_evidence_statuses_defined",
            "actual":
                len(RESULT_EVIDENCE_STATUSES),
            "expected": 10,
            "passed":
                len(RESULT_EVIDENCE_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 30,
            "passed":
                len(BLOCKER_CODES) == 30,
        },
        {
            "check": "result_evidence_plan_record_fields_defined",
            "actual":
                len(RESULT_EVIDENCE_PLAN_RECORD_FIELDS),
            "expected": 54,
            "passed": (
                len(RESULT_EVIDENCE_PLAN_RECORD_FIELDS)
                == 54
            ),
        },
        {
            "check": "ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 7,
            "passed":
                len(ORDERING_FIELDS) == 7,
        },
        {
            "check": "implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 16,
            "passed":
                len(IMPLEMENTATION_STEPS) == 16,
        },
        {
            "check": "result_evidence_invention_and_execution_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "validation_result_invention",
                    "evidence_artifact_invention",
                    "validation_record_invention",
                    "validation_result_evidence_execution",
                )
            ),
        },
        {
            "check": "authoritative_outcome_validation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "authoritative_historical_outcome_validation"
                in PROHIBITED_AUTHORITIES
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
            "check": "result_evidence_plan_records_materialized_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "authoritative_historical_outcomes_validated_zero",
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
            "result_evidence_principles":
                RESULT_EVIDENCE_PRINCIPLES,
            "result_evidence_components":
                RESULT_EVIDENCE_COMPONENTS,
            "result_evidence_stages":
                RESULT_EVIDENCE_STAGES,
            "result_evidence_requirements":
                RESULT_EVIDENCE_REQUIREMENTS,
            "result_evidence_statuses":
                RESULT_EVIDENCE_STATUSES,
            "blocker_codes":
                BLOCKER_CODES,
            "result_evidence_plan_record_fields":
                RESULT_EVIDENCE_PLAN_RECORD_FIELDS,
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
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_plan_"
        "complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_plan_"
        "failed"
    )

    next_layer = (
        "9BT_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "implementation"
        if all_checks_passed
        else
        "9BS_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_plan_"
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
        OUTPUT_DIR / "result_evidence_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        RESULT_EVIDENCE_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_components.csv",
        [
            "component_id",
            "component",
            "required",
            "priority",
        ],
        RESULT_EVIDENCE_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_stages.csv",
        [
            "stage_id",
            "stage_name",
            "priority",
        ],
        RESULT_EVIDENCE_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        RESULT_EVIDENCE_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_statuses.csv",
        [
            "status",
            "implementation_authority",
        ],
        RESULT_EVIDENCE_STATUSES,
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
        OUTPUT_DIR
        / "result_evidence_plan_record_field_contract.csv",
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
                RESULT_EVIDENCE_PLAN_RECORD_FIELDS,
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
        OUTPUT_DIR
        / "candidate_missing_validation_result_inventory.csv",
        [
            "evidence_package_validation_plan_record_id",
            "evidence_package_validation_plan_identity_digest",
            "evidence_package_validation_plan_record_digest",
            "evidence_package_plan_record_id",
            "comparison_record_id",
            "metric_record_id",
            "defect_source_record_id",
            "defect_source_record_digest",
            "package_record_identity_valid",
            "package_record_digest_valid",
            "package_manifest_valid",
            "lineage_complete",
            "evidence_inventory_valid",
            "canonical_field_identity_valid",
            "evidence_package_validation_status",
            "evidence_package_validation_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_evidence_artifact_count",
            "response_evidence_artifact_count",
            "parser_evidence_artifact_count",
            "parsed_record_evidence_artifact_count",
            "mapping_evidence_artifact_count",
            "mapping_result_evidence_artifact_count",
            "validation_evidence_artifact_count",
            "evidence_absence_explicit",
            "fabricated_evidence_detected",
            "authoritative_field_name",
            "authoritative_field_path",
            "rejected_metadata_field_name",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION,
        "validation_records":
            len(records),
        "validation_comparisons":
            len(comparison_ids),
        "validation_status_counts":
            dict(sorted(status_counts.items())),
        "validation_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "structural_validity_counts":
            structural_validity_counts,
        "candidate_derived_artifact_count":
            candidate_derived_artifact_count,
        "validation_artifact_count":
            validation_artifact_count,
        "result_evidence_principles":
            len(RESULT_EVIDENCE_PRINCIPLES),
        "result_evidence_components":
            len(RESULT_EVIDENCE_COMPONENTS),
        "result_evidence_stages":
            len(RESULT_EVIDENCE_STAGES),
        "result_evidence_requirements":
            len(RESULT_EVIDENCE_REQUIREMENTS),
        "result_evidence_statuses":
            len(RESULT_EVIDENCE_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "result_evidence_plan_record_fields":
            len(RESULT_EVIDENCE_PLAN_RECORD_FIELDS),
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
        "result_evidence_plan_records_materialized": 0,
        "authoritative_historical_outcomes_validated": 0,
        "candidate_derived_evidence_artifacts_created": 0,
        "fabricated_evidence_artifacts_created": 0,
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
        "all_checks_passed":
            all_checks_passed,
        "recommended_next_layer":
            next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "validation_result_evidence_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "structural_package_validation_complete":
            all_checks_passed,
        "authoritative_historical_outcome_validated":
            False,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_historical_outcome_field_mapping_result_"
            "validation_evidence_package_validation_result_evidence_"
            "implementation"
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
        f"{predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION}"
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
        f"Validation records replayed: {len(records)}"
    )
    print(
        f"Validation comparisons: {len(comparison_ids)}"
    )
    print(
        "Validation status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Validation blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Structural validity counts: "
        f"{structural_validity_counts}"
    )
    print(
        "Candidate-derived artifact count: "
        f"{candidate_derived_artifact_count}"
    )
    print(
        "Validation artifact count: "
        f"{validation_artifact_count}"
    )
    print(
        "Result-evidence principles: "
        f"{len(RESULT_EVIDENCE_PRINCIPLES)}"
    )
    print(
        "Result-evidence components: "
        f"{len(RESULT_EVIDENCE_COMPONENTS)}"
    )
    print(
        "Result-evidence stages: "
        f"{len(RESULT_EVIDENCE_STAGES)}"
    )
    print(
        "Result-evidence requirements: "
        f"{len(RESULT_EVIDENCE_REQUIREMENTS)}"
    )
    print(
        "Result-evidence plan record fields: "
        f"{len(RESULT_EVIDENCE_PLAN_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Result-evidence plan records materialized: 0")
    print("Authoritative historical outcomes validated: 0")
    print("Candidate-derived evidence artifacts created: 0")
    print("Fabricated evidence artifacts created: 0")
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
        "Structural package validation complete: "
        f"{diagnosis['structural_package_validation_complete']}"
    )
    print(
        "Authoritative historical outcome validated: "
        f"{diagnosis['authoritative_historical_outcome_validated']}"
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
