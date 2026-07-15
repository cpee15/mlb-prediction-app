#!/usr/bin/env python3
"""
Layer 9BQ
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Source Evidence
Historical Outcome Field Mapping Result Validation
Evidence Package Validation Plan

Plans deterministic validation of Layer 9BP evidence-package records.

Layer 9BP packaged verified absence, lineage, canonical field identity,
validation blockers, limitations, manifest digests, and authority boundaries.
No endpoint candidate or candidate-derived evidence exists.

Planning only. No evidence-package validation execution, retrieval, parsing,
mapping, extraction, mutation, recomputation, production, or betting authority
is granted.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BQ"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_plan"
)

PLAN_VERSION = (
    "layer_9BQ_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BQ_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package_validation_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "package_9BP_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BP_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_contract_v1"
)

EXPECTED_MANIFEST_VERSION = (
    "layer_9BP_historical_outcome_mapping_result_validation_"
    "evidence_package_manifest_v1"
)

EXPECTED_PACKAGE_RECORDS = 16
EXPECTED_PACKAGE_COMPARISONS = 16

EXPECTED_PACKAGE_STATUS = "candidate_not_supplied"

EXPECTED_PACKAGE_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


VALIDATION_PRINCIPLES = [
    {
        "principle_id": "HOASEHOFMRVEPV-P01",
        "principle": (
            "Evidence-package validation must replay the package records and "
            "manifests deterministically."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPV-P02",
        "principle": (
            "Every package record must preserve complete validation, mapping, "
            "comparison, metric, and defect lineage."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPV-P03",
        "principle": (
            "Package record, identity, and manifest digests must be valid and "
            "independently reproducible."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPV-P04",
        "principle": (
            "Candidate-derived evidence absence must remain explicit and may "
            "not be replaced with fabricated evidence."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPV-P05",
        "principle": (
            "The canonical outcome_value identity and rejected metadata identity "
            "must remain unchanged."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPV-P06",
        "principle": (
            "Evidence-package structural validity does not establish an "
            "authoritative historical outcome."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPV-P07",
        "principle": (
            "Validation planning may not execute network retrieval, parsing, "
            "mapping, extraction, mutation, or recomputation."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPV-P08",
        "principle": (
            "Successful planning grants evidence-package validation "
            "implementation authority only."
        ),
    },
]


VALIDATION_COMPONENTS = [
    {
        "component_id": "HOASEHOFMRVEPV-C01",
        "component": "package_record_replay_validation",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEHOFMRVEPV-C02",
        "component": "package_identity_and_digest_validation",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEHOFMRVEPV-C03",
        "component": "manifest_identity_and_digest_validation",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEHOFMRVEPV-C04",
        "component": "lineage_completeness_validation",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEHOFMRVEPV-C05",
        "component": "evidence_inventory_and_absence_validation",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEHOFMRVEPV-C06",
        "component": "canonical_field_identity_validation",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEHOFMRVEPV-C07",
        "component": "blocker_limitation_and_authority_validation",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEHOFMRVEPV-C08",
        "component": "validation_disposition_and_authority_boundary",
        "required": True,
        "priority": 8,
    },
]


VALIDATION_STAGES = [
    {
        "stage_id": "HOASEHOFMRVEPV-S01",
        "stage_name": "evidence_package_record_replay",
        "priority": 1,
    },
    {
        "stage_id": "HOASEHOFMRVEPV-S02",
        "stage_name": "package_record_identity_validation",
        "priority": 2,
    },
    {
        "stage_id": "HOASEHOFMRVEPV-S03",
        "stage_name": "package_record_digest_validation",
        "priority": 3,
    },
    {
        "stage_id": "HOASEHOFMRVEPV-S04",
        "stage_name": "package_manifest_validation",
        "priority": 4,
    },
    {
        "stage_id": "HOASEHOFMRVEPV-S05",
        "stage_name": "lineage_completeness_validation",
        "priority": 5,
    },
    {
        "stage_id": "HOASEHOFMRVEPV-S06",
        "stage_name": "evidence_inventory_validation",
        "priority": 6,
    },
    {
        "stage_id": "HOASEHOFMRVEPV-S07",
        "stage_name": "evidence_absence_and_fabrication_validation",
        "priority": 7,
    },
    {
        "stage_id": "HOASEHOFMRVEPV-S08",
        "stage_name": "canonical_field_identity_validation",
        "priority": 8,
    },
    {
        "stage_id": "HOASEHOFMRVEPV-S09",
        "stage_name": "blocker_limitation_and_authority_validation",
        "priority": 9,
    },
    {
        "stage_id": "HOASEHOFMRVEPV-S10",
        "stage_name": "validation_disposition_assignment",
        "priority": 10,
    },
    {
        "stage_id": "HOASEHOFMRVEPV-S11",
        "stage_name": "deterministic_validation_plan_emission",
        "priority": 11,
    },
]


VALIDATION_REQUIREMENTS = [
    {"requirement_id": "HOASEHOFMRVEPV-R01", "requirement": "package_record_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R02", "requirement": "package_record_id_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R03", "requirement": "package_identity_digest_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R04", "requirement": "package_record_digest_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R05", "requirement": "package_record_digest_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R06", "requirement": "package_manifest_version_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R07", "requirement": "package_manifest_digest_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R08", "requirement": "package_manifest_digest_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R09", "requirement": "validation_record_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R10", "requirement": "mapping_record_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R11", "requirement": "comparison_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R12", "requirement": "defect_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R13", "requirement": "candidate_derived_artifact_counts_zero", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R14", "requirement": "validation_artifact_count_is_one", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R15", "requirement": "evidence_absence_explicit", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R16", "requirement": "fabricated_evidence_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R17", "requirement": "authoritative_field_name_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R18", "requirement": "authoritative_field_path_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R19", "requirement": "rejected_metadata_field_identity_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R20", "requirement": "package_status_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R21", "requirement": "package_blockers_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R22", "requirement": "package_rationale_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R23", "requirement": "package_limitations_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R24", "requirement": "package_authority_boundary_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R25", "requirement": "package_replay_deterministic", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPV-R26", "requirement": "validation_execution_during_planning", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPV-R27", "requirement": "network_retrieval_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPV-R28", "requirement": "mapping_or_extraction_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPV-R29", "requirement": "canonical_records_mutated_or_recomputed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPV-R30", "requirement": "production_or_betting_authority_granted", "expected": False},
]


VALIDATION_STATUSES = [
    {
        "status": "evidence_package_validation_ready",
        "implementation_authority": True,
    },
    {
        "status": "candidate_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "package_record_identity_invalid",
        "implementation_authority": False,
    },
    {
        "status": "package_record_digest_invalid",
        "implementation_authority": False,
    },
    {
        "status": "package_manifest_invalid",
        "implementation_authority": False,
    },
    {
        "status": "package_lineage_incomplete",
        "implementation_authority": False,
    },
    {
        "status": "package_evidence_inventory_invalid",
        "implementation_authority": False,
    },
    {
        "status": "package_fabricated_evidence_detected",
        "implementation_authority": False,
    },
    {
        "status": "package_canonical_field_identity_invalid",
        "implementation_authority": False,
    },
    {
        "status": "package_authority_boundary_invalid",
        "implementation_authority": False,
    },
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "evidence_package_record_id_missing", "category": "identity"},
    {"code": "evidence_package_identity_digest_missing", "category": "identity"},
    {"code": "evidence_package_identity_digest_invalid", "category": "identity"},
    {"code": "evidence_package_record_digest_missing", "category": "integrity"},
    {"code": "evidence_package_record_digest_invalid", "category": "integrity"},
    {"code": "evidence_package_manifest_version_missing", "category": "manifest"},
    {"code": "evidence_package_manifest_version_invalid", "category": "manifest"},
    {"code": "evidence_package_manifest_digest_missing", "category": "manifest"},
    {"code": "evidence_package_manifest_digest_invalid", "category": "manifest"},
    {"code": "evidence_package_validation_record_lineage_missing", "category": "lineage"},
    {"code": "evidence_package_mapping_record_lineage_missing", "category": "lineage"},
    {"code": "evidence_package_comparison_lineage_missing", "category": "lineage"},
    {"code": "evidence_package_metric_lineage_missing", "category": "lineage"},
    {"code": "evidence_package_defect_lineage_missing", "category": "lineage"},
    {"code": "evidence_package_candidate_artifact_count_invalid", "category": "inventory"},
    {"code": "evidence_package_response_artifact_count_invalid", "category": "inventory"},
    {"code": "evidence_package_parser_artifact_count_invalid", "category": "inventory"},
    {"code": "evidence_package_parsed_record_artifact_count_invalid", "category": "inventory"},
    {"code": "evidence_package_mapping_artifact_count_invalid", "category": "inventory"},
    {"code": "evidence_package_mapping_result_artifact_count_invalid", "category": "inventory"},
    {"code": "evidence_package_validation_artifact_count_invalid", "category": "inventory"},
    {"code": "evidence_package_evidence_absence_not_explicit", "category": "evidence"},
    {"code": "evidence_package_fabricated_evidence_detected", "category": "evidence"},
    {"code": "evidence_package_canonical_field_identity_invalid", "category": "field"},
    {"code": "evidence_package_rejected_metadata_identity_invalid", "category": "field"},
    {"code": "evidence_package_status_or_blocker_missing", "category": "validation"},
    {"code": "evidence_package_rationale_or_limitations_missing", "category": "validation"},
    {"code": "evidence_package_authority_boundary_missing", "category": "authority"},
    {"code": "evidence_package_validation_execution_requested", "category": "authority"},
]


VALIDATION_PLAN_RECORD_FIELDS = [
    "evidence_package_validation_plan_contract_version",
    "evidence_package_validation_plan_record_id",
    "evidence_package_plan_record_id",
    "evidence_package_plan_identity_digest",
    "evidence_package_plan_record_digest",
    "package_manifest_version",
    "package_manifest_digest",
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
    "candidate_evidence_artifact_count",
    "response_evidence_artifact_count",
    "parser_evidence_artifact_count",
    "parsed_record_evidence_artifact_count",
    "mapping_evidence_artifact_count",
    "mapping_result_evidence_artifact_count",
    "validation_evidence_artifact_count",
    "evidence_absence_explicit",
    "fabricated_evidence_detected",
    "evidence_package_status",
    "evidence_package_blocker_codes",
    "evidence_package_rationale",
    "evidence_package_limitations",
    "evidence_package_authority_boundary",
    "package_record_identity_valid",
    "package_record_digest_valid",
    "package_manifest_valid",
    "lineage_complete",
    "evidence_inventory_valid",
    "canonical_field_identity_valid",
    "evidence_package_validation_status",
    "evidence_package_validation_blocker_codes",
    "evidence_package_validation_implementation_authority_granted",
    "evidence_package_validation_rationale",
    "evidence_package_validation_limitations",
    "evidence_package_validation_plan_identity_digest",
    "evidence_package_validation_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "mapping_id"},
    {"ordinal": 5, "field": "mapping_result_id"},
    {"ordinal": 6, "field": "mapping_result_validation_plan_record_id"},
    {"ordinal": 7, "field": "evidence_package_plan_record_id"},
    {"ordinal": 8, "field": "evidence_package_validation_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9BP_evidence_package_records"},
    {"ordinal": 2, "step": "preserve_package_and_manifest_identity"},
    {"ordinal": 3, "step": "validate_package_identity_digests"},
    {"ordinal": 4, "step": "validate_package_record_digests"},
    {"ordinal": 5, "step": "validate_package_manifest_versions_and_digests"},
    {"ordinal": 6, "step": "validate_validation_mapping_comparison_metric_and_defect_lineage"},
    {"ordinal": 7, "step": "validate_candidate_derived_artifact_counts_zero"},
    {"ordinal": 8, "step": "validate_one_validation_artifact_per_package_record"},
    {"ordinal": 9, "step": "validate_evidence_absence_and_no_fabrication"},
    {"ordinal": 10, "step": "validate_canonical_and_rejected_field_identities"},
    {"ordinal": 11, "step": "validate_status_blockers_rationale_and_limitations"},
    {"ordinal": 12, "step": "validate_package_authority_boundaries"},
    {"ordinal": 13, "step": "withhold_network_mapping_extraction_and_mutation"},
    {"ordinal": 14, "step": "emit_deterministic_package_validation_plan_records"},
    {"ordinal": 15, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 16, "step": "grant_package_validation_implementation_only_when_complete"},
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
    "package_record_identity_invention",
    "package_record_digest_invention",
    "package_manifest_invention",
    "package_manifest_digest_invention",
    "evidence_package_validation_execution",
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
        "layer_9bp_predecessor",
    )

    if (
        predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BP contract version: "
            f"{predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION}"
        )

    if (
        predecessor.PACKAGE_MANIFEST_VERSION
        != EXPECTED_MANIFEST_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BP manifest version: "
            f"{predecessor.PACKAGE_MANIFEST_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_package_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_package_records(
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
        row["evidence_package_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "evidence_package_blocker_codes"
        ]
    )

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
            "check": "nine_bp_contract_version_verified",
            "actual":
                predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_bp_manifest_version_verified",
            "actual":
                predecessor.PACKAGE_MANIFEST_VERSION,
            "expected": EXPECTED_MANIFEST_VERSION,
            "passed": (
                predecessor.PACKAGE_MANIFEST_VERSION
                == EXPECTED_MANIFEST_VERSION
            ),
        },
        {
            "check": "nine_bp_replay_deterministic",
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
            "check": "nine_bp_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": (
                sha256_payload(records)
                == sha256_payload(reverse_records)
            ),
        },
        {
            "check": "expected_package_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_PACKAGE_RECORDS,
            "passed": (
                len(records)
                == EXPECTED_PACKAGE_RECORDS
            ),
        },
        {
            "check": "expected_package_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_PACKAGE_COMPARISONS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_PACKAGE_COMPARISONS
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(
                sorted(status_counts.items())
            ),
            "expected": {
                EXPECTED_PACKAGE_STATUS:
                    EXPECTED_PACKAGE_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_PACKAGE_STATUS:
                        EXPECTED_PACKAGE_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(
                sorted(blocker_counts.items())
            ),
            "expected": {
                EXPECTED_PACKAGE_BLOCKER:
                    EXPECTED_PACKAGE_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_PACKAGE_BLOCKER:
                        EXPECTED_PACKAGE_RECORDS
                }
            ),
        },
        {
            "check": "all_package_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_package_plan_identity_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_package_plan_identity_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "all_package_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_package_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_package_plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "all_manifest_digests_valid",
            "actual": sum(
                valid_sha256(
                    row["package_manifest_digest"]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row["package_manifest_digest"]
                )
                for row in records
            ),
        },
        {
            "check": "manifest_version_preserved",
            "actual": sorted(
                {
                    row["package_manifest_version"]
                    for row in records
                }
            ),
            "expected": [EXPECTED_MANIFEST_VERSION],
            "passed": all(
                row["package_manifest_version"]
                == EXPECTED_MANIFEST_VERSION
                for row in records
            ),
        },
        {
            "check": "candidate_derived_artifact_counts_zero",
            "actual":
                candidate_derived_artifact_count,
            "expected": 0,
            "passed":
                candidate_derived_artifact_count == 0,
        },
        {
            "check": "one_validation_artifact_per_record",
            "actual": validation_artifact_count,
            "expected": EXPECTED_PACKAGE_RECORDS,
            "passed": (
                validation_artifact_count
                == EXPECTED_PACKAGE_RECORDS
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
            "check": "package_rationale_limitations_and_authority_present",
            "actual": sum(
                bool(
                    str(
                        row["evidence_package_rationale"]
                    ).strip()
                )
                and bool(
                    row["evidence_package_limitations"]
                )
                and bool(
                    str(
                        row[
                            "evidence_package_authority_boundary"
                        ]
                    ).strip()
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                bool(
                    str(
                        row["evidence_package_rationale"]
                    ).strip()
                )
                and bool(
                    row["evidence_package_limitations"]
                )
                and bool(
                    str(
                        row[
                            "evidence_package_authority_boundary"
                        ]
                    ).strip()
                )
                for row in records
            ),
        },
        {
            "check": "validation_principles_defined",
            "actual": len(VALIDATION_PRINCIPLES),
            "expected": 8,
            "passed":
                len(VALIDATION_PRINCIPLES) == 8,
        },
        {
            "check": "validation_components_defined",
            "actual": len(VALIDATION_COMPONENTS),
            "expected": 8,
            "passed":
                len(VALIDATION_COMPONENTS) == 8,
        },
        {
            "check": "validation_stages_defined",
            "actual": len(VALIDATION_STAGES),
            "expected": 11,
            "passed":
                len(VALIDATION_STAGES) == 11,
        },
        {
            "check": "validation_requirements_defined",
            "actual": len(VALIDATION_REQUIREMENTS),
            "expected": 30,
            "passed":
                len(VALIDATION_REQUIREMENTS) == 30,
        },
        {
            "check": "validation_statuses_defined",
            "actual": len(VALIDATION_STATUSES),
            "expected": 10,
            "passed":
                len(VALIDATION_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 30,
            "passed":
                len(BLOCKER_CODES) == 30,
        },
        {
            "check": "validation_plan_record_fields_defined",
            "actual":
                len(VALIDATION_PLAN_RECORD_FIELDS),
            "expected": 70,
            "passed": (
                len(VALIDATION_PLAN_RECORD_FIELDS)
                == 70
            ),
        },
        {
            "check": "ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 8,
            "passed":
                len(ORDERING_FIELDS) == 8,
        },
        {
            "check": "implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 16,
            "passed":
                len(IMPLEMENTATION_STEPS) == 16,
        },
        {
            "check": "package_and_manifest_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "package_record_invention",
                    "package_record_identity_invention",
                    "package_record_digest_invention",
                    "package_manifest_invention",
                    "package_manifest_digest_invention",
                )
            ),
        },
        {
            "check": "validation_network_mapping_and_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "evidence_package_validation_execution",
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
            "check": "validation_plan_records_materialized_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "package_records_validated_zero",
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
            "validation_principles":
                VALIDATION_PRINCIPLES,
            "validation_components":
                VALIDATION_COMPONENTS,
            "validation_stages":
                VALIDATION_STAGES,
            "validation_requirements":
                VALIDATION_REQUIREMENTS,
            "validation_statuses":
                VALIDATION_STATUSES,
            "blocker_codes":
                BLOCKER_CODES,
            "validation_plan_record_fields":
                VALIDATION_PLAN_RECORD_FIELDS,
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
        "result_validation_evidence_package_validation_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_plan_failed"
    )

    next_layer = (
        "9BR_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_implementation"
        if all_checks_passed
        else
        "9BQ_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_plan_remediation"
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
        OUTPUT_DIR / "validation_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        VALIDATION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "validation_components.csv",
        [
            "component_id",
            "component",
            "required",
            "priority",
        ],
        VALIDATION_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "validation_stages.csv",
        [
            "stage_id",
            "stage_name",
            "priority",
        ],
        VALIDATION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "validation_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        VALIDATION_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "validation_statuses.csv",
        [
            "status",
            "implementation_authority",
        ],
        VALIDATION_STATUSES,
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
        / "validation_plan_record_field_contract.csv",
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
                VALIDATION_PLAN_RECORD_FIELDS,
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
        / "candidate_missing_evidence_package_inventory.csv",
        [
            "evidence_package_plan_record_id",
            "evidence_package_plan_identity_digest",
            "evidence_package_plan_record_digest",
            "package_manifest_version",
            "package_manifest_digest",
            "mapping_result_validation_plan_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "evidence_package_status",
            "evidence_package_blocker_codes",
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
            predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION,
        "predecessor_manifest_version":
            predecessor.PACKAGE_MANIFEST_VERSION,
        "package_records":
            len(records),
        "package_comparisons":
            len(comparison_ids),
        "package_status_counts":
            dict(sorted(status_counts.items())),
        "package_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "candidate_derived_artifact_count":
            candidate_derived_artifact_count,
        "validation_artifact_count":
            validation_artifact_count,
        "validation_principles":
            len(VALIDATION_PRINCIPLES),
        "validation_components":
            len(VALIDATION_COMPONENTS),
        "validation_stages":
            len(VALIDATION_STAGES),
        "validation_requirements":
            len(VALIDATION_REQUIREMENTS),
        "validation_statuses":
            len(VALIDATION_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "validation_plan_record_fields":
            len(VALIDATION_PLAN_RECORD_FIELDS),
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
        "validation_plan_records_materialized": 0,
        "package_records_validated": 0,
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
        / "evidence_package_validation_plan_summary.json",
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
            "source_evidence_historical_outcome_field_mapping_result_"
            "validation_evidence_package_validation_implementation"
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
        f"{predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION}"
    )
    print(
        "Predecessor manifest version: "
        f"{predecessor.PACKAGE_MANIFEST_VERSION}"
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
        f"Package records replayed: {len(records)}"
    )
    print(
        f"Package comparisons: {len(comparison_ids)}"
    )
    print(
        "Package status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Package blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
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
        f"Validation principles: {len(VALIDATION_PRINCIPLES)}"
    )
    print(
        f"Validation components: {len(VALIDATION_COMPONENTS)}"
    )
    print(
        f"Validation stages: {len(VALIDATION_STAGES)}"
    )
    print(
        f"Validation requirements: {len(VALIDATION_REQUIREMENTS)}"
    )
    print(
        "Validation plan record fields: "
        f"{len(VALIDATION_PLAN_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Validation plan records materialized: 0")
    print("Package records validated: 0")
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
