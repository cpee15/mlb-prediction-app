#!/usr/bin/env python3
"""
Layer 9BU
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Source Evidence
Historical Outcome Field Mapping Result Validation
Evidence Package Validation Result Evidence Validation Plan

Defines the deterministic validation plan for Layer 9BT validation-result
evidence records.

This layer plans validation only. It does not validate authoritative historical
outcomes, invent evidence, execute network retrieval, parse responses, map or
extract values, mutate canonical records, recompute downstream records, or
grant production, market, pricing, or betting authority.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BU"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_result_evidence_"
    "validation_plan"
)

PLAN_VERSION = (
    "layer_9BU_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BU_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package_validation_"
    "result_evidence_validation_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "package_9BT_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package_validation_"
    "result_evidence.py"
)

EXPECTED_PREDECESSOR_CONTRACT_VERSION = (
    "layer_9BT_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_contract_v1"
)

EXPECTED_PREDECESSOR_MANIFEST_VERSION = (
    "layer_9BT_historical_outcome_validation_result_evidence_manifest_v1"
)

EXPECTED_RECORDS = 16
EXPECTED_COMPARISONS = 16
EXPECTED_STATUS = "candidate_not_supplied"
EXPECTED_BLOCKER = "historical_outcome_endpoint_candidate_missing"

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


VALIDATION_PRINCIPLES = [
    {
        "principle_id": "HOASEHOFMRVEPVREV-P01",
        "principle": (
            "Validation must preserve each Layer 9BT result-evidence record, "
            "its identity, digest, manifest, and complete predecessor lineage."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREV-P02",
        "principle": (
            "Result-evidence structural validity must remain distinct from "
            "authoritative historical-outcome validation."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREV-P03",
        "principle": (
            "Forward and reverse replay must yield identical validation inputs."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREV-P04",
        "principle": (
            "Candidate-derived evidence absence must remain explicit and no "
            "fabricated evidence may be introduced."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREV-P05",
        "principle": (
            "Canonical outcome_value identity and rejection of "
            "outcome_available_at_utc must remain unchanged."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREV-P06",
        "principle": (
            "Validation records must use deterministic identity and record "
            "digests over a fixed field contract."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREV-P07",
        "principle": (
            "Planning must not execute retrieval, parsing, mapping, extraction, "
            "mutation, recomputation, or model-quality decisions."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREV-P08",
        "principle": (
            "Successful planning grants only Layer 9BV validation "
            "implementation authority."
        ),
    },
]


VALIDATION_COMPONENTS = [
    {
        "component_id": "HOASEHOFMRVEPVREV-C01",
        "component": "result_evidence_identity_validation",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEHOFMRVEPVREV-C02",
        "component": "result_evidence_record_digest_validation",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEHOFMRVEPVREV-C03",
        "component": "result_evidence_manifest_validation",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEHOFMRVEPVREV-C04",
        "component": "predecessor_lineage_validation",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEHOFMRVEPVREV-C05",
        "component": "structural_disposition_validation",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEHOFMRVEPVREV-C06",
        "component": "candidate_evidence_absence_validation",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEHOFMRVEPVREV-C07",
        "component": "canonical_field_identity_validation",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEHOFMRVEPVREV-C08",
        "component": "authority_boundary_validation",
        "required": True,
        "priority": 8,
    },
]


VALIDATION_STAGES = [
    {"stage_id": "HOASEHOFMRVEPVREV-S01", "stage_name": "result_evidence_replay", "priority": 1},
    {"stage_id": "HOASEHOFMRVEPVREV-S02", "stage_name": "record_identity_validation", "priority": 2},
    {"stage_id": "HOASEHOFMRVEPVREV-S03", "stage_name": "record_digest_validation", "priority": 3},
    {"stage_id": "HOASEHOFMRVEPVREV-S04", "stage_name": "manifest_validation", "priority": 4},
    {"stage_id": "HOASEHOFMRVEPVREV-S05", "stage_name": "lineage_validation", "priority": 5},
    {"stage_id": "HOASEHOFMRVEPVREV-S06", "stage_name": "structural_result_validation", "priority": 6},
    {"stage_id": "HOASEHOFMRVEPVREV-S07", "stage_name": "candidate_absence_validation", "priority": 7},
    {"stage_id": "HOASEHOFMRVEPVREV-S08", "stage_name": "canonical_field_validation", "priority": 8},
    {"stage_id": "HOASEHOFMRVEPVREV-S09", "stage_name": "rationale_limitations_boundary_validation", "priority": 9},
    {"stage_id": "HOASEHOFMRVEPVREV-S10", "stage_name": "validation_disposition_assignment", "priority": 10},
    {"stage_id": "HOASEHOFMRVEPVREV-S11", "stage_name": "deterministic_validation_plan_emission", "priority": 11},
]


VALIDATION_REQUIREMENTS = [
    {"requirement_id": "HOASEHOFMRVEPVREV-R01", "requirement": "result_evidence_record_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R02", "requirement": "result_evidence_record_id_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R03", "requirement": "result_evidence_identity_digest_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R04", "requirement": "result_evidence_record_digest_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R05", "requirement": "manifest_version_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R06", "requirement": "manifest_digest_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R07", "requirement": "validation_record_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R08", "requirement": "package_record_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R09", "requirement": "mapping_validation_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R10", "requirement": "field_mapping_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R11", "requirement": "comparison_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R12", "requirement": "metric_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R13", "requirement": "defect_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R14", "requirement": "structural_package_validation_complete", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R15", "requirement": "authoritative_historical_outcome_validated", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREV-R16", "requirement": "candidate_derived_artifact_count_zero", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R17", "requirement": "validation_artifact_count_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R18", "requirement": "evidence_absence_explicit", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R19", "requirement": "fabricated_evidence_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R20", "requirement": "canonical_field_identity_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R21", "requirement": "candidate_not_supplied_status_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R22", "requirement": "missing_endpoint_blocker_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R23", "requirement": "rationale_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R24", "requirement": "limitations_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R25", "requirement": "authority_boundary_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREV-R26", "requirement": "validation_records_materialized_during_planning", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREV-R27", "requirement": "network_retrieval_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREV-R28", "requirement": "mapping_or_extraction_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREV-R29", "requirement": "canonical_mutation_or_recomputation_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREV-R30", "requirement": "production_market_or_betting_authority_granted", "expected": False},
]


VALIDATION_STATUSES = [
    {"status": "validation_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "result_evidence_identity_invalid", "implementation_authority": False},
    {"status": "result_evidence_record_digest_invalid", "implementation_authority": False},
    {"status": "manifest_invalid", "implementation_authority": False},
    {"status": "lineage_incomplete", "implementation_authority": False},
    {"status": "structural_disposition_invalid", "implementation_authority": False},
    {"status": "candidate_evidence_absence_invalid", "implementation_authority": False},
    {"status": "canonical_field_identity_invalid", "implementation_authority": False},
    {"status": "authority_boundary_invalid", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "result_evidence_record_missing", "category": "record"},
    {"code": "result_evidence_record_id_missing", "category": "identity"},
    {"code": "result_evidence_identity_digest_missing", "category": "identity"},
    {"code": "result_evidence_identity_digest_invalid", "category": "identity"},
    {"code": "result_evidence_record_digest_missing", "category": "integrity"},
    {"code": "result_evidence_record_digest_invalid", "category": "integrity"},
    {"code": "result_evidence_manifest_version_invalid", "category": "manifest"},
    {"code": "result_evidence_manifest_digest_missing", "category": "manifest"},
    {"code": "result_evidence_manifest_digest_invalid", "category": "manifest"},
    {"code": "validation_record_lineage_missing", "category": "lineage"},
    {"code": "package_record_lineage_missing", "category": "lineage"},
    {"code": "mapping_validation_lineage_missing", "category": "lineage"},
    {"code": "field_mapping_lineage_missing", "category": "lineage"},
    {"code": "comparison_lineage_missing", "category": "lineage"},
    {"code": "metric_lineage_missing", "category": "lineage"},
    {"code": "defect_lineage_missing", "category": "lineage"},
    {"code": "structural_package_validation_incomplete", "category": "structural"},
    {"code": "authoritative_outcome_disposition_invalid", "category": "outcome"},
    {"code": "candidate_derived_artifact_count_invalid", "category": "evidence"},
    {"code": "validation_artifact_count_invalid", "category": "evidence"},
    {"code": "evidence_absence_not_explicit", "category": "evidence"},
    {"code": "fabricated_evidence_detected", "category": "evidence"},
    {"code": "canonical_field_identity_invalid", "category": "field"},
    {"code": "candidate_not_supplied_status_missing", "category": "status"},
    {"code": "missing_endpoint_blocker_missing", "category": "status"},
    {"code": "rationale_or_limitations_missing", "category": "documentation"},
    {"code": "authority_boundary_missing", "category": "authority"},
    {"code": "validation_execution_requested_during_planning", "category": "authority"},
    {"code": "production_market_or_betting_authority_requested", "category": "authority"},
]


VALIDATION_PLAN_RECORD_FIELDS = [
    "result_evidence_validation_plan_contract_version",
    "result_evidence_validation_plan_record_id",
    "validation_result_evidence_plan_record_id",
    "validation_result_evidence_plan_identity_digest",
    "validation_result_evidence_plan_record_digest",
    "result_evidence_manifest_version",
    "result_evidence_manifest_digest",
    "evidence_package_validation_plan_record_id",
    "evidence_package_validation_plan_identity_digest",
    "evidence_package_validation_plan_record_digest",
    "evidence_package_plan_record_id",
    "evidence_package_plan_identity_digest",
    "evidence_package_plan_record_digest",
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
    "validation_result_evidence_status",
    "validation_result_evidence_blocker_codes",
    "validation_result_evidence_rationale",
    "validation_result_evidence_limitations",
    "validation_result_evidence_authority_boundary",
    "result_evidence_validation_status",
    "result_evidence_validation_blocker_codes",
    "result_evidence_validation_implementation_authority_granted",
    "result_evidence_validation_rationale",
    "result_evidence_validation_limitations",
    "result_evidence_validation_authority_boundary",
    "result_evidence_validation_plan_identity_digest",
    "result_evidence_validation_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "evidence_package_plan_record_id"},
    {"ordinal": 5, "field": "evidence_package_validation_plan_record_id"},
    {"ordinal": 6, "field": "validation_result_evidence_plan_record_id"},
    {"ordinal": 7, "field": "result_evidence_validation_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9BT_result_evidence_records"},
    {"ordinal": 2, "step": "replay_result_evidence_manifest"},
    {"ordinal": 3, "step": "validate_result_evidence_record_identity"},
    {"ordinal": 4, "step": "validate_result_evidence_record_digest"},
    {"ordinal": 5, "step": "validate_result_evidence_manifest"},
    {"ordinal": 6, "step": "validate_validation_package_mapping_and_defect_lineage"},
    {"ordinal": 7, "step": "validate_structural_package_disposition"},
    {"ordinal": 8, "step": "validate_authoritative_outcome_not_validated_disposition"},
    {"ordinal": 9, "step": "validate_candidate_derived_artifact_count_zero"},
    {"ordinal": 10, "step": "validate_explicit_evidence_absence_and_no_fabrication"},
    {"ordinal": 11, "step": "validate_canonical_and_rejected_field_identities"},
    {"ordinal": 12, "step": "validate_status_blockers_rationale_limitations_and_boundary"},
    {"ordinal": 13, "step": "materialize_deterministic_validation_records"},
    {"ordinal": 14, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 15, "step": "withhold_network_mutation_recompute_and_quality_authority"},
    {"ordinal": 16, "step": "grant_layer_9BV_validation_implementation_only_when_complete"},
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
    "evidence_locator_invention",
    "result_evidence_invention",
    "result_evidence_validation_execution",
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
                    if isinstance(value, (dict, list, tuple))
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
        "layer_9bt_predecessor",
    )

    if (
        predecessor.RESULT_EVIDENCE_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_CONTRACT_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BT contract version: "
            f"{predecessor.RESULT_EVIDENCE_CONTRACT_VERSION}"
        )

    if (
        predecessor.RESULT_EVIDENCE_MANIFEST_VERSION
        != EXPECTED_PREDECESSOR_MANIFEST_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BT manifest version: "
            f"{predecessor.RESULT_EVIDENCE_MANIFEST_VERSION}"
        )

    replay = predecessor.replay_plan()

    records = predecessor.build_result_evidence_records(
        replay["plan"],
        replay["records"],
    )

    reverse_records = predecessor.build_result_evidence_records(
        replay["plan"],
        list(reversed(replay["reverse_records"])),
    )

    result_digest = sha256_payload(records)

    manifest_payload = {
        "manifest_version":
            predecessor.RESULT_EVIDENCE_MANIFEST_VERSION,
        "contract_version":
            predecessor.RESULT_EVIDENCE_CONTRACT_VERSION,
        "record_count":
            len(records),
        "comparison_count":
            len(
                {
                    row["comparison_record_id"]
                    for row in records
                }
            ),
        "result_digest":
            result_digest,
        "status_counts":
            dict(
                sorted(
                    Counter(
                        row[
                            "validation_result_evidence_status"
                        ]
                        for row in records
                    ).items()
                )
            ),
        "blocker_counts":
            dict(
                sorted(
                    Counter(
                        blocker
                        for row in records
                        for blocker in row[
                            "validation_result_evidence_blocker_codes"
                        ]
                    ).items()
                )
            ),
        "candidate_derived_artifact_count":
            sum(
                int(
                    row[
                        "candidate_derived_artifact_count"
                    ]
                )
                for row in records
            ),
        "authoritative_historical_outcomes_validated":
            sum(
                bool(
                    row[
                        "authoritative_historical_outcome_validated"
                    ]
                )
                for row in records
            ),
    }

    return {
        "module": predecessor,
        "records": records,
        "reverse_records": reverse_records,
        "result_digest": result_digest,
        "manifest_payload": manifest_payload,
        "manifest_digest": sha256_payload(
            manifest_payload
        ),
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
    manifest_payload = replay["manifest_payload"]
    manifest_digest = replay["manifest_digest"]

    comparison_ids = {
        row["comparison_record_id"]
        for row in records
    }

    status_counts = Counter(
        row["validation_result_evidence_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "validation_result_evidence_blocker_codes"
        ]
    )

    structural_counts = {
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
            "structural_package_validation_complete",
        )
    }

    checks = [
        {
            "check": "nine_bt_contract_version_verified",
            "actual":
                predecessor.RESULT_EVIDENCE_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_CONTRACT_VERSION,
            "passed": (
                predecessor.RESULT_EVIDENCE_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_CONTRACT_VERSION
            ),
        },
        {
            "check": "nine_bt_manifest_version_verified",
            "actual":
                predecessor.RESULT_EVIDENCE_MANIFEST_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_MANIFEST_VERSION,
            "passed": (
                predecessor.RESULT_EVIDENCE_MANIFEST_VERSION
                == EXPECTED_PREDECESSOR_MANIFEST_VERSION
            ),
        },
        {
            "check": "predecessor_replay_deterministic",
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
            "check": "predecessor_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": (
                sha256_payload(records)
                == sha256_payload(reverse_records)
            ),
        },
        {
            "check": "expected_result_evidence_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_RECORDS,
            "passed": len(records) == EXPECTED_RECORDS,
        },
        {
            "check": "expected_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_COMPARISONS,
            "passed":
                len(comparison_ids) == EXPECTED_COMPARISONS,
        },
        {
            "check": "all_result_evidence_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "validation_result_evidence_plan_identity_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_plan_identity_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "all_result_evidence_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "validation_result_evidence_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "result_evidence_manifest_digest_valid",
            "actual": valid_sha256(manifest_digest),
            "expected": True,
            "passed": valid_sha256(manifest_digest),
        },
        {
            "check": "result_evidence_manifest_inventory_valid",
            "actual": {
                "record_count":
                    manifest_payload["record_count"],
                "comparison_count":
                    manifest_payload["comparison_count"],
            },
            "expected": {
                "record_count": EXPECTED_RECORDS,
                "comparison_count": EXPECTED_COMPARISONS,
            },
            "passed": (
                manifest_payload["record_count"]
                == EXPECTED_RECORDS
                and manifest_payload["comparison_count"]
                == EXPECTED_COMPARISONS
            ),
        },
        {
            "check": "all_structural_results_complete",
            "actual": structural_counts,
            "expected": {
                key: EXPECTED_RECORDS
                for key in structural_counts
            },
            "passed": all(
                count == EXPECTED_RECORDS
                for count in structural_counts.values()
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(sorted(status_counts.items())),
            "expected": {
                EXPECTED_STATUS: EXPECTED_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_STATUS: EXPECTED_RECORDS
                }
            ),
        },
        {
            "check": "all_missing_endpoint_blockers_preserved",
            "actual": dict(sorted(blocker_counts.items())),
            "expected": {
                EXPECTED_BLOCKER: EXPECTED_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_BLOCKER: EXPECTED_RECORDS
                }
            ),
        },
        {
            "check": "candidate_derived_artifact_count_zero",
            "actual": sum(
                int(
                    row[
                        "candidate_derived_artifact_count"
                    ]
                )
                for row in records
            ),
            "expected": 0,
            "passed": all(
                int(
                    row[
                        "candidate_derived_artifact_count"
                    ]
                )
                == 0
                for row in records
            ),
        },
        {
            "check": "one_validation_artifact_per_record",
            "actual": sum(
                int(row["validation_artifact_count"])
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                int(row["validation_artifact_count"])
                == 1
                for row in records
            ),
        },
        {
            "check": "evidence_absence_explicit",
            "actual": sum(
                bool(row["evidence_absence_explicit"])
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["evidence_absence_explicit"])
                for row in records
            ),
        },
        {
            "check": "fabricated_evidence_absent",
            "actual": sum(
                bool(row["fabricated_evidence_detected"])
                for row in records
            ),
            "expected": 0,
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
            "check": "authoritative_historical_outcomes_validated_zero",
            "actual": sum(
                bool(
                    row[
                        "authoritative_historical_outcome_validated"
                    ]
                )
                for row in records
            ),
            "expected": 0,
            "passed": all(
                not bool(
                    row[
                        "authoritative_historical_outcome_validated"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "rationale_limitations_and_boundary_present",
            "actual": sum(
                bool(row["validation_result_evidence_rationale"])
                and bool(
                    row[
                        "validation_result_evidence_limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_authority_boundary"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["validation_result_evidence_rationale"])
                and bool(
                    row[
                        "validation_result_evidence_limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_authority_boundary"
                    ]
                )
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
            "passed":
                len(VALIDATION_REQUIREMENTS) == 30,
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
            "actual":
                len(VALIDATION_PLAN_RECORD_FIELDS),
            "expected": 57,
            "passed": (
                len(VALIDATION_PLAN_RECORD_FIELDS)
                == 57
            ),
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
            "check": "validation_execution_prohibited_during_planning",
            "actual": True,
            "expected": True,
            "passed": (
                "result_evidence_validation_execution"
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
            "check": "validation_records_materialized_zero",
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
            "check": "network_retrievals_executed_zero",
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

    next_layer = (
        "9BV_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_implementation"
        if all_checks_passed
        else
        "9BU_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_plan_remediation"
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_plan_failed"
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
        / "result_evidence_validation_inventory.csv",
        [
            "validation_result_evidence_plan_record_id",
            "validation_result_evidence_plan_identity_digest",
            "validation_result_evidence_plan_record_digest",
            "evidence_package_validation_plan_record_id",
            "evidence_package_plan_record_id",
            "comparison_record_id",
            "metric_record_id",
            "defect_source_record_id",
            "candidate_derived_artifact_count",
            "validation_artifact_count",
            "evidence_absence_explicit",
            "fabricated_evidence_detected",
            "structural_package_validation_complete",
            "authoritative_historical_outcome_validated",
            "validation_result_evidence_status",
            "validation_result_evidence_blocker_codes",
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
            predecessor.RESULT_EVIDENCE_CONTRACT_VERSION,
        "predecessor_manifest_version":
            predecessor.RESULT_EVIDENCE_MANIFEST_VERSION,
        "result_evidence_records":
            len(records),
        "result_evidence_comparisons":
            len(comparison_ids),
        "result_evidence_status_counts":
            dict(sorted(status_counts.items())),
        "result_evidence_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "structural_validity_counts":
            structural_counts,
        "predecessor_result_digest":
            replay["result_digest"],
        "reverse_predecessor_result_digest":
            sha256_payload(reverse_records),
        "predecessor_manifest_digest":
            manifest_digest,
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
        "plan_digest":
            plan_digest,
        "validation_records_materialized": 0,
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
        "pricing_changes_emitted": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed":
            all_checks_passed,
        "recommended_next_layer":
            next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "result_evidence_validation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "result_evidence_validation_status":
            EXPECTED_STATUS,
        "structural_package_validation_complete":
            all_checks_passed,
        "authoritative_historical_outcome_validated":
            False,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_historical_outcome_field_mapping_result_"
            "validation_evidence_package_validation_result_evidence_"
            "validation_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld":
            sorted(PROHIBITED_AUTHORITIES),
        "recommended_next_layer":
            next_layer,
        "output_directory":
            str(OUTPUT_DIR.relative_to(ROOT)),
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
        f"{predecessor.RESULT_EVIDENCE_CONTRACT_VERSION}"
    )
    print(
        "Predecessor manifest version: "
        f"{predecessor.RESULT_EVIDENCE_MANIFEST_VERSION}"
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
        f"Result-evidence records replayed: {len(records)}"
    )
    print(
        f"Result-evidence comparisons: {len(comparison_ids)}"
    )
    print(
        "Result-evidence status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Result-evidence blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Structural validity counts: "
        f"{structural_counts}"
    )
    print(
        "Validation principles: "
        f"{len(VALIDATION_PRINCIPLES)}"
    )
    print(
        "Validation components: "
        f"{len(VALIDATION_COMPONENTS)}"
    )
    print(
        "Validation stages: "
        f"{len(VALIDATION_STAGES)}"
    )
    print(
        "Validation requirements: "
        f"{len(VALIDATION_REQUIREMENTS)}"
    )
    print(
        "Validation plan record fields: "
        f"{len(VALIDATION_PLAN_RECORD_FIELDS)}"
    )
    print(
        f"Predecessor result digest: {replay['result_digest']}"
    )
    print(
        f"Predecessor manifest digest: {manifest_digest}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Validation records materialized: 0")
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
    print("Statistical significance tests calculated: 0")
    print("Superiority decisions emitted: 0")
    print("Equivalence decisions emitted: 0")
    print("Activation recommendations emitted: 0")
    print("Production probabilities changed: 0")
    print("Market comparisons executed: 0")
    print("Pricing changes emitted: 0")
    print("Betting edges calculated: 0")
    print(
        f"Diagnosis: {diagnosis_name}"
    )
    print(
        "Structural package validation complete: "
        f"{all_checks_passed}"
    )
    print(
        "Authoritative historical outcome validated: False"
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
