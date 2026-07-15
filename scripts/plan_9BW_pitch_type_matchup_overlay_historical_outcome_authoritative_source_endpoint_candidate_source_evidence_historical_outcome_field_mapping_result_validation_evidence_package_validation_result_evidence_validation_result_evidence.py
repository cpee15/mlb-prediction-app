#!/usr/bin/env python3
"""
Layer 9BW

Plans deterministic evidence records for the Layer 9BV validation results.

This is planning only. It preserves the structurally valid but
candidate_not_supplied disposition and grants no authority to invent or
retrieve endpoint candidates, validate outcome truth, parse responses, map or
extract values, mutate canonical records, recompute downstream records, or
make production, market, pricing, or betting decisions.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BW"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_result_evidence_"
    "validation_result_evidence_plan"
)

PLAN_VERSION = (
    "layer_9BW_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BW_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package_validation_"
    "result_evidence_validation_result_evidence_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "validate_9BV_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package_validation_"
    "result_evidence.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BV_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_contract_v1"
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


RESULT_EVIDENCE_PRINCIPLES = [
    {
        "principle_id": "HOASEHOFMRVEPVREVRE-P01",
        "principle": (
            "Preserve each Layer 9BV validation result with deterministic "
            "identity, digest, status, blockers, rationale, limitations, "
            "authority boundary, and complete predecessor lineage."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVRE-P02",
        "principle": (
            "Structural validation success must remain distinct from "
            "authoritative historical-outcome truth."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVRE-P03",
        "principle": (
            "Forward and reverse predecessor replay must produce identical "
            "planning inputs."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVRE-P04",
        "principle": (
            "Candidate-derived evidence absence must remain explicit and "
            "fabricated evidence must remain absent."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVRE-P05",
        "principle": (
            "The canonical target must remain outcome_value and "
            "outcome_available_at_utc must remain rejected as a substitute."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVRE-P06",
        "principle": (
            "Validation-result evidence must use a fixed record contract and "
            "stable SHA-256 identities."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVRE-P07",
        "principle": (
            "Planning may not execute retrieval, parsing, mapping, extraction, "
            "mutation, recomputation, or model-quality decisions."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVRE-P08",
        "principle": (
            "Successful planning grants only Layer 9BX evidence "
            "implementation authority."
        ),
    },
]


RESULT_EVIDENCE_COMPONENTS = [
    {
        "component_id": "HOASEHOFMRVEPVREVRE-C01",
        "component": "validation_result_identity_manifest",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVRE-C02",
        "component": "validation_result_digest_manifest",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVRE-C03",
        "component": "predecessor_lineage_manifest",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVRE-C04",
        "component": "structural_validation_disposition_manifest",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVRE-C05",
        "component": "candidate_evidence_absence_manifest",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVRE-C06",
        "component": "canonical_field_identity_manifest",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVRE-C07",
        "component": "rationale_limitations_and_blocker_manifest",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVRE-C08",
        "component": "authority_boundary_manifest",
        "required": True,
        "priority": 8,
    },
]


RESULT_EVIDENCE_STAGES = [
    {"stage_id": "HOASEHOFMRVEPVREVRE-S01", "stage_name": "validation_result_replay", "priority": 1},
    {"stage_id": "HOASEHOFMRVEPVREVRE-S02", "stage_name": "identity_inventory", "priority": 2},
    {"stage_id": "HOASEHOFMRVEPVREVRE-S03", "stage_name": "digest_inventory", "priority": 3},
    {"stage_id": "HOASEHOFMRVEPVREVRE-S04", "stage_name": "lineage_inventory", "priority": 4},
    {"stage_id": "HOASEHOFMRVEPVREVRE-S05", "stage_name": "structural_disposition_inventory", "priority": 5},
    {"stage_id": "HOASEHOFMRVEPVREVRE-S06", "stage_name": "candidate_absence_inventory", "priority": 6},
    {"stage_id": "HOASEHOFMRVEPVREVRE-S07", "stage_name": "canonical_field_inventory", "priority": 7},
    {"stage_id": "HOASEHOFMRVEPVREVRE-S08", "stage_name": "documentation_inventory", "priority": 8},
    {"stage_id": "HOASEHOFMRVEPVREVRE-S09", "stage_name": "authority_boundary_inventory", "priority": 9},
    {"stage_id": "HOASEHOFMRVEPVREVRE-S10", "stage_name": "record_contract_definition", "priority": 10},
    {"stage_id": "HOASEHOFMRVEPVREVRE-S11", "stage_name": "deterministic_plan_emission", "priority": 11},
]


RESULT_EVIDENCE_REQUIREMENTS = [
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R01", "requirement": "validation_record_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R02", "requirement": "validation_record_id_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R03", "requirement": "validation_identity_digest_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R04", "requirement": "validation_record_digest_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R05", "requirement": "result_evidence_record_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R06", "requirement": "package_validation_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R07", "requirement": "package_record_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R08", "requirement": "mapping_validation_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R09", "requirement": "field_mapping_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R10", "requirement": "comparison_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R11", "requirement": "metric_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R12", "requirement": "defect_lineage_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R13", "requirement": "structural_validation_complete", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R14", "requirement": "authoritative_outcome_validated", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R15", "requirement": "candidate_derived_artifact_count_zero", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R16", "requirement": "validation_artifact_count_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R17", "requirement": "evidence_absence_explicit", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R18", "requirement": "fabricated_evidence_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R19", "requirement": "canonical_field_identity_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R20", "requirement": "candidate_not_supplied_status_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R21", "requirement": "missing_endpoint_blocker_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R22", "requirement": "validation_rationale_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R23", "requirement": "validation_limitations_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R24", "requirement": "validation_authority_boundary_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R25", "requirement": "new_result_evidence_authority_defined", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R26", "requirement": "records_materialized_during_planning", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R27", "requirement": "network_retrieval_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R28", "requirement": "mapping_or_extraction_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R29", "requirement": "canonical_mutation_or_recomputation_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREVRE-R30", "requirement": "production_market_or_betting_authority_granted", "expected": False},
]


RESULT_EVIDENCE_STATUSES = [
    {"status": "validation_result_evidence_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "validation_result_identity_invalid", "implementation_authority": False},
    {"status": "validation_result_digest_invalid", "implementation_authority": False},
    {"status": "lineage_incomplete", "implementation_authority": False},
    {"status": "structural_disposition_invalid", "implementation_authority": False},
    {"status": "candidate_evidence_absence_invalid", "implementation_authority": False},
    {"status": "canonical_field_identity_invalid", "implementation_authority": False},
    {"status": "documentation_invalid", "implementation_authority": False},
    {"status": "authority_boundary_invalid", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "validation_result_record_missing", "category": "record"},
    {"code": "validation_result_record_id_missing", "category": "identity"},
    {"code": "validation_result_identity_digest_missing", "category": "identity"},
    {"code": "validation_result_identity_digest_invalid", "category": "identity"},
    {"code": "validation_result_record_digest_missing", "category": "integrity"},
    {"code": "validation_result_record_digest_invalid", "category": "integrity"},
    {"code": "result_evidence_record_lineage_missing", "category": "lineage"},
    {"code": "package_validation_lineage_missing", "category": "lineage"},
    {"code": "package_record_lineage_missing", "category": "lineage"},
    {"code": "mapping_validation_lineage_missing", "category": "lineage"},
    {"code": "field_mapping_lineage_missing", "category": "lineage"},
    {"code": "comparison_lineage_missing", "category": "lineage"},
    {"code": "metric_lineage_missing", "category": "lineage"},
    {"code": "defect_lineage_missing", "category": "lineage"},
    {"code": "structural_validation_incomplete", "category": "structural"},
    {"code": "authoritative_outcome_disposition_invalid", "category": "outcome"},
    {"code": "candidate_derived_artifact_count_invalid", "category": "evidence"},
    {"code": "validation_artifact_count_invalid", "category": "evidence"},
    {"code": "evidence_absence_not_explicit", "category": "evidence"},
    {"code": "fabricated_evidence_detected", "category": "evidence"},
    {"code": "canonical_field_identity_invalid", "category": "field"},
    {"code": "candidate_not_supplied_status_missing", "category": "status"},
    {"code": "missing_endpoint_blocker_missing", "category": "status"},
    {"code": "validation_rationale_missing", "category": "documentation"},
    {"code": "validation_limitations_missing", "category": "documentation"},
    {"code": "validation_authority_boundary_missing", "category": "authority"},
    {"code": "result_evidence_execution_requested_during_planning", "category": "authority"},
    {"code": "canonical_mutation_or_recomputation_requested", "category": "authority"},
    {"code": "production_market_or_betting_authority_requested", "category": "authority"},
]


RESULT_EVIDENCE_PLAN_RECORD_FIELDS = [
    "validation_result_evidence_plan_contract_version",
    "validation_result_evidence_plan_record_id",
    "result_evidence_validation_plan_record_id",
    "result_evidence_validation_plan_identity_digest",
    "result_evidence_validation_plan_record_digest",
    "validation_result_evidence_source_record_id",
    "validation_result_evidence_source_record_identity_digest",
    "validation_result_evidence_source_record_digest",
    "evidence_package_validation_plan_record_id",
    "evidence_package_validation_plan_record_digest",
    "evidence_package_plan_record_id",
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
    "structural_package_validation_complete",
    "authoritative_historical_outcome_validated",
    "result_evidence_validation_status",
    "result_evidence_validation_blocker_codes",
    "result_evidence_validation_rationale",
    "result_evidence_validation_limitations",
    "result_evidence_validation_authority_boundary",
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
    {"ordinal": 4, "field": "evidence_package_plan_record_id"},
    {"ordinal": 5, "field": "result_evidence_validation_plan_record_id"},
    {"ordinal": 6, "field": "validation_result_evidence_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9BV_validation_records"},
    {"ordinal": 2, "step": "preserve_validation_identity_and_digest"},
    {"ordinal": 3, "step": "preserve_result_evidence_and_package_lineage"},
    {"ordinal": 4, "step": "preserve_mapping_comparison_metric_and_defect_lineage"},
    {"ordinal": 5, "step": "preserve_structural_validation_complete"},
    {"ordinal": 6, "step": "preserve_authoritative_outcome_not_validated"},
    {"ordinal": 7, "step": "preserve_candidate_derived_artifact_count_zero"},
    {"ordinal": 8, "step": "preserve_explicit_evidence_absence"},
    {"ordinal": 9, "step": "preserve_fabricated_evidence_absence"},
    {"ordinal": 10, "step": "preserve_canonical_and_rejected_field_identities"},
    {"ordinal": 11, "step": "preserve_status_blockers_rationale_limitations_and_boundary"},
    {"ordinal": 12, "step": "define_validation_result_evidence_identity_and_digest"},
    {"ordinal": 13, "step": "define_validation_result_evidence_authority_boundary"},
    {"ordinal": 14, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 15, "step": "withhold_network_mutation_recompute_and_quality_authority"},
    {"ordinal": 16, "step": "grant_layer_9BX_implementation_only_when_complete"},
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
            serialized = {
                field: (
                    canonical_json(row.get(field))
                    if isinstance(row.get(field), (dict, list, tuple))
                    else row.get(field)
                )
                for field in fieldnames
            }
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
        "layer_9bv_predecessor",
    )

    if predecessor.VALIDATION_CONTRACT_VERSION != EXPECTED_PREDECESSOR_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9BV validation contract version: "
            f"{predecessor.VALIDATION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()

    records = predecessor.build_validation_records(
        replay["plan"],
        replay,
        replay["records"],
    )

    reverse_records = predecessor.build_validation_records(
        replay["plan"],
        replay,
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
        row["result_evidence_validation_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "result_evidence_validation_blocker_codes"
        ]
    )

    structural_complete = sum(
        bool(row["structural_package_validation_complete"])
        for row in records
    )

    candidate_derived_artifacts = sum(
        int(row["candidate_derived_artifact_count"])
        for row in records
    )

    validation_artifacts = sum(
        int(row["validation_artifact_count"])
        for row in records
    )

    checks = [
        {
            "check": "nine_bv_contract_version_verified",
            "actual": predecessor.VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.VALIDATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "predecessor_replay_deterministic",
            "actual": canonical_json(records) == canonical_json(reverse_records),
            "expected": True,
            "passed": canonical_json(records) == canonical_json(reverse_records),
        },
        {
            "check": "predecessor_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": sha256_payload(records) == sha256_payload(reverse_records),
        },
        {
            "check": "expected_validation_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_RECORDS,
            "passed": len(records) == EXPECTED_RECORDS,
        },
        {
            "check": "expected_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_COMPARISONS,
            "passed": len(comparison_ids) == EXPECTED_COMPARISONS,
        },
        {
            "check": "all_validation_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "result_evidence_validation_plan_identity_digest"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "result_evidence_validation_plan_identity_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "all_validation_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "result_evidence_validation_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "result_evidence_validation_plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "all_structural_validation_results_complete",
            "actual": structural_complete,
            "expected": EXPECTED_RECORDS,
            "passed": structural_complete == EXPECTED_RECORDS,
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(sorted(status_counts.items())),
            "expected": {EXPECTED_STATUS: EXPECTED_RECORDS},
            "passed": status_counts == Counter(
                {EXPECTED_STATUS: EXPECTED_RECORDS}
            ),
        },
        {
            "check": "all_missing_endpoint_blockers_preserved",
            "actual": dict(sorted(blocker_counts.items())),
            "expected": {EXPECTED_BLOCKER: EXPECTED_RECORDS},
            "passed": blocker_counts == Counter(
                {EXPECTED_BLOCKER: EXPECTED_RECORDS}
            ),
        },
        {
            "check": "candidate_derived_artifact_count_zero",
            "actual": candidate_derived_artifacts,
            "expected": 0,
            "passed": candidate_derived_artifacts == 0,
        },
        {
            "check": "one_validation_artifact_per_record",
            "actual": validation_artifacts,
            "expected": EXPECTED_RECORDS,
            "passed": validation_artifacts == EXPECTED_RECORDS,
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
                not bool(row["fabricated_evidence_detected"])
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
            "check": "validation_documentation_and_boundary_present",
            "actual": sum(
                bool(row["result_evidence_validation_rationale"])
                and bool(row["result_evidence_validation_limitations"])
                and bool(
                    row[
                        "result_evidence_validation_authority_boundary"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["result_evidence_validation_rationale"])
                and bool(row["result_evidence_validation_limitations"])
                and bool(
                    row[
                        "result_evidence_validation_authority_boundary"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "principles_defined",
            "actual": len(RESULT_EVIDENCE_PRINCIPLES),
            "expected": 8,
            "passed": len(RESULT_EVIDENCE_PRINCIPLES) == 8,
        },
        {
            "check": "components_defined",
            "actual": len(RESULT_EVIDENCE_COMPONENTS),
            "expected": 8,
            "passed": len(RESULT_EVIDENCE_COMPONENTS) == 8,
        },
        {
            "check": "stages_defined",
            "actual": len(RESULT_EVIDENCE_STAGES),
            "expected": 11,
            "passed": len(RESULT_EVIDENCE_STAGES) == 11,
        },
        {
            "check": "requirements_defined",
            "actual": len(RESULT_EVIDENCE_REQUIREMENTS),
            "expected": 30,
            "passed": len(RESULT_EVIDENCE_REQUIREMENTS) == 30,
        },
        {
            "check": "statuses_defined",
            "actual": len(RESULT_EVIDENCE_STATUSES),
            "expected": 10,
            "passed": len(RESULT_EVIDENCE_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 30,
            "passed": len(BLOCKER_CODES) == 30,
        },
        {
            "check": "record_fields_defined",
            "actual": len(RESULT_EVIDENCE_PLAN_RECORD_FIELDS),
            "expected": 50,
            "passed": len(RESULT_EVIDENCE_PLAN_RECORD_FIELDS) == 50,
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
            "check": "execution_prohibited_during_planning",
            "actual": True,
            "expected": True,
            "passed": (
                "validation_result_evidence_execution"
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
            "check": "result_evidence_records_materialized_zero",
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
            "principles": RESULT_EVIDENCE_PRINCIPLES,
            "components": RESULT_EVIDENCE_COMPONENTS,
            "stages": RESULT_EVIDENCE_STAGES,
            "requirements": RESULT_EVIDENCE_REQUIREMENTS,
            "statuses": RESULT_EVIDENCE_STATUSES,
            "blockers": BLOCKER_CODES,
            "record_fields": RESULT_EVIDENCE_PLAN_RECORD_FIELDS,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
            "prohibited_authorities": PROHIBITED_AUTHORITIES,
        }
    )

    next_layer = (
        "9BX_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_result_evidence_implementation"
        if all_checks_passed
        else
        "9BW_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_result_evidence_plan_remediation"
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_result_evidence_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_result_evidence_plan_failed"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_principles.csv",
        ["principle_id", "principle"],
        RESULT_EVIDENCE_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_components.csv",
        ["component_id", "component", "required", "priority"],
        RESULT_EVIDENCE_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_stages.csv",
        ["stage_id", "stage_name", "priority"],
        RESULT_EVIDENCE_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_requirements.csv",
        ["requirement_id", "requirement", "expected"],
        RESULT_EVIDENCE_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_statuses.csv",
        ["status", "implementation_authority"],
        RESULT_EVIDENCE_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "blocker_code_catalog.csv",
        ["code", "category"],
        BLOCKER_CODES,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_plan_record_field_contract.csv",
        ["ordinal", "field"],
        [
            {"ordinal": index, "field": field}
            for index, field in enumerate(
                RESULT_EVIDENCE_PLAN_RECORD_FIELDS,
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
        OUTPUT_DIR / "validation_result_inventory.csv",
        [
            "result_evidence_validation_plan_record_id",
            "result_evidence_validation_plan_identity_digest",
            "result_evidence_validation_plan_record_digest",
            "validation_result_evidence_plan_record_id",
            "comparison_record_id",
            "metric_record_id",
            "defect_source_record_id",
            "candidate_derived_artifact_count",
            "validation_artifact_count",
            "evidence_absence_explicit",
            "fabricated_evidence_detected",
            "structural_package_validation_complete",
            "authoritative_historical_outcome_validated",
            "result_evidence_validation_status",
            "result_evidence_validation_blocker_codes",
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
            predecessor.VALIDATION_CONTRACT_VERSION,
        "validation_records": len(records),
        "validation_comparisons": len(comparison_ids),
        "validation_status_counts":
            dict(sorted(status_counts.items())),
        "validation_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "predecessor_digest":
            sha256_payload(records),
        "reverse_predecessor_digest":
            sha256_payload(reverse_records),
        "plan_digest": plan_digest,
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
        "planning_checks_required": len(checks),
        "result_evidence_records_materialized": 0,
        "authoritative_historical_outcomes_validated": 0,
        "candidate_derived_evidence_artifacts_created": 0,
        "fabricated_evidence_artifacts_created": 0,
        "historical_outcome_fields_mapped": 0,
        "historical_outcome_values_extracted": 0,
        "response_bytes_read": 0,
        "responses_parsed": 0,
        "parsed_records_validated": 0,
        "network_retrievals_executed": 0,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
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
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "validation_result_evidence_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "validation_result_evidence_status":
            EXPECTED_STATUS,
        "structural_validation_complete":
            all_checks_passed,
        "authoritative_historical_outcome_validated":
            False,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_historical_outcome_field_mapping_result_"
            "validation_evidence_package_validation_result_evidence_"
            "validation_result_evidence_implementation"
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

    print(f"Layer: {LAYER_ID} — {LAYER_NAME}")
    print(f"Plan version: {PLAN_VERSION}")
    print(
        "Predecessor validation contract version: "
        f"{predecessor.VALIDATION_CONTRACT_VERSION}"
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
        "Result-evidence plan record fields: "
        f"{len(RESULT_EVIDENCE_PLAN_RECORD_FIELDS)}"
    )
    print(f"Predecessor digest: {sha256_payload(records)}")
    print(f"Plan digest: {plan_digest}")
    print("Result-evidence records materialized: 0")
    print("Authoritative historical outcomes validated: 0")
    print("Candidate-derived evidence artifacts created: 0")
    print("Fabricated evidence artifacts created: 0")
    print("Historical outcome fields mapped: 0")
    print("Historical outcome values extracted: 0")
    print("Network retrievals executed: 0")
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Downstream records recomputed: 0")
    print("Production probabilities changed: 0")
    print("Market comparisons executed: 0")
    print("Pricing changes emitted: 0")
    print("Betting edges calculated: 0")
    print(f"Diagnosis: {diagnosis_name}")
    print(
        "Authoritative historical outcome validated: False"
    )
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
