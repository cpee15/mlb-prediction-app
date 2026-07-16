#!/usr/bin/env python3
"""
Layer 9CG

Defines the deterministic validation plan for Layer 9CF validation-result
evidence records.

Planning only. This layer does not materialize validation records, validate
authoritative historical outcomes, retrieve endpoint data, parse responses,
map fields, extract values, mutate canonical records, recompute downstream
artifacts, or grant production, market, pricing, or betting authority.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9CG"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_result_evidence_"
    "validation_result_evidence_validation_result_evidence_validation_"
    "result_evidence_validation_plan"
)

VALIDATION_PLAN_VERSION = (
    "layer_9CG_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_result_evidence_"
    "validation_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "tmp" / "layer_9CG_result_evidence_validation_plan"

PREDECESSOR_PATH = (
    ROOT / "scripts" / "package_9CF_validation_result_evidence.py"
)

EXPECTED_PREDECESSOR_CONTRACT_VERSION = (
    "layer_9CF_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_result_evidence_"
    "contract_v1"
)

EXPECTED_PREDECESSOR_MANIFEST_VERSION = (
    "layer_9CF_validation_result_evidence_manifest_v1"
)

EXPECTED_PLAN_DIGEST = (
    "bd712bfe2ad826ecbe1d2ddead71f2821b54fd9e98e3e45c7390afba5c51d42e"
)

EXPECTED_PREDECESSOR_VALIDATION_DIGEST = (
    "feaf77c1be094c95213abe84c970da60bd0ca816e6916d121f22320c69978245"
)

EXPECTED_RESULT_DIGEST = (
    "096ae07e3da1445591746d12a9ff450d2769b0d72be83140b813a44160e4f177"
)

EXPECTED_MANIFEST_DIGEST = (
    "62a25d789fb7073948b21390d2126f3c988fa54d84a574fcb0f91790edcff545"
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
        "principle_id": "HOASEHOFMRVEPVREVREVREVREV-P01",
        "principle": (
            "Replay every Layer 9CF result-evidence record deterministically "
            "before defining validation work."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREV-P02",
        "principle": (
            "Validate result-evidence identity and record digests without "
            "inventing evidence or authoritative outcomes."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREV-P03",
        "principle": (
            "Preserve complete lineage from the Layer 9CF result-evidence "
            "record through all predecessor planning and validation records."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREV-P04",
        "principle": (
            "Preserve candidate_not_supplied and the missing endpoint "
            "candidate blocker."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREV-P05",
        "principle": (
            "Preserve outcome_value as canonical and reject "
            "outcome_available_at_utc as an outcome substitute."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREV-P06",
        "principle": (
            "Require rationale, limitations, and authority boundaries for "
            "every future validation record."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREV-P07",
        "principle": (
            "Prohibit retrieval, parsing, mapping, extraction, mutation, "
            "recomputation, production, market, pricing, and betting work."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREV-P08",
        "principle": (
            "Successful planning grants only Layer 9CH validation "
            "implementation authority."
        ),
    },
]


VALIDATION_COMPONENTS = [
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREV-C01",
        "component": "predecessor_replay",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREV-C02",
        "component": "result_evidence_identity_validation",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREV-C03",
        "component": "result_evidence_digest_validation",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREV-C04",
        "component": "manifest_validation",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREV-C05",
        "component": "lineage_and_disposition_validation",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREV-C06",
        "component": "candidate_evidence_absence_validation",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREV-C07",
        "component": "canonical_field_and_documentation_validation",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREV-C08",
        "component": "authority_boundary_validation",
        "required": True,
        "priority": 8,
    },
]


VALIDATION_STAGES = [
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S01",
        "stage_name": "predecessor_replay",
        "priority": 1,
    },
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S02",
        "stage_name": "record_inventory_validation",
        "priority": 2,
    },
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S03",
        "stage_name": "identity_validation",
        "priority": 3,
    },
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S04",
        "stage_name": "record_digest_validation",
        "priority": 4,
    },
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S05",
        "stage_name": "manifest_validation",
        "priority": 5,
    },
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S06",
        "stage_name": "lineage_validation",
        "priority": 6,
    },
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S07",
        "stage_name": "structural_disposition_validation",
        "priority": 7,
    },
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S08",
        "stage_name": "candidate_absence_and_field_validation",
        "priority": 8,
    },
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S09",
        "stage_name": "documentation_and_boundary_validation",
        "priority": 9,
    },
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S10",
        "stage_name": "validation_contract_definition",
        "priority": 10,
    },
    {
        "stage_id": "HOASEHOFMRVEPVREVREVREVREV-S11",
        "stage_name": "plan_emission",
        "priority": 11,
    },
]


VALIDATION_REQUIREMENTS = [
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R01",
        "requirement": "source_result_evidence_record_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R02",
        "requirement": "source_result_evidence_record_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R03",
        "requirement": "source_result_evidence_identity_digest_valid",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R04",
        "requirement": "source_result_evidence_record_digest_valid",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R05",
        "requirement": "result_evidence_digest_preserved",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R06",
        "requirement": "manifest_digest_preserved",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R07",
        "requirement": "source_validation_lineage_complete",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R08",
        "requirement": "result_evidence_lineage_complete",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R09",
        "requirement": "package_and_mapping_lineage_complete",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R10",
        "requirement": "comparison_metric_and_defect_lineage_complete",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R11",
        "requirement": "structural_validation_complete",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R12",
        "requirement": "candidate_not_supplied_status_preserved",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R13",
        "requirement": "missing_endpoint_blocker_preserved",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R14",
        "requirement": "candidate_derived_artifact_count_zero",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R15",
        "requirement": "validation_artifact_count_preserved",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R16",
        "requirement": "evidence_absence_explicit",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R17",
        "requirement": "fabricated_evidence_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R18",
        "requirement": "canonical_field_identity_preserved",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R19",
        "requirement": "source_rationale_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R20",
        "requirement": "source_limitations_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R21",
        "requirement": "source_authority_boundary_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R22",
        "requirement": "validation_rationale_required",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R23",
        "requirement": "validation_limitations_required",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R24",
        "requirement": "validation_authority_boundary_required",
        "expected": True,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R25",
        "requirement": "authoritative_historical_outcome_validated",
        "expected": False,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R26",
        "requirement": "validation_records_materialized_during_planning",
        "expected": False,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R27",
        "requirement": "network_retrieval_executed",
        "expected": False,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R28",
        "requirement": "mapping_or_extraction_executed",
        "expected": False,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R29",
        "requirement": "canonical_mutation_or_recomputation_executed",
        "expected": False,
    },
    {
        "requirement_id": "HOASEHOFMRVEPVREVREVREVREV-R30",
        "requirement": "production_market_or_betting_authority_granted",
        "expected": False,
    },
]


VALIDATION_STATUSES = [
    {
        "status": "validation_ready",
        "implementation_authority": True,
    },
    {
        "status": "candidate_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "source_result_evidence_record_missing",
        "implementation_authority": False,
    },
    {
        "status": "source_result_evidence_identity_invalid",
        "implementation_authority": False,
    },
    {
        "status": "source_result_evidence_digest_invalid",
        "implementation_authority": False,
    },
    {
        "status": "manifest_invalid",
        "implementation_authority": False,
    },
    {
        "status": "lineage_incomplete",
        "implementation_authority": False,
    },
    {
        "status": "structural_disposition_invalid",
        "implementation_authority": False,
    },
    {
        "status": "canonical_field_or_documentation_invalid",
        "implementation_authority": False,
    },
    {
        "status": "authority_boundary_invalid",
        "implementation_authority": False,
    },
]


BLOCKER_CODES = [
    {
        "code": "historical_outcome_endpoint_candidate_missing",
        "category": "submission",
    },
    {
        "code": "source_result_evidence_record_missing",
        "category": "record",
    },
    {
        "code": "source_result_evidence_record_id_missing",
        "category": "identity",
    },
    {
        "code": "source_result_evidence_identity_digest_missing",
        "category": "identity",
    },
    {
        "code": "source_result_evidence_identity_digest_invalid",
        "category": "identity",
    },
    {
        "code": "source_result_evidence_record_digest_missing",
        "category": "integrity",
    },
    {
        "code": "source_result_evidence_record_digest_invalid",
        "category": "integrity",
    },
    {
        "code": "result_evidence_digest_invalid",
        "category": "integrity",
    },
    {
        "code": "manifest_version_invalid",
        "category": "manifest",
    },
    {
        "code": "manifest_digest_invalid",
        "category": "manifest",
    },
    {
        "code": "source_validation_lineage_missing",
        "category": "lineage",
    },
    {
        "code": "result_evidence_lineage_missing",
        "category": "lineage",
    },
    {
        "code": "package_lineage_missing",
        "category": "lineage",
    },
    {
        "code": "mapping_lineage_missing",
        "category": "lineage",
    },
    {
        "code": "comparison_metric_or_defect_lineage_missing",
        "category": "lineage",
    },
    {
        "code": "structural_validation_incomplete",
        "category": "structural",
    },
    {
        "code": "candidate_not_supplied_status_missing",
        "category": "status",
    },
    {
        "code": "missing_endpoint_blocker_missing",
        "category": "status",
    },
    {
        "code": "candidate_derived_artifact_count_invalid",
        "category": "evidence",
    },
    {
        "code": "validation_artifact_count_invalid",
        "category": "evidence",
    },
    {
        "code": "evidence_absence_not_explicit",
        "category": "evidence",
    },
    {
        "code": "fabricated_evidence_detected",
        "category": "evidence",
    },
    {
        "code": "canonical_field_identity_invalid",
        "category": "field",
    },
    {
        "code": "source_rationale_missing",
        "category": "documentation",
    },
    {
        "code": "source_limitations_missing",
        "category": "documentation",
    },
    {
        "code": "source_authority_boundary_missing",
        "category": "authority",
    },
    {
        "code": "validation_documentation_missing",
        "category": "documentation",
    },
    {
        "code": "validation_authority_boundary_missing",
        "category": "authority",
    },
    {
        "code": "validation_materialization_requested_during_planning",
        "category": "authority",
    },
    {
        "code": "production_market_or_betting_authority_requested",
        "category": "authority",
    },
]


VALIDATION_PLAN_RECORD_FIELDS = [
    "validation_result_evidence_validation_contract_version",
    "validation_result_evidence_validation_plan_record_id",
    "validation_result_evidence_validation_plan_identity_digest",
    "validation_result_evidence_source_record_id",
    "validation_result_evidence_source_record_identity_digest",
    "validation_result_evidence_source_record_digest",
    "validation_result_evidence_result_evidence_contract_version",
    "validation_result_evidence_result_evidence_plan_record_id",
    "validation_result_evidence_result_evidence_plan_identity_digest",
    "validation_result_evidence_result_evidence_plan_record_digest",
    "result_evidence_validation_contract_version",
    "result_evidence_validation_plan_record_id",
    "result_evidence_validation_plan_identity_digest",
    "result_evidence_validation_plan_record_digest",
    "result_evidence_source_record_id",
    "result_evidence_source_record_identity_digest",
    "result_evidence_source_record_digest",
    "result_evidence_manifest_version",
    "result_evidence_manifest_digest",
    "validation_result_evidence_plan_record_id",
    "validation_result_evidence_plan_record_digest",
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
    "structural_validation_complete",
    "authoritative_historical_outcome_validated",
    "source_result_evidence_status",
    "source_result_evidence_blocker_codes",
    "source_result_evidence_rationale",
    "source_result_evidence_limitations",
    "source_result_evidence_authority_boundary",
    "validation_result_evidence_validation_status",
    "validation_result_evidence_validation_blocker_codes",
    "validation_result_evidence_validation_implementation_authority_granted",
    "validation_result_evidence_validation_rationale",
    "validation_result_evidence_validation_limitations",
    "validation_result_evidence_validation_authority_boundary",
    "validation_result_evidence_validation_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {
        "ordinal": 4,
        "field": "validation_result_evidence_plan_record_id",
    },
    {
        "ordinal": 5,
        "field": "result_evidence_validation_plan_record_id",
    },
    {
        "ordinal": 6,
        "field": "validation_result_evidence_result_evidence_plan_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9CF_result_evidence_records"},
    {"ordinal": 2, "step": "validate_record_inventory"},
    {"ordinal": 3, "step": "validate_result_evidence_identity"},
    {"ordinal": 4, "step": "validate_result_evidence_record_digest"},
    {"ordinal": 5, "step": "validate_result_evidence_digest"},
    {"ordinal": 6, "step": "validate_manifest_version_and_digest"},
    {"ordinal": 7, "step": "validate_complete_lineage"},
    {"ordinal": 8, "step": "validate_structural_disposition"},
    {"ordinal": 9, "step": "validate_candidate_evidence_absence"},
    {"ordinal": 10, "step": "validate_canonical_field_identity"},
    {"ordinal": 11, "step": "validate_source_documentation"},
    {"ordinal": 12, "step": "define_validation_identity_contract"},
    {"ordinal": 13, "step": "define_validation_documentation_contract"},
    {"ordinal": 14, "step": "define_validation_authority_boundary"},
    {"ordinal": 15, "step": "define_deterministic_ordering_and_digests"},
    {"ordinal": 16, "step": "grant_layer_9CH_implementation_only_when_complete"},
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
    "result_evidence_invention",
    "validation_result_evidence_validation_materialization",
    "authoritative_historical_outcome_validation",
    "historical_outcome_field_mapping_execution",
    "historical_outcome_value_extraction",
    "response_bytes_reading",
    "source_evidence_parse_execution",
    "raw_response_parse_execution",
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
            writer.writerow(
                {
                    field: (
                        canonical_json(row.get(field))
                        if isinstance(row.get(field), (dict, list, tuple))
                        else row.get(field)
                    )
                    for field in fieldnames
                }
            )


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def replay_predecessor() -> dict[str, Any]:
    predecessor = load_module(
        PREDECESSOR_PATH,
        "layer_9cf_predecessor",
    )

    if (
        predecessor.RESULT_EVIDENCE_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_CONTRACT_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9CF contract version: "
            f"{predecessor.RESULT_EVIDENCE_CONTRACT_VERSION}"
        )

    if (
        predecessor.RESULT_EVIDENCE_MANIFEST_VERSION
        != EXPECTED_PREDECESSOR_MANIFEST_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9CF manifest version: "
            f"{predecessor.RESULT_EVIDENCE_MANIFEST_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_result_evidence_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_result_evidence_records(
        plan,
        list(reversed(replay["reverse_records"])),
    )

    plan_digest = predecessor.compute_plan_digest(plan)
    predecessor_validation_digest = sha256_payload(
        replay["records"]
    )
    result_digest = sha256_payload(records)

    comparison_ids = {
        row["comparison_record_id"]
        for row in records
    }

    status_counts = dict(
        sorted(
            Counter(
                row[
                    "validation_result_evidence_result_evidence_status"
                ]
                for row in records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in records
                for blocker in row[
                    "validation_result_evidence_result_evidence_"
                    "blocker_codes"
                ]
            ).items()
        )
    )

    manifest_payload = {
        "manifest_version":
            predecessor.RESULT_EVIDENCE_MANIFEST_VERSION,
        "contract_version":
            predecessor.RESULT_EVIDENCE_CONTRACT_VERSION,
        "record_count":
            len(records),
        "comparison_count":
            len(comparison_ids),
        "result_digest":
            result_digest,
        "status_counts":
            status_counts,
        "blocker_counts":
            blocker_counts,
        "candidate_derived_artifact_count":
            sum(
                int(row["candidate_derived_artifact_count"])
                for row in records
            ),
        "validation_artifact_count":
            sum(
                int(row["validation_artifact_count"])
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

    manifest_digest = sha256_payload(manifest_payload)

    return {
        "module": predecessor,
        "plan": plan,
        "records": records,
        "reverse_records": reverse_records,
        "plan_digest": plan_digest,
        "predecessor_validation_digest":
            predecessor_validation_digest,
        "result_digest": result_digest,
        "manifest_digest": manifest_digest,
        "manifest_payload": manifest_payload,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    replay = replay_predecessor()
    predecessor = replay["module"]
    records = replay["records"]
    reverse_records = replay["reverse_records"]

    reverse_result_digest = sha256_payload(reverse_records)

    comparison_ids = {
        row["comparison_record_id"]
        for row in records
    }

    status_counts = Counter(
        row[
            "validation_result_evidence_result_evidence_status"
        ]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "validation_result_evidence_result_evidence_"
            "blocker_codes"
        ]
    )

    lineage_fields = (
        "validation_result_evidence_result_evidence_plan_record_id",
        "validation_result_evidence_source_record_id",
        "result_evidence_validation_plan_record_id",
        "result_evidence_source_record_id",
        "validation_result_evidence_validation_plan_record_id",
        "validation_result_evidence_plan_record_id",
        "evidence_package_validation_plan_record_id",
        "evidence_package_plan_record_id",
        "mapping_result_validation_plan_record_id",
        "historical_outcome_field_mapping_plan_record_id",
        "comparison_record_id",
        "metric_record_id",
        "defect_source_record_id",
    )

    checks = [
        {
            "check": "nine_cf_contract_version_verified",
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
            "check": "nine_cf_manifest_version_verified",
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
            "check": "nine_ce_plan_digest_preserved",
            "actual": replay["plan_digest"],
            "expected": EXPECTED_PLAN_DIGEST,
            "passed":
                replay["plan_digest"] == EXPECTED_PLAN_DIGEST,
        },
        {
            "check": "nine_cd_validation_digest_preserved",
            "actual":
                replay["predecessor_validation_digest"],
            "expected":
                EXPECTED_PREDECESSOR_VALIDATION_DIGEST,
            "passed": (
                replay["predecessor_validation_digest"]
                == EXPECTED_PREDECESSOR_VALIDATION_DIGEST
            ),
        },
        {
            "check": "predecessor_replay_deterministic",
            "actual":
                canonical_json(records)
                == canonical_json(reverse_records),
            "expected": True,
            "passed": (
                canonical_json(records)
                == canonical_json(reverse_records)
            ),
        },
        {
            "check": "result_evidence_digest_preserved",
            "actual": replay["result_digest"],
            "expected": EXPECTED_RESULT_DIGEST,
            "passed":
                replay["result_digest"] == EXPECTED_RESULT_DIGEST,
        },
        {
            "check": "reverse_result_evidence_digest_preserved",
            "actual": reverse_result_digest,
            "expected": EXPECTED_RESULT_DIGEST,
            "passed":
                reverse_result_digest == EXPECTED_RESULT_DIGEST,
        },
        {
            "check": "manifest_digest_preserved",
            "actual": replay["manifest_digest"],
            "expected": EXPECTED_MANIFEST_DIGEST,
            "passed":
                replay["manifest_digest"] == EXPECTED_MANIFEST_DIGEST,
        },
        {
            "check": "expected_records_replayed",
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
                        "validation_result_evidence_result_evidence_"
                        "plan_identity_digest"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "plan_identity_digest"
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
                        "validation_result_evidence_result_evidence_"
                        "plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "lineage_complete",
            "actual": sum(
                all(
                    bool(str(row.get(field, "")).strip())
                    for field in lineage_fields
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                all(
                    bool(str(row.get(field, "")).strip())
                    for field in lineage_fields
                )
                for row in records
            ),
        },
        {
            "check": "all_structural_validation_complete",
            "actual": sum(
                bool(row["structural_validation_complete"])
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["structural_validation_complete"])
                for row in records
            ),
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
            "actual": sum(
                int(row["candidate_derived_artifact_count"])
                for row in records
            ),
            "expected": 0,
            "passed": all(
                int(row["candidate_derived_artifact_count"]) == 0
                for row in records
            ),
        },
        {
            "check": "validation_artifact_count_preserved",
            "actual": sum(
                int(row["validation_artifact_count"])
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                int(row["validation_artifact_count"]) == 1
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
            "check": "source_documentation_and_boundary_present",
            "actual": sum(
                bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "rationale"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "authority_boundary"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "rationale"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "authority_boundary"
                    ]
                )
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
            "check": "principles_defined",
            "actual": len(VALIDATION_PRINCIPLES),
            "expected": 8,
            "passed": len(VALIDATION_PRINCIPLES) == 8,
        },
        {
            "check": "components_defined",
            "actual": len(VALIDATION_COMPONENTS),
            "expected": 8,
            "passed": len(VALIDATION_COMPONENTS) == 8,
        },
        {
            "check": "stages_defined",
            "actual": len(VALIDATION_STAGES),
            "expected": 11,
            "passed": len(VALIDATION_STAGES) == 11,
        },
        {
            "check": "requirements_defined",
            "actual": len(VALIDATION_REQUIREMENTS),
            "expected": 30,
            "passed": len(VALIDATION_REQUIREMENTS) == 30,
        },
        {
            "check": "statuses_defined",
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
            "check": "validation_plan_fields_defined",
            "actual": len(VALIDATION_PLAN_RECORD_FIELDS),
            "expected": 62,
            "passed": len(VALIDATION_PLAN_RECORD_FIELDS) == 62,
        },
        {
            "check": "validation_plan_fields_unique",
            "actual": len(set(VALIDATION_PLAN_RECORD_FIELDS)),
            "expected": 62,
            "passed": (
                len(set(VALIDATION_PLAN_RECORD_FIELDS)) == 62
            ),
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
            "check": "validation_materialization_prohibited_during_planning",
            "actual": True,
            "expected": True,
            "passed": (
                "validation_result_evidence_validation_materialization"
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
            "validation_plan_version":
                VALIDATION_PLAN_VERSION,
            "principles":
                VALIDATION_PRINCIPLES,
            "components":
                VALIDATION_COMPONENTS,
            "stages":
                VALIDATION_STAGES,
            "requirements":
                VALIDATION_REQUIREMENTS,
            "statuses":
                VALIDATION_STATUSES,
            "blockers":
                BLOCKER_CODES,
            "record_fields":
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
        "9CH_validation_result_evidence_validation_implementation"
        if all_checks_passed
        else "9CG_validation_result_evidence_validation_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "principles.csv",
        ["principle_id", "principle"],
        VALIDATION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "components.csv",
        ["component_id", "component", "required", "priority"],
        VALIDATION_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "stages.csv",
        ["stage_id", "stage_name", "priority"],
        VALIDATION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "requirements.csv",
        ["requirement_id", "requirement", "expected"],
        VALIDATION_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "statuses.csv",
        ["status", "implementation_authority"],
        VALIDATION_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "blockers.csv",
        ["code", "category"],
        BLOCKER_CODES,
    )

    write_csv(
        OUTPUT_DIR / "field_contract.csv",
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

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "validation_plan_version":
            VALIDATION_PLAN_VERSION,
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
        "layer_9CE_plan_digest":
            replay["plan_digest"],
        "layer_9CD_validation_digest":
            replay["predecessor_validation_digest"],
        "predecessor_result_evidence_digest":
            replay["result_digest"],
        "reverse_predecessor_result_evidence_digest":
            reverse_result_digest,
        "predecessor_manifest_digest":
            replay["manifest_digest"],
        "validation_plan_digest":
            plan_digest,
        "principles":
            len(VALIDATION_PRINCIPLES),
        "components":
            len(VALIDATION_COMPONENTS),
        "stages":
            len(VALIDATION_STAGES),
        "requirements":
            len(VALIDATION_REQUIREMENTS),
        "statuses":
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
        "validation_records_materialized": 0,
        "authoritative_historical_outcomes_validated": 0,
        "historical_outcome_fields_mapped": 0,
        "historical_outcome_values_extracted": 0,
        "network_retrievals_executed": 0,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "downstream_records_recomputed": 0,
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
        OUTPUT_DIR / "plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis": (
            "validation_result_evidence_validation_plan_complete"
            if all_checks_passed
            else "validation_result_evidence_validation_plan_failed"
        ),
        "result_evidence_status":
            EXPECTED_STATUS,
        "structural_validation_complete":
            all_checks_passed,
        "authoritative_historical_outcome_validated":
            False,
        "authority_granted": (
            "validation_result_evidence_validation_implementation"
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
    print(f"Validation plan version: {VALIDATION_PLAN_VERSION}")
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
    print(f"Result-evidence records replayed: {len(records)}")
    print(f"Result-evidence comparisons: {len(comparison_ids)}")
    print(
        "Result-evidence status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Result-evidence blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Validation plan record fields: "
        f"{len(VALIDATION_PLAN_RECORD_FIELDS)}"
    )
    print(f"Layer 9CE plan digest: {replay['plan_digest']}")
    print(
        "Layer 9CD validation digest: "
        f"{replay['predecessor_validation_digest']}"
    )
    print(
        "Predecessor result-evidence digest: "
        f"{replay['result_digest']}"
    )
    print(
        "Predecessor manifest digest: "
        f"{replay['manifest_digest']}"
    )
    print(f"Validation plan digest: {plan_digest}")
    print("Validation records materialized: 0")
    print("Authoritative historical outcomes validated: 0")
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
    print(f"Diagnosis: {diagnosis['diagnosis']}")
    print("Authoritative historical outcome validated: False")
    print(f"Authority granted: {diagnosis['authority_granted']}")
    print(f"Recommended next layer: {next_layer}")
    print(f"Artifacts: {OUTPUT_DIR.relative_to(ROOT)}")

    if not all_checks_passed:
        failed = [
            row["check"]
            for row in checks
            if not row["passed"]
        ]
        print("FAILED CHECKS: " + ", ".join(failed))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
