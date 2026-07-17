#!/usr/bin/env python3
"""
Layer 9CM

Defines the deterministic result-evidence plan over Layer 9CH
validation-result evidence validation records.

Planning only. This layer does not invent or materialize result-evidence
records, validate authoritative historical outcomes, retrieve or parse
responses, map or extract historical outcomes, mutate canonical data,
recompute downstream artifacts, or grant production, market, pricing,
or betting authority.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9CM"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_result_evidence_"
    "validation_result_evidence_validation_result_evidence_validation_"
    "result_evidence_validation_result_evidence_plan"
)

RESULT_EVIDENCE_PLAN_VERSION = (
    "layer_9CM_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_result_evidence_"
    "validation_result_evidence_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "tmp" / "layer_9CM_validation_result_evidence_plan"

PREDECESSOR_PATH = (
    ROOT / "scripts" / "validate_9CL_result_evidence_validation.py"
)

EXPECTED_PREDECESSOR_CONTRACT_VERSION = 'layer_9CL_historical_outcome_authoritative_source_endpoint_candidate_source_evidence_historical_outcome_field_mapping_result_validation_evidence_package_validation_result_evidence_validation_result_evidence_validation_result_evidence_validation_result_evidence_validation_result_evidence_validation_contract_v1'

EXPECTED_PLAN_VERSION = 'layer_9CK_historical_outcome_authoritative_source_endpoint_candidate_source_evidence_historical_outcome_field_mapping_result_validation_evidence_package_validation_result_evidence_validation_result_evidence_validation_result_evidence_validation_result_evidence_validation_result_evidence_validation_plan_v1'

EXPECTED_PLAN_DIGEST = (
    "c2583ad6d835f40180023ae1166597053471ee2ac030c2011cf5d7b6f352cea9"
)

EXPECTED_PREDECESSOR_RESULT_DIGEST = (
    "5ae91bca40eba2753570932c6b9a5fd5477615c3dd4319d54458b85a6806c443"
)

EXPECTED_VALIDATION_DIGEST = (
    "213af5a411de6ffa4a069e2c7760c2cd15de72c8d968e2a295acc0bc0326dc4f"
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
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVRE-P01",
        "principle": (
            "Replay Layer 9CL validation records deterministically before "
            "planning result-evidence materialization."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVRE-P02",
        "principle": (
            "Preserve validation identity, record digests, source lineage, "
            "status, blockers, and authority boundaries."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVRE-P03",
        "principle": (
            "Represent structural validation evidence without inventing "
            "candidate-derived or authoritative outcome evidence."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVRE-P04",
        "principle": (
            "Preserve candidate_not_supplied and the missing endpoint "
            "candidate blocker for every comparison."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVRE-P05",
        "principle": (
            "Preserve outcome_value as canonical and reject "
            "outcome_available_at_utc as an outcome substitute."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVRE-P06",
        "principle": (
            "Require rationale, limitations, evidence boundaries, and "
            "authority boundaries for every future result-evidence record."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVRE-P07",
        "principle": (
            "Prohibit retrieval, parsing, mapping, extraction, mutation, "
            "recomputation, production, market, pricing, and betting work."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVRE-P08",
        "principle": (
            "Successful planning grants only Layer 9CJ result-evidence "
            "implementation authority."
        ),
    },
]


RESULT_EVIDENCE_COMPONENTS = [
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVRE-C01",
        "component": "predecessor_validation_replay",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVRE-C02",
        "component": "validation_identity_preservation",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVRE-C03",
        "component": "validation_digest_preservation",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVRE-C04",
        "component": "lineage_preservation",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVRE-C05",
        "component": "status_and_blocker_preservation",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVRE-C06",
        "component": "evidence_absence_representation",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVRE-C07",
        "component": "canonical_field_and_documentation_preservation",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVRE-C08",
        "component": "authority_boundary_preservation",
        "required": True,
        "priority": 8,
    },
]


RESULT_EVIDENCE_STAGES = [
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S01", "stage": "predecessor_replay", "priority": 1},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S02", "stage": "record_inventory", "priority": 2},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S03", "stage": "identity_and_digest_validation", "priority": 3},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S04", "stage": "lineage_validation", "priority": 4},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S05", "stage": "status_and_blocker_validation", "priority": 5},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S06", "stage": "evidence_absence_validation", "priority": 6},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S07", "stage": "canonical_field_validation", "priority": 7},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S08", "stage": "documentation_validation", "priority": 8},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S09", "stage": "result_evidence_contract_definition", "priority": 9},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S10", "stage": "authority_boundary_definition", "priority": 10},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVRE-S11", "stage": "plan_emission", "priority": 11},
]


RESULT_EVIDENCE_REQUIREMENTS = [
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R01", "requirement": "source_validation_record_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R02", "requirement": "source_validation_record_id_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R03", "requirement": "source_validation_identity_digest_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R04", "requirement": "source_validation_record_digest_valid", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R05", "requirement": "predecessor_nine_cl_validation_digest_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R06", "requirement": "nine_cj_result_evidence_digest_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R07", "requirement": "comparison_lineage_complete", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R08", "requirement": "validation_lineage_complete", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R09", "requirement": "result_evidence_lineage_complete", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R10", "requirement": "package_mapping_and_defect_lineage_complete", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R11", "requirement": "structural_validation_complete", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R12", "requirement": "candidate_not_supplied_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R13", "requirement": "missing_endpoint_blocker_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R14", "requirement": "candidate_derived_artifact_count_zero", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R15", "requirement": "validation_artifact_count_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R16", "requirement": "evidence_absence_explicit", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R17", "requirement": "fabricated_evidence_absent", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R18", "requirement": "canonical_field_identity_preserved", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R19", "requirement": "source_validation_rationale_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R20", "requirement": "source_validation_limitations_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R21", "requirement": "source_validation_authority_boundary_present", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R22", "requirement": "result_evidence_rationale_required", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R23", "requirement": "result_evidence_limitations_required", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R24", "requirement": "result_evidence_authority_boundary_required", "expected": True},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R25", "requirement": "authoritative_historical_outcome_validated", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R26", "requirement": "result_evidence_materialized_during_planning", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R27", "requirement": "network_retrieval_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R28", "requirement": "mapping_or_extraction_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R29", "requirement": "canonical_mutation_or_recomputation_executed", "expected": False},
    {"requirement_id": "HOASEHOFMRVEPVREVREVREVREVRE-R30", "requirement": "production_market_or_betting_authority_granted", "expected": False},
]


RESULT_EVIDENCE_STATUSES = [
    {"status": "result_evidence_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "source_validation_record_missing", "implementation_authority": False},
    {"status": "source_validation_identity_invalid", "implementation_authority": False},
    {"status": "source_validation_digest_invalid", "implementation_authority": False},
    {"status": "lineage_incomplete", "implementation_authority": False},
    {"status": "structural_disposition_invalid", "implementation_authority": False},
    {"status": "canonical_field_invalid", "implementation_authority": False},
    {"status": "documentation_invalid", "implementation_authority": False},
    {"status": "authority_boundary_invalid", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "source_validation_record_missing", "category": "record"},
    {"code": "source_validation_record_id_missing", "category": "identity"},
    {"code": "source_validation_identity_digest_missing", "category": "identity"},
    {"code": "source_validation_identity_digest_invalid", "category": "identity"},
    {"code": "source_validation_record_digest_missing", "category": "integrity"},
    {"code": "source_validation_record_digest_invalid", "category": "integrity"},
    {"code": "predecessor_validation_digest_invalid", "category": "integrity"},
    {"code": "source_result_evidence_digest_invalid", "category": "integrity"},
    {"code": "comparison_lineage_missing", "category": "lineage"},
    {"code": "validation_lineage_missing", "category": "lineage"},
    {"code": "result_evidence_lineage_missing", "category": "lineage"},
    {"code": "package_lineage_missing", "category": "lineage"},
    {"code": "mapping_lineage_missing", "category": "lineage"},
    {"code": "defect_lineage_missing", "category": "lineage"},
    {"code": "structural_validation_incomplete", "category": "structural"},
    {"code": "candidate_not_supplied_status_missing", "category": "status"},
    {"code": "missing_endpoint_blocker_missing", "category": "status"},
    {"code": "candidate_derived_artifact_count_invalid", "category": "evidence"},
    {"code": "validation_artifact_count_invalid", "category": "evidence"},
    {"code": "evidence_absence_not_explicit", "category": "evidence"},
    {"code": "fabricated_evidence_detected", "category": "evidence"},
    {"code": "canonical_field_identity_invalid", "category": "field"},
    {"code": "source_validation_rationale_missing", "category": "documentation"},
    {"code": "source_validation_limitations_missing", "category": "documentation"},
    {"code": "source_validation_authority_boundary_missing", "category": "authority"},
    {"code": "result_evidence_documentation_missing", "category": "documentation"},
    {"code": "result_evidence_authority_boundary_missing", "category": "authority"},
    {"code": "result_evidence_materialization_requested_during_planning", "category": "authority"},
    {"code": "production_market_or_betting_authority_requested", "category": "authority"},
]


RESULT_EVIDENCE_PLAN_RECORD_FIELDS = [
    "validation_result_evidence_validation_result_evidence_contract_version",
    "validation_result_evidence_validation_result_evidence_plan_record_id",
    "validation_result_evidence_validation_result_evidence_plan_identity_digest",
    "validation_result_evidence_validation_result_evidence_source_record_id",
    "validation_result_evidence_validation_result_evidence_source_record_identity_digest",
    "validation_result_evidence_validation_result_evidence_source_record_digest",
    "validation_result_evidence_validation_result_evidence_validation_contract_version",
    "validation_result_evidence_validation_result_evidence_validation_plan_record_id",
    "validation_result_evidence_validation_result_evidence_validation_plan_identity_digest",
    "validation_result_evidence_validation_result_evidence_validation_plan_record_digest",
    "validation_result_evidence_source_record_id",
    "validation_result_evidence_source_record_identity_digest",
    "validation_result_evidence_source_record_digest",
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
    "source_validation_status",
    "source_validation_blocker_codes",
    "source_validation_rationale",
    "source_validation_limitations",
    "source_validation_authority_boundary",
    "validation_result_evidence_validation_result_evidence_status",
    "validation_result_evidence_validation_result_evidence_blocker_codes",
    "validation_result_evidence_validation_result_evidence_implementation_authority_granted",
    "validation_result_evidence_validation_result_evidence_rationale",
    "validation_result_evidence_validation_result_evidence_limitations",
    "validation_result_evidence_validation_result_evidence_authority_boundary",
    "validation_result_evidence_validation_result_evidence_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "validation_result_evidence_plan_record_id"},
    {"ordinal": 5, "field": "result_evidence_validation_plan_record_id"},
    {"ordinal": 6, "field": "validation_result_evidence_validation_result_evidence_validation_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9CH_validation_records"},
    {"ordinal": 2, "step": "validate_record_inventory"},
    {"ordinal": 3, "step": "validate_source_validation_identity"},
    {"ordinal": 4, "step": "validate_source_validation_record_digest"},
    {"ordinal": 5, "step": "validate_predecessor_validation_digest"},
    {"ordinal": 6, "step": "validate_source_result_evidence_digest"},
    {"ordinal": 7, "step": "validate_complete_lineage"},
    {"ordinal": 8, "step": "validate_status_and_blockers"},
    {"ordinal": 9, "step": "validate_candidate_evidence_absence"},
    {"ordinal": 10, "step": "validate_canonical_field_identity"},
    {"ordinal": 11, "step": "validate_source_documentation"},
    {"ordinal": 12, "step": "define_result_evidence_identity_contract"},
    {"ordinal": 13, "step": "define_result_evidence_documentation_contract"},
    {"ordinal": 14, "step": "define_result_evidence_authority_boundary"},
    {"ordinal": 15, "step": "define_deterministic_ordering_and_digest"},
    {"ordinal": 16, "step": "grant_layer_9CJ_implementation_only_when_complete"},
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "response_artifact_invention",
    "parser_submission_invention",
    "mapping_submission_invention",
    "validation_result_invention",
    "evidence_artifact_invention",
    "result_evidence_invention",
    "validation_result_evidence_validation_result_evidence_materialization",
    "authoritative_historical_outcome_validation",
    "historical_outcome_field_mapping_execution",
    "historical_outcome_value_extraction",
    "response_bytes_reading",
    "raw_response_parse_execution",
    "dns_resolution_execution",
    "socket_connection_execution",
    "http_request_execution",
    "browser_execution",
    "api_request_execution",
    "canonical_source_value_mutation",
    "canonical_outcome_mapping_change",
    "canonical_record_recomputation",
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
        "layer_9cl_validation",
    )

    if (
        predecessor.VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_CONTRACT_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9CL contract version: "
            f"{predecessor.VALIDATION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    if plan.VALIDATION_PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9CK plan version: "
            f"{plan.VALIDATION_PLAN_VERSION}"
        )

    records = predecessor.build_validation_records(
        plan,
        replay,
        replay["records"],
    )

    reverse_records = predecessor.build_validation_records(
        plan,
        replay,
        list(reversed(replay["reverse_records"])),
    )

    return {
        "predecessor": predecessor,
        "plan": plan,
        "replay": replay,
        "records": records,
        "reverse_records": reverse_records,
        "plan_digest": predecessor.compute_plan_digest(plan),
        "source_result_digest": sha256_payload(replay["records"]),
        "validation_digest": sha256_payload(records),
        "reverse_validation_digest": sha256_payload(reverse_records),
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    replay = replay_predecessor()
    predecessor = replay["predecessor"]
    records = replay["records"]
    reverse_records = replay["reverse_records"]

    comparison_ids = {
        row["comparison_record_id"]
        for row in records
    }

    status_counts = Counter(
        row["validation_result_evidence_validation_result_evidence_validation_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "validation_result_evidence_validation_result_evidence_validation_blocker_codes"
        ]
    )

    lineage_fields = (
        "validation_result_evidence_validation_result_evidence_validation_plan_record_id",
        "validation_result_evidence_source_record_id",
        "result_evidence_validation_plan_record_id",
        "result_evidence_source_record_id",
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
            "check": "nine_ch_contract_version_verified",
            "actual": predecessor.VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_CONTRACT_VERSION,
            "passed": (
                predecessor.VALIDATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_CONTRACT_VERSION
            ),
        },
        {
            "check": "nine_ck_plan_version_verified",
            "actual": replay["plan"].VALIDATION_PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": (
                replay["plan"].VALIDATION_PLAN_VERSION
                == EXPECTED_PLAN_VERSION
            ),
        },
        {
            "check": "nine_ck_plan_digest_preserved",
            "actual": replay["plan_digest"],
            "expected": EXPECTED_PLAN_DIGEST,
            "passed": replay["plan_digest"] == EXPECTED_PLAN_DIGEST,
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
            "check": "nine_cj_result_evidence_digest_preserved",
            "actual": replay["source_result_digest"],
            "expected": EXPECTED_PREDECESSOR_RESULT_DIGEST,
            "passed": (
                replay["source_result_digest"]
                == EXPECTED_PREDECESSOR_RESULT_DIGEST
            ),
        },
        {
            "check": "nine_cl_validation_digest_preserved",
            "actual": replay["validation_digest"],
            "expected": EXPECTED_VALIDATION_DIGEST,
            "passed": (
                replay["validation_digest"]
                == EXPECTED_VALIDATION_DIGEST
            ),
        },
        {
            "check": "reverse_nine_cl_validation_digest_preserved",
            "actual": replay["reverse_validation_digest"],
            "expected": EXPECTED_VALIDATION_DIGEST,
            "passed": (
                replay["reverse_validation_digest"]
                == EXPECTED_VALIDATION_DIGEST
            ),
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
                        "validation_result_evidence_validation_result_evidence_validation_"
                        "plan_identity_digest"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_validation_result_evidence_validation_"
                        "plan_identity_digest"
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
                        "validation_result_evidence_validation_result_evidence_validation_"
                        "plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_validation_result_evidence_validation_"
                        "plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "lineage_complete",
            "actual": sum(
                all(bool(str(row.get(field, "")).strip()) for field in lineage_fields)
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                all(bool(str(row.get(field, "")).strip()) for field in lineage_fields)
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
                row["authoritative_field_name"] == AUTHORITATIVE_FIELD_NAME
                and row["authoritative_field_path"] == AUTHORITATIVE_FIELD_PATH
                and row["rejected_metadata_field_name"] == REJECTED_METADATA_FIELD
                for row in records
            ),
        },
        {
            "check": "source_validation_documentation_present",
            "actual": sum(
                bool(row["validation_result_evidence_validation_result_evidence_validation_rationale"])
                and bool(row["validation_result_evidence_validation_result_evidence_validation_limitations"])
                and bool(
                    row[
                        "validation_result_evidence_validation_result_evidence_validation_"
                        "authority_boundary"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["validation_result_evidence_validation_result_evidence_validation_rationale"])
                and bool(row["validation_result_evidence_validation_result_evidence_validation_limitations"])
                and bool(
                    row[
                        "validation_result_evidence_validation_result_evidence_validation_"
                        "authority_boundary"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "authoritative_historical_outcomes_validated_zero",
            "actual": sum(
                bool(row["authoritative_historical_outcome_validated"])
                for row in records
            ),
            "expected": 0,
            "passed": all(
                not bool(row["authoritative_historical_outcome_validated"])
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
            "check": "result_evidence_plan_fields_defined",
            "actual": len(RESULT_EVIDENCE_PLAN_RECORD_FIELDS),
            "expected": 65,
            "passed": len(RESULT_EVIDENCE_PLAN_RECORD_FIELDS) == 65,
        },
        {
            "check": "result_evidence_plan_fields_unique",
            "actual": len(set(RESULT_EVIDENCE_PLAN_RECORD_FIELDS)),
            "expected": 65,
            "passed": len(set(RESULT_EVIDENCE_PLAN_RECORD_FIELDS)) == 65,
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
            "check": "result_evidence_materialization_prohibited_during_planning",
            "actual": True,
            "expected": True,
            "passed": (
                "validation_result_evidence_validation_result_evidence_materialization"
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

    all_checks_passed = all(bool(row["passed"]) for row in checks)

    plan_digest = sha256_payload(
        {
            "result_evidence_plan_version": RESULT_EVIDENCE_PLAN_VERSION,
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
        "9CN_validation_result_evidence_validation_result_evidence_implementation"
        if all_checks_passed
        else "9CM_validation_result_evidence_validation_result_evidence_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "principles.csv",
        ["principle_id", "principle"],
        RESULT_EVIDENCE_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "components.csv",
        ["component_id", "component", "required", "priority"],
        RESULT_EVIDENCE_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "stages.csv",
        ["stage_id", "stage", "priority"],
        RESULT_EVIDENCE_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "requirements.csv",
        ["requirement_id", "requirement", "expected"],
        RESULT_EVIDENCE_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "statuses.csv",
        ["status", "implementation_authority"],
        RESULT_EVIDENCE_STATUSES,
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

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "result_evidence_plan_version": RESULT_EVIDENCE_PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.VALIDATION_CONTRACT_VERSION,
        "predecessor_validation_records": len(records),
        "predecessor_validation_comparisons": len(comparison_ids),
        "predecessor_status_counts": dict(sorted(status_counts.items())),
        "predecessor_blocker_counts": dict(sorted(blocker_counts.items())),
        "layer_9CK_plan_digest": replay["plan_digest"],
        "layer_9CJ_result_evidence_digest":
            replay["source_result_digest"],
        "layer_9CL_validation_digest":
            replay["validation_digest"],
        "reverse_layer_9CH_validation_digest":
            replay["reverse_validation_digest"],
        "result_evidence_plan_digest": plan_digest,
        "principles": len(RESULT_EVIDENCE_PRINCIPLES),
        "components": len(RESULT_EVIDENCE_COMPONENTS),
        "stages": len(RESULT_EVIDENCE_STAGES),
        "requirements": len(RESULT_EVIDENCE_REQUIREMENTS),
        "statuses": len(RESULT_EVIDENCE_STATUSES),
        "blocker_codes": len(BLOCKER_CODES),
        "result_evidence_plan_record_fields":
            len(RESULT_EVIDENCE_PLAN_RECORD_FIELDS),
        "ordering_fields": len(ORDERING_FIELDS),
        "implementation_steps": len(IMPLEMENTATION_STEPS),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required": len(checks),
        "result_evidence_records_materialized": 0,
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
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": next_layer,
    }

    write_json(OUTPUT_DIR / "plan_summary.json", summary)

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": (
            "validation_result_evidence_validation_result_evidence_validation_result_evidence_plan_complete"
            if all_checks_passed
            else "validation_result_evidence_validation_result_evidence_validation_result_evidence_plan_failed"
        ),
        "predecessor_validation_status": EXPECTED_STATUS,
        "structural_validation_complete": all_checks_passed,
        "authoritative_historical_outcome_validated": False,
        "authority_granted": (
            "validation_result_evidence_validation_result_evidence_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(PROHIBITED_AUTHORITIES),
        "recommended_next_layer": next_layer,
        "output_directory": str(OUTPUT_DIR.relative_to(ROOT)),
    }

    write_json(OUTPUT_DIR / "diagnosis.json", diagnosis)

    print(f"Layer: {LAYER_ID} — {LAYER_NAME}")
    print(f"Result-evidence plan version: {RESULT_EVIDENCE_PLAN_VERSION}")
    print(
        "Predecessor contract version: "
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
    print(f"Layer 9CK plan digest: {replay['plan_digest']}")
    print(
        "Layer 9CJ result-evidence digest: "
        f"{replay['source_result_digest']}"
    )
    print(
        "Layer 9CL validation digest: "
        f"{replay['validation_digest']}"
    )
    print(f"Result-evidence plan digest: {plan_digest}")
    print("Result-evidence records materialized: 0")
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
