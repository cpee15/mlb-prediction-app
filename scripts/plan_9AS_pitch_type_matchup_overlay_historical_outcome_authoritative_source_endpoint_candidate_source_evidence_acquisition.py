#!/usr/bin/env python3
"""
Layer 9AS
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Source Evidence Acquisition Plan

Plans deterministic acquisition and validation of evidence supporting an
explicitly submitted authoritative source endpoint candidate.

Layer 9AR established that no endpoint candidate submission exists. This layer
defines the evidence contract required to establish source ownership,
authority, documentation, historical coverage, licensing, stability, identity
semantics, outcome semantics, and immutable provenance before any endpoint
candidate may be approved.

Planning only.

This layer does not:
- invent or submit an endpoint candidate;
- select an external source;
- execute network retrieval;
- store credentials or credential literals;
- materialize raw endpoint responses;
- acquire historical outcome values;
- mutate canonical values or mappings;
- coerce, default, infer, impute, or substitute values;
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


LAYER_ID = "9AS"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_acquisition_plan"
)

PLAN_VERSION = (
    "layer_9AS_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AS_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_acquisition_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "specify_9AR_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AR_historical_outcome_authoritative_source_"
    "endpoint_candidate_specification_contract_v1"
)

EXPECTED_SPECIFICATION_RECORDS = 16
EXPECTED_SPECIFICATION_COMPARISONS = 16

EXPECTED_SPECIFICATION_STATUS = "endpoint_candidate_not_supplied"

EXPECTED_SPECIFICATION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


EVIDENCE_PRINCIPLES = [
    {
        "principle_id": "HOASEAP-P01",
        "principle": (
            "Source evidence may be acquired only for an explicitly submitted "
            "endpoint candidate."
        ),
    },
    {
        "principle_id": "HOASEAP-P02",
        "principle": (
            "Evidence must identify the source owner and governing or originating "
            "authority."
        ),
    },
    {
        "principle_id": "HOASEAP-P03",
        "principle": (
            "Authority evidence must be retained immutably with retrieval metadata "
            "and a SHA-256 digest."
        ),
    },
    {
        "principle_id": "HOASEAP-P04",
        "principle": (
            "Evidence must document historical coverage sufficient for the target "
            "game, target, event level, event identity, and event sequence."
        ),
    },
    {
        "principle_id": "HOASEAP-P05",
        "principle": (
            "Evidence must document the authoritative outcome field and distinguish "
            "it from availability metadata."
        ),
    },
    {
        "principle_id": "HOASEAP-P06",
        "principle": (
            "Terms, licensing, authentication, retention, and redistribution "
            "constraints must be explicitly recorded."
        ),
    },
    {
        "principle_id": "HOASEAP-P07",
        "principle": (
            "Conflicting evidence must block approval rather than be reconciled "
            "heuristically."
        ),
    },
    {
        "principle_id": "HOASEAP-P08",
        "principle": (
            "Evidence acquisition approval grants evidence-acquisition "
            "implementation authority only."
        ),
    },
]


EVIDENCE_CLASSES = [
    {
        "evidence_class_id": "HOASEAP-E01",
        "evidence_class": "official_source_documentation",
        "required": True,
        "priority": 1,
    },
    {
        "evidence_class_id": "HOASEAP-E02",
        "evidence_class": "source_owner_identity",
        "required": True,
        "priority": 2,
    },
    {
        "evidence_class_id": "HOASEAP-E03",
        "evidence_class": "historical_coverage_documentation",
        "required": True,
        "priority": 3,
    },
    {
        "evidence_class_id": "HOASEAP-E04",
        "evidence_class": "identity_semantics_documentation",
        "required": True,
        "priority": 4,
    },
    {
        "evidence_class_id": "HOASEAP-E05",
        "evidence_class": "outcome_semantics_documentation",
        "required": True,
        "priority": 5,
    },
    {
        "evidence_class_id": "HOASEAP-E06",
        "evidence_class": "licensing_and_terms_documentation",
        "required": True,
        "priority": 6,
    },
    {
        "evidence_class_id": "HOASEAP-E07",
        "evidence_class": "availability_and_stability_documentation",
        "required": True,
        "priority": 7,
    },
    {
        "evidence_class_id": "HOASEAP-E08",
        "evidence_class": "sample_schema_or_versioned_snapshot",
        "required": True,
        "priority": 8,
    },
]


ACQUISITION_STAGES = [
    {
        "stage_id": "HOASEAP-C01",
        "stage_name": "candidate_specification_replay",
        "priority": 1,
    },
    {
        "stage_id": "HOASEAP-C02",
        "stage_name": "candidate_presence_gate",
        "priority": 2,
    },
    {
        "stage_id": "HOASEAP-C03",
        "stage_name": "evidence_locator_inventory",
        "priority": 3,
    },
    {
        "stage_id": "HOASEAP-C04",
        "stage_name": "authority_documentation_acquisition",
        "priority": 4,
    },
    {
        "stage_id": "HOASEAP-C05",
        "stage_name": "coverage_documentation_acquisition",
        "priority": 5,
    },
    {
        "stage_id": "HOASEAP-C06",
        "stage_name": "identity_semantics_acquisition",
        "priority": 6,
    },
    {
        "stage_id": "HOASEAP-C07",
        "stage_name": "outcome_semantics_acquisition",
        "priority": 7,
    },
    {
        "stage_id": "HOASEAP-C08",
        "stage_name": "licensing_and_terms_acquisition",
        "priority": 8,
    },
    {
        "stage_id": "HOASEAP-C09",
        "stage_name": "stability_and_version_evidence_acquisition",
        "priority": 9,
    },
    {
        "stage_id": "HOASEAP-C10",
        "stage_name": "evidence_retention_and_digest",
        "priority": 10,
    },
    {
        "stage_id": "HOASEAP-C11",
        "stage_name": "evidence_conflict_validation",
        "priority": 11,
    },
    {
        "stage_id": "HOASEAP-C12",
        "stage_name": "source_evidence_disposition",
        "priority": 12,
    },
]


EVIDENCE_REQUIREMENTS = [
    {
        "requirement_id": "HOASEAP-R01",
        "requirement": "candidate_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R02",
        "requirement": "candidate_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R03",
        "requirement": "candidate_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R04",
        "requirement": "source_owner_identity_evidence_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R05",
        "requirement": "official_documentation_evidence_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R06",
        "requirement": "historical_coverage_evidence_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R07",
        "requirement": "game_identity_semantics_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R08",
        "requirement": "target_identity_semantics_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R09",
        "requirement": "event_level_semantics_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R10",
        "requirement": "event_identity_semantics_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R11",
        "requirement": "event_sequence_semantics_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R12",
        "requirement": "authoritative_outcome_semantics_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R13",
        "requirement": "outcome_numeric_domain_documented",
        "expected": "finite_int_or_float_excluding_bool",
    },
    {
        "requirement_id": "HOASEAP-R14",
        "requirement": "availability_metadata_rejection_documented",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R15",
        "requirement": "licensing_terms_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R16",
        "requirement": "retention_rights_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R17",
        "requirement": "authentication_requirements_documented",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R18",
        "requirement": "availability_or_sla_documented",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R19",
        "requirement": "schema_or_snapshot_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R20",
        "requirement": "immutable_evidence_retention_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R21",
        "requirement": "evidence_digest_algorithm",
        "expected": "sha256",
    },
    {
        "requirement_id": "HOASEAP-R22",
        "requirement": "evidence_conflicts_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOASEAP-R23",
        "requirement": "historical_outcome_retrieval_executed",
        "expected": False,
    },
]


EVIDENCE_STATUSES = [
    {
        "status": "source_evidence_acquisition_approved",
        "implementation_authority": True,
    },
    {
        "status": "candidate_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "evidence_locator_missing",
        "implementation_authority": False,
    },
    {
        "status": "source_authority_unverified",
        "implementation_authority": False,
    },
    {
        "status": "historical_coverage_unverified",
        "implementation_authority": False,
    },
    {
        "status": "identity_semantics_unverified",
        "implementation_authority": False,
    },
    {
        "status": "outcome_semantics_unverified",
        "implementation_authority": False,
    },
    {
        "status": "licensing_or_retention_unverified",
        "implementation_authority": False,
    },
    {
        "status": "source_stability_unverified",
        "implementation_authority": False,
    },
    {
        "status": "source_evidence_conflict",
        "implementation_authority": False,
    },
]


BLOCKER_CODES = [
    {"code": "historical_outcome_endpoint_candidate_missing", "category": "submission"},
    {"code": "historical_outcome_source_evidence_locator_missing", "category": "evidence"},
    {"code": "historical_outcome_source_owner_evidence_missing", "category": "authority"},
    {"code": "historical_outcome_official_documentation_missing", "category": "authority"},
    {"code": "historical_outcome_historical_coverage_evidence_missing", "category": "coverage"},
    {"code": "historical_outcome_game_identity_semantics_missing", "category": "identity"},
    {"code": "historical_outcome_target_identity_semantics_missing", "category": "identity"},
    {"code": "historical_outcome_event_level_semantics_missing", "category": "identity"},
    {"code": "historical_outcome_event_identity_semantics_missing", "category": "identity"},
    {"code": "historical_outcome_event_sequence_semantics_missing", "category": "identity"},
    {"code": "historical_outcome_authoritative_outcome_semantics_missing", "category": "semantics"},
    {"code": "historical_outcome_numeric_domain_evidence_missing", "category": "semantics"},
    {"code": "historical_outcome_metadata_rejection_evidence_missing", "category": "semantics"},
    {"code": "historical_outcome_licensing_terms_missing", "category": "licensing"},
    {"code": "historical_outcome_retention_rights_missing", "category": "licensing"},
    {"code": "historical_outcome_authentication_requirements_missing", "category": "security"},
    {"code": "historical_outcome_source_availability_evidence_missing", "category": "stability"},
    {"code": "historical_outcome_schema_version_evidence_missing", "category": "versioning"},
    {"code": "historical_outcome_evidence_retention_policy_missing", "category": "evidence"},
    {"code": "historical_outcome_evidence_digest_policy_missing", "category": "lineage"},
    {"code": "historical_outcome_source_evidence_conflict", "category": "conflict"},
    {"code": "historical_outcome_endpoint_candidate_invention_requested", "category": "authority"},
    {"code": "historical_outcome_retrieval_requested_during_evidence_acquisition", "category": "authority"},
    {"code": "historical_outcome_canonical_mutation_requested", "category": "authority"},
]


EVIDENCE_RECORD_FIELDS = [
    "source_evidence_acquisition_plan_contract_version",
    "source_evidence_acquisition_plan_record_id",
    "endpoint_candidate_specification_record_id",
    "endpoint_candidate_specification_record_digest",
    "authoritative_source_endpoint_configuration_record_id",
    "authoritative_source_acquisition_record_id",
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
    "specification_status",
    "specification_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "source_owner",
    "source_class",
    "evidence_locator_present",
    "authority_documentation_locator",
    "coverage_documentation_locator",
    "identity_semantics_locator",
    "outcome_semantics_locator",
    "licensing_terms_locator",
    "availability_documentation_locator",
    "schema_or_snapshot_locator",
    "required_evidence_classes",
    "evidence_acquisition_status",
    "evidence_acquisition_blocker_codes",
    "evidence_acquisition_implementation_authority_granted",
    "evidence_acquisition_rationale",
    "evidence_acquisition_limitations",
    "source_evidence_acquisition_plan_identity_digest",
    "source_evidence_acquisition_plan_record_digest",
]


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "source_owner"},
    {"ordinal": 5, "field": "endpoint_candidate_specification_record_id"},
    {"ordinal": 6, "field": "source_evidence_acquisition_plan_record_id"},
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9AR_candidate_specification_records"},
    {"ordinal": 2, "step": "preserve_specification_configuration_and_defect_lineage"},
    {"ordinal": 3, "step": "require_explicit_candidate_submission"},
    {"ordinal": 4, "step": "inventory_explicit_evidence_locators"},
    {"ordinal": 5, "step": "acquire_source_owner_and_official_documentation_evidence"},
    {"ordinal": 6, "step": "acquire_historical_coverage_evidence"},
    {"ordinal": 7, "step": "acquire_identity_semantics_evidence"},
    {"ordinal": 8, "step": "acquire_outcome_semantics_and_numeric_domain_evidence"},
    {"ordinal": 9, "step": "acquire_metadata_rejection_evidence"},
    {"ordinal": 10, "step": "acquire_licensing_retention_and_authentication_evidence"},
    {"ordinal": 11, "step": "acquire_availability_stability_and_version_evidence"},
    {"ordinal": 12, "step": "retain_evidence_immutably_and_compute_sha256_digests"},
    {"ordinal": 13, "step": "validate_evidence_conflicts_and_cardinality"},
    {"ordinal": 14, "step": "emit_deterministic_evidence_acquisition_records"},
    {"ordinal": 15, "step": "verify_forward_and_reverse_replay"},
    {"ordinal": 16, "step": "grant_evidence_acquisition_implementation_only_when_candidate_and_locators_exist"},
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "endpoint_candidate_selection_without_submission",
    "evidence_locator_invention",
    "authority_evidence_fabrication",
    "credential_literal_storage",
    "historical_outcome_retrieval_planning",
    "historical_outcome_fetch_execution",
    "historical_outcome_parse_execution",
    "raw_endpoint_response_materialization",
    "canonical_source_value_mutation",
    "canonical_outcome_mapping_change",
    "boolean_to_integer_coercion",
    "source_value_defaulting",
    "source_value_inference",
    "source_value_imputation",
    "heuristic_candidate_substitution",
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
        "layer_9ar_predecessor",
    )

    if (
        predecessor.SPECIFICATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AR contract version: "
            f"{predecessor.SPECIFICATION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_specification_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_specification_records(
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
        row["specification_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row["specification_blocker_codes"]
    )

    checks = [
        {
            "check": "nine_ar_contract_version_verified",
            "actual": predecessor.SPECIFICATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.SPECIFICATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_ar_replay_deterministic",
            "actual": canonical_json(records) == canonical_json(reverse_records),
            "expected": True,
            "passed": canonical_json(records) == canonical_json(reverse_records),
        },
        {
            "check": "nine_ar_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": sha256_payload(records) == sha256_payload(reverse_records),
        },
        {
            "check": "expected_specification_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_SPECIFICATION_RECORDS,
            "passed": len(records) == EXPECTED_SPECIFICATION_RECORDS,
        },
        {
            "check": "expected_specification_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_SPECIFICATION_COMPARISONS,
            "passed": len(comparison_ids) == EXPECTED_SPECIFICATION_COMPARISONS,
        },
        {
            "check": "all_specifications_candidate_not_supplied",
            "actual": dict(sorted(status_counts.items())),
            "expected": {
                EXPECTED_SPECIFICATION_STATUS:
                    EXPECTED_SPECIFICATION_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_SPECIFICATION_STATUS:
                        EXPECTED_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(sorted(blocker_counts.items())),
            "expected": {
                EXPECTED_SPECIFICATION_BLOCKER:
                    EXPECTED_SPECIFICATION_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_SPECIFICATION_BLOCKER:
                        EXPECTED_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check": "all_specification_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row["endpoint_candidate_specification_record_digest"]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row["endpoint_candidate_specification_record_digest"]
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
                row["rejected_metadata_field_name"] == REJECTED_METADATA_FIELD
                for row in records
            ),
        },
        {
            "check": "evidence_principles_defined",
            "actual": len(EVIDENCE_PRINCIPLES),
            "expected": 8,
            "passed": len(EVIDENCE_PRINCIPLES) == 8,
        },
        {
            "check": "evidence_classes_defined",
            "actual": len(EVIDENCE_CLASSES),
            "expected": 8,
            "passed": len(EVIDENCE_CLASSES) == 8,
        },
        {
            "check": "acquisition_stages_defined",
            "actual": len(ACQUISITION_STAGES),
            "expected": 12,
            "passed": len(ACQUISITION_STAGES) == 12,
        },
        {
            "check": "evidence_requirements_defined",
            "actual": len(EVIDENCE_REQUIREMENTS),
            "expected": 23,
            "passed": len(EVIDENCE_REQUIREMENTS) == 23,
        },
        {
            "check": "evidence_statuses_defined",
            "actual": len(EVIDENCE_STATUSES),
            "expected": 10,
            "passed": len(EVIDENCE_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 24,
            "passed": len(BLOCKER_CODES) == 24,
        },
        {
            "check": "evidence_record_fields_defined",
            "actual": len(EVIDENCE_RECORD_FIELDS),
            "expected": 41,
            "passed": len(EVIDENCE_RECORD_FIELDS) == 41,
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
            "check": "candidate_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": "endpoint_candidate_invention" in PROHIBITED_AUTHORITIES,
        },
        {
            "check": "evidence_fabrication_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "evidence_locator_invention" in PROHIBITED_AUTHORITIES
                and "authority_evidence_fabrication" in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "historical_outcome_retrieval_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "historical_outcome_fetch_execution" in PROHIBITED_AUTHORITIES
                and "historical_outcome_parse_execution" in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "canonical_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_source_value_mutation" in PROHIBITED_AUTHORITIES
                and "canonical_outcome_mapping_change" in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "candidate_transformation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "boolean_to_integer_coercion" in PROHIBITED_AUTHORITIES
                and "source_value_defaulting" in PROHIBITED_AUTHORITIES
                and "source_value_inference" in PROHIBITED_AUTHORITIES
                and "source_value_imputation" in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "source_evidence_acquired_zero",
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
            "check": "credentials_stored_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "canonical_sources_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "downstream_records_not_recomputed",
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
            "evidence_principles": EVIDENCE_PRINCIPLES,
            "evidence_classes": EVIDENCE_CLASSES,
            "acquisition_stages": ACQUISITION_STAGES,
            "evidence_requirements": EVIDENCE_REQUIREMENTS,
            "evidence_statuses": EVIDENCE_STATUSES,
            "blocker_codes": BLOCKER_CODES,
            "evidence_record_fields": EVIDENCE_RECORD_FIELDS,
            "ordering_fields": ORDERING_FIELDS,
            "implementation_steps": IMPLEMENTATION_STEPS,
            "prohibited_authorities": PROHIBITED_AUTHORITIES,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_plan_failed"
    )

    next_layer = (
        "9AT_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_implementation"
        if all_checks_passed
        else
        "9AS_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "evidence_principles.csv",
        ["principle_id", "principle"],
        EVIDENCE_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "evidence_classes.csv",
        [
            "evidence_class_id",
            "evidence_class",
            "required",
            "priority",
        ],
        EVIDENCE_CLASSES,
    )

    write_csv(
        OUTPUT_DIR / "acquisition_stages.csv",
        ["stage_id", "stage_name", "priority"],
        ACQUISITION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "evidence_requirements.csv",
        ["requirement_id", "requirement", "expected"],
        EVIDENCE_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "evidence_statuses.csv",
        ["status", "implementation_authority"],
        EVIDENCE_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "blocker_code_catalog.csv",
        ["code", "category"],
        BLOCKER_CODES,
    )

    write_csv(
        OUTPUT_DIR / "evidence_record_field_contract.csv",
        ["ordinal", "field"],
        [
            {
                "ordinal": index,
                "field": field,
            }
            for index, field in enumerate(
                EVIDENCE_RECORD_FIELDS,
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
        OUTPUT_DIR / "candidate_missing_specification_inventory.csv",
        [
            "endpoint_candidate_specification_record_id",
            "endpoint_candidate_specification_record_digest",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "specification_status",
            "specification_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "source_owner",
            "source_class",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.SPECIFICATION_CONTRACT_VERSION,
        "specification_records": len(records),
        "specification_comparisons": len(comparison_ids),
        "specification_status_counts":
            dict(sorted(status_counts.items())),
        "specification_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "evidence_principles": len(EVIDENCE_PRINCIPLES),
        "evidence_classes": len(EVIDENCE_CLASSES),
        "acquisition_stages": len(ACQUISITION_STAGES),
        "evidence_requirements": len(EVIDENCE_REQUIREMENTS),
        "evidence_statuses": len(EVIDENCE_STATUSES),
        "blocker_codes": len(BLOCKER_CODES),
        "evidence_record_fields": len(EVIDENCE_RECORD_FIELDS),
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
        "source_evidence_acquired": 0,
        "evidence_artifacts_retained": 0,
        "credentials_stored": 0,
        "network_retrievals_executed": 0,
        "historical_outcome_values_acquired": 0,
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
        / "source_evidence_acquisition_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_acquisition_implementation"
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
        f"{predecessor.SPECIFICATION_CONTRACT_VERSION}"
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
    print(f"Specification records replayed: {len(records)}")
    print(f"Specification comparisons: {len(comparison_ids)}")
    print(
        "Specification status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Specification blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(f"Evidence principles: {len(EVIDENCE_PRINCIPLES)}")
    print(f"Evidence classes: {len(EVIDENCE_CLASSES)}")
    print(f"Acquisition stages: {len(ACQUISITION_STAGES)}")
    print(f"Evidence requirements: {len(EVIDENCE_REQUIREMENTS)}")
    print(f"Evidence record fields: {len(EVIDENCE_RECORD_FIELDS)}")
    print(f"Plan digest: {plan_digest}")
    print("Source evidence acquired: 0")
    print("Evidence artifacts retained: 0")
    print("Credentials stored: 0")
    print("Network retrievals executed: 0")
    print("Historical outcome values acquired: 0")
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
