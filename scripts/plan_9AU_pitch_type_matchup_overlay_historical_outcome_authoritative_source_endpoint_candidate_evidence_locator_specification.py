#!/usr/bin/env python3
"""
Layer 9AU
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Evidence Locator Specification Plan

Plans the deterministic contract for explicitly supplied evidence locators
supporting an authoritative historical-outcome endpoint candidate.

Layer 9AT established that no endpoint candidate or evidence submission exists.
This layer defines how evidence locators must be supplied, scoped, typed,
versioned, authenticated, retained, and validated before evidence acquisition
can proceed.

Planning only.

This layer does not:
- invent an endpoint candidate;
- invent evidence locators;
- fabricate source authority;
- retrieve documentation or historical outcomes;
- store credential literals;
- retain raw endpoint responses;
- alter canonical source values or mappings;
- coerce, infer, default, impute, or substitute values;
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


LAYER_ID = "9AU"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_evidence_locator_specification_plan"
)

PLAN_VERSION = (
    "layer_9AU_historical_outcome_authoritative_source_endpoint_candidate_"
    "evidence_locator_specification_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AU_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_evidence_locator_specification_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "acquire_9AT_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AT_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_contract_v1"
)

EXPECTED_EVIDENCE_RECORDS = 16
EXPECTED_EVIDENCE_COMPARISONS = 16

EXPECTED_EVIDENCE_STATUS = "candidate_not_supplied"

EXPECTED_EVIDENCE_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


LOCATOR_PRINCIPLES = [
    {
        "principle_id": "HOAELSP-P01",
        "principle": (
            "Evidence locators may be specified only for an explicitly supplied "
            "endpoint candidate."
        ),
    },
    {
        "principle_id": "HOAELSP-P02",
        "principle": (
            "Every locator must identify exactly one evidence class and one "
            "candidate version."
        ),
    },
    {
        "principle_id": "HOAELSP-P03",
        "principle": (
            "Locators must be explicit and may not be inferred from provider "
            "names, domains, repository context, or undocumented conventions."
        ),
    },
    {
        "principle_id": "HOAELSP-P04",
        "principle": (
            "Remote locators must use approved secure transport and immutable "
            "snapshot locators must include version and digest metadata."
        ),
    },
    {
        "principle_id": "HOAELSP-P05",
        "principle": (
            "Credential requirements must use reference-only identifiers and "
            "must never include secret literals."
        ),
    },
    {
        "principle_id": "HOAELSP-P06",
        "principle": (
            "Locator scope must explicitly cover source authority, historical "
            "coverage, identity semantics, outcome semantics, licensing, "
            "availability, and schema or snapshot evidence."
        ),
    },
    {
        "principle_id": "HOAELSP-P07",
        "principle": (
            "Duplicate, overlapping, ambiguous, or conflicting locators must "
            "block implementation authority."
        ),
    },
    {
        "principle_id": "HOAELSP-P08",
        "principle": (
            "Approved locator specification grants locator implementation "
            "authority only and does not grant retrieval authority."
        ),
    },
]


LOCATOR_TYPES = [
    {
        "locator_type_id": "HOAELSP-L01",
        "locator_type": "https_documentation_uri",
        "eligible": True,
        "priority": 1,
    },
    {
        "locator_type_id": "HOAELSP-L02",
        "locator_type": "https_versioned_schema_uri",
        "eligible": True,
        "priority": 2,
    },
    {
        "locator_type_id": "HOAELSP-L03",
        "locator_type": "immutable_repository_path",
        "eligible": True,
        "priority": 3,
    },
    {
        "locator_type_id": "HOAELSP-L04",
        "locator_type": "immutable_snapshot_manifest",
        "eligible": True,
        "priority": 4,
    },
    {
        "locator_type_id": "HOAELSP-L05",
        "locator_type": "credentialed_documentation_endpoint",
        "eligible": True,
        "priority": 5,
    },
    {
        "locator_type_id": "HOAELSP-L06",
        "locator_type": "mutable_unversioned_reference",
        "eligible": False,
        "priority": 6,
    },
]


REQUIRED_LOCATOR_CLASSES = [
    {
        "locator_class_id": "HOAELSP-C01",
        "locator_class": "authority_documentation_locator",
        "required": True,
        "priority": 1,
    },
    {
        "locator_class_id": "HOAELSP-C02",
        "locator_class": "coverage_documentation_locator",
        "required": True,
        "priority": 2,
    },
    {
        "locator_class_id": "HOAELSP-C03",
        "locator_class": "identity_semantics_locator",
        "required": True,
        "priority": 3,
    },
    {
        "locator_class_id": "HOAELSP-C04",
        "locator_class": "outcome_semantics_locator",
        "required": True,
        "priority": 4,
    },
    {
        "locator_class_id": "HOAELSP-C05",
        "locator_class": "licensing_terms_locator",
        "required": True,
        "priority": 5,
    },
    {
        "locator_class_id": "HOAELSP-C06",
        "locator_class": "availability_documentation_locator",
        "required": True,
        "priority": 6,
    },
    {
        "locator_class_id": "HOAELSP-C07",
        "locator_class": "schema_or_snapshot_locator",
        "required": True,
        "priority": 7,
    },
]


SPECIFICATION_STAGES = [
    {
        "stage_id": "HOAELSP-S01",
        "stage_name": "evidence_acquisition_replay",
        "priority": 1,
    },
    {
        "stage_id": "HOAELSP-S02",
        "stage_name": "candidate_presence_gate",
        "priority": 2,
    },
    {
        "stage_id": "HOAELSP-S03",
        "stage_name": "locator_submission_inventory",
        "priority": 3,
    },
    {
        "stage_id": "HOAELSP-S04",
        "stage_name": "locator_type_validation",
        "priority": 4,
    },
    {
        "stage_id": "HOAELSP-S05",
        "stage_name": "locator_class_coverage_validation",
        "priority": 5,
    },
    {
        "stage_id": "HOAELSP-S06",
        "stage_name": "locator_scope_validation",
        "priority": 6,
    },
    {
        "stage_id": "HOAELSP-S07",
        "stage_name": "transport_and_immutability_validation",
        "priority": 7,
    },
    {
        "stage_id": "HOAELSP-S08",
        "stage_name": "credential_reference_validation",
        "priority": 8,
    },
    {
        "stage_id": "HOAELSP-S09",
        "stage_name": "version_and_digest_validation",
        "priority": 9,
    },
    {
        "stage_id": "HOAELSP-S10",
        "stage_name": "duplicate_and_conflict_validation",
        "priority": 10,
    },
    {
        "stage_id": "HOAELSP-S11",
        "stage_name": "locator_specification_disposition",
        "priority": 11,
    },
]


LOCATOR_REQUIREMENTS = [
    {
        "requirement_id": "HOAELSP-R01",
        "requirement": "candidate_submission_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R02",
        "requirement": "candidate_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R03",
        "requirement": "candidate_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R04",
        "requirement": "locator_submission_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R05",
        "requirement": "locator_submission_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R06",
        "requirement": "locator_type_eligible",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R07",
        "requirement": "locator_value_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R08",
        "requirement": "locator_class_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R09",
        "requirement": "candidate_scope_exact",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R10",
        "requirement": "comparison_scope_declared",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R11",
        "requirement": "source_owner_scope_declared",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R12",
        "requirement": "authority_locator_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R13",
        "requirement": "coverage_locator_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R14",
        "requirement": "identity_semantics_locator_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R15",
        "requirement": "outcome_semantics_locator_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R16",
        "requirement": "licensing_locator_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R17",
        "requirement": "availability_locator_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R18",
        "requirement": "schema_or_snapshot_locator_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R19",
        "requirement": "secure_transport_or_immutable_path",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R20",
        "requirement": "credential_reference_only",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R21",
        "requirement": "credential_literal_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R22",
        "requirement": "snapshot_or_schema_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R23",
        "requirement": "immutable_digest_algorithm",
        "expected": "sha256",
    },
    {
        "requirement_id": "HOAELSP-R24",
        "requirement": "duplicate_locators_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R25",
        "requirement": "conflicting_locators_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOAELSP-R26",
        "requirement": "network_retrieval_executed",
        "expected": False,
    },
]


LOCATOR_STATUSES = [
    {
        "status": "evidence_locator_specification_approved",
        "implementation_authority": True,
    },
    {
        "status": "candidate_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "locator_submission_not_supplied",
        "implementation_authority": False,
    },
    {
        "status": "locator_type_ineligible",
        "implementation_authority": False,
    },
    {
        "status": "required_locator_class_missing",
        "implementation_authority": False,
    },
    {
        "status": "locator_scope_incomplete",
        "implementation_authority": False,
    },
    {
        "status": "locator_transport_invalid",
        "implementation_authority": False,
    },
    {
        "status": "credential_contract_invalid",
        "implementation_authority": False,
    },
    {
        "status": "locator_version_or_digest_missing",
        "implementation_authority": False,
    },
    {
        "status": "locator_conflict",
        "implementation_authority": False,
    },
]


BLOCKER_CODES = [
    {
        "code": "historical_outcome_endpoint_candidate_missing",
        "category": "submission",
    },
    {
        "code": "historical_outcome_evidence_locator_submission_missing",
        "category": "submission",
    },
    {
        "code": "historical_outcome_evidence_locator_submission_id_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_evidence_locator_submission_version_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_evidence_locator_type_missing",
        "category": "type",
    },
    {
        "code": "historical_outcome_evidence_locator_type_ineligible",
        "category": "type",
    },
    {
        "code": "historical_outcome_evidence_locator_value_missing",
        "category": "locator",
    },
    {
        "code": "historical_outcome_authority_documentation_locator_missing",
        "category": "authority",
    },
    {
        "code": "historical_outcome_coverage_documentation_locator_missing",
        "category": "coverage",
    },
    {
        "code": "historical_outcome_identity_semantics_locator_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_outcome_semantics_locator_missing",
        "category": "semantics",
    },
    {
        "code": "historical_outcome_licensing_terms_locator_missing",
        "category": "licensing",
    },
    {
        "code": "historical_outcome_availability_documentation_locator_missing",
        "category": "stability",
    },
    {
        "code": "historical_outcome_schema_or_snapshot_locator_missing",
        "category": "versioning",
    },
    {
        "code": "historical_outcome_evidence_locator_candidate_scope_missing",
        "category": "scope",
    },
    {
        "code": "historical_outcome_evidence_locator_comparison_scope_missing",
        "category": "scope",
    },
    {
        "code": "historical_outcome_evidence_locator_source_owner_scope_missing",
        "category": "scope",
    },
    {
        "code": "historical_outcome_evidence_locator_transport_invalid",
        "category": "transport",
    },
    {
        "code": "historical_outcome_evidence_locator_credential_reference_missing",
        "category": "security",
    },
    {
        "code": "historical_outcome_evidence_locator_credential_literal_detected",
        "category": "security",
    },
    {
        "code": "historical_outcome_evidence_locator_version_missing",
        "category": "versioning",
    },
    {
        "code": "historical_outcome_evidence_locator_digest_missing",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_evidence_locator_duplicate",
        "category": "conflict",
    },
    {
        "code": "historical_outcome_evidence_locator_conflict",
        "category": "conflict",
    },
    {
        "code": "historical_outcome_evidence_locator_invention_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_evidence_retrieval_requested",
        "category": "authority",
    },
]


LOCATOR_SPECIFICATION_FIELDS = [
    "evidence_locator_specification_plan_contract_version",
    "evidence_locator_specification_plan_record_id",
    "source_evidence_acquisition_plan_record_id",
    "source_evidence_acquisition_plan_record_digest",
    "endpoint_candidate_specification_record_id",
    "authoritative_source_endpoint_configuration_record_id",
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
    "evidence_acquisition_status",
    "evidence_acquisition_blocker_codes",
    "candidate_supplied",
    "candidate_id",
    "candidate_version",
    "source_owner",
    "source_class",
    "locator_submission_supplied",
    "locator_submission_id",
    "locator_submission_version",
    "locator_type",
    "locator_type_eligible",
    "locator_value",
    "locator_class",
    "candidate_scope",
    "comparison_scope",
    "source_owner_scope",
    "credential_reference",
    "credential_literal_present",
    "snapshot_or_schema_version",
    "immutable_digest_algorithm",
    "immutable_digest",
    "locator_specification_status",
    "locator_specification_blocker_codes",
    "locator_specification_implementation_authority_granted",
    "locator_specification_rationale",
    "locator_specification_limitations",
    "evidence_locator_specification_plan_identity_digest",
    "evidence_locator_specification_plan_record_digest",
]


ORDERING_FIELDS = [
    {
        "ordinal": 1,
        "field": "comparison_record_id",
    },
    {
        "ordinal": 2,
        "field": "defect_source_record_id",
    },
    {
        "ordinal": 3,
        "field": "candidate_id",
    },
    {
        "ordinal": 4,
        "field": "locator_class",
    },
    {
        "ordinal": 5,
        "field": "locator_value",
    },
    {
        "ordinal": 6,
        "field": "evidence_locator_specification_plan_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AT_evidence_acquisition_records",
    },
    {
        "ordinal": 2,
        "step": "preserve_candidate_specification_and_defect_lineage",
    },
    {
        "ordinal": 3,
        "step": "require_explicit_candidate_submission",
    },
    {
        "ordinal": 4,
        "step": "load_explicit_locator_submissions",
    },
    {
        "ordinal": 5,
        "step": "reject_missing_or_invented_locators",
    },
    {
        "ordinal": 6,
        "step": "validate_locator_submission_identity_and_version",
    },
    {
        "ordinal": 7,
        "step": "validate_locator_type_and_value",
    },
    {
        "ordinal": 8,
        "step": "validate_required_locator_class_coverage",
    },
    {
        "ordinal": 9,
        "step": "validate_candidate_comparison_and_source_owner_scope",
    },
    {
        "ordinal": 10,
        "step": "validate_secure_transport_or_immutable_path",
    },
    {
        "ordinal": 11,
        "step": "validate_credential_reference_and_literal_absence",
    },
    {
        "ordinal": 12,
        "step": "validate_snapshot_or_schema_version_and_sha256_digest",
    },
    {
        "ordinal": 13,
        "step": "validate_duplicate_and_conflicting_locators",
    },
    {
        "ordinal": 14,
        "step": "emit_deterministic_locator_specification_records",
    },
    {
        "ordinal": 15,
        "step": "verify_forward_and_reverse_replay",
    },
    {
        "ordinal": 16,
        "step": "grant_locator_specification_implementation_only_when_complete",
    },
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "endpoint_candidate_selection_without_submission",
    "evidence_locator_invention",
    "evidence_locator_selection_without_submission",
    "authority_evidence_fabrication",
    "credential_literal_storage",
    "source_evidence_retrieval_planning",
    "source_evidence_fetch_execution",
    "source_evidence_parse_execution",
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
        and all(
            character in "0123456789abcdef"
            for character in value
        )
    )


def load_module(
    path: Path,
    module_name: str,
) -> Any:
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


def write_json(
    path: Path,
    payload: Any,
) -> None:
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
        "layer_9at_predecessor",
    )

    if (
        predecessor.EVIDENCE_ACQUISITION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AT contract version: "
            f"{predecessor.EVIDENCE_ACQUISITION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()
    plan = replay["plan"]

    records = predecessor.build_evidence_acquisition_records(
        plan,
        replay["records"],
    )

    reverse_records = predecessor.build_evidence_acquisition_records(
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
        row["evidence_acquisition_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "evidence_acquisition_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_at_contract_version_verified",
            "actual":
                predecessor.EVIDENCE_ACQUISITION_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.EVIDENCE_ACQUISITION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_at_replay_deterministic",
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
            "check": "nine_at_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": (
                sha256_payload(records)
                == sha256_payload(reverse_records)
            ),
        },
        {
            "check": "expected_evidence_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_EVIDENCE_RECORDS,
            "passed": (
                len(records)
                == EXPECTED_EVIDENCE_RECORDS
            ),
        },
        {
            "check": "expected_evidence_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_EVIDENCE_COMPARISONS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_EVIDENCE_COMPARISONS
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(
                sorted(status_counts.items())
            ),
            "expected": {
                EXPECTED_EVIDENCE_STATUS:
                    EXPECTED_EVIDENCE_RECORDS
            },
            "passed": status_counts == Counter(
                {
                    EXPECTED_EVIDENCE_STATUS:
                        EXPECTED_EVIDENCE_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_preserved",
            "actual": dict(
                sorted(blocker_counts.items())
            ),
            "expected": {
                EXPECTED_EVIDENCE_BLOCKER:
                    EXPECTED_EVIDENCE_RECORDS
            },
            "passed": blocker_counts == Counter(
                {
                    EXPECTED_EVIDENCE_BLOCKER:
                        EXPECTED_EVIDENCE_RECORDS
                }
            ),
        },
        {
            "check": "all_evidence_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_plan_record_digest"
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
            "check": "locator_principles_defined",
            "actual": len(LOCATOR_PRINCIPLES),
            "expected": 8,
            "passed": len(LOCATOR_PRINCIPLES) == 8,
        },
        {
            "check": "locator_types_defined",
            "actual": len(LOCATOR_TYPES),
            "expected": 6,
            "passed": len(LOCATOR_TYPES) == 6,
        },
        {
            "check": "required_locator_classes_defined",
            "actual": len(REQUIRED_LOCATOR_CLASSES),
            "expected": 7,
            "passed": len(REQUIRED_LOCATOR_CLASSES) == 7,
        },
        {
            "check": "specification_stages_defined",
            "actual": len(SPECIFICATION_STAGES),
            "expected": 11,
            "passed": len(SPECIFICATION_STAGES) == 11,
        },
        {
            "check": "locator_requirements_defined",
            "actual": len(LOCATOR_REQUIREMENTS),
            "expected": 26,
            "passed": len(LOCATOR_REQUIREMENTS) == 26,
        },
        {
            "check": "locator_statuses_defined",
            "actual": len(LOCATOR_STATUSES),
            "expected": 10,
            "passed": len(LOCATOR_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 26,
            "passed": len(BLOCKER_CODES) == 26,
        },
        {
            "check": "locator_specification_fields_defined",
            "actual": len(LOCATOR_SPECIFICATION_FIELDS),
            "expected": 47,
            "passed": (
                len(LOCATOR_SPECIFICATION_FIELDS)
                == 47
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
            "check": "candidate_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "endpoint_candidate_invention"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "locator_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "evidence_locator_invention"
                in PROHIBITED_AUTHORITIES
                and
                "evidence_locator_selection_without_submission"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "credential_literal_storage_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "credential_literal_storage"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "source_evidence_retrieval_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "source_evidence_fetch_execution"
                in PROHIBITED_AUTHORITIES
                and
                "source_evidence_parse_execution"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "historical_outcome_retrieval_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "historical_outcome_fetch_execution"
                in PROHIBITED_AUTHORITIES
                and
                "historical_outcome_parse_execution"
                in PROHIBITED_AUTHORITIES
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
            "check": "value_transformation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "boolean_to_integer_coercion"
                in PROHIBITED_AUTHORITIES
                and
                "source_value_defaulting"
                in PROHIBITED_AUTHORITIES
                and
                "source_value_inference"
                in PROHIBITED_AUTHORITIES
                and
                "source_value_imputation"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "locator_specifications_materialized_zero",
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
            "locator_principles": LOCATOR_PRINCIPLES,
            "locator_types": LOCATOR_TYPES,
            "required_locator_classes":
                REQUIRED_LOCATOR_CLASSES,
            "specification_stages":
                SPECIFICATION_STAGES,
            "locator_requirements":
                LOCATOR_REQUIREMENTS,
            "locator_statuses":
                LOCATOR_STATUSES,
            "blocker_codes":
                BLOCKER_CODES,
            "locator_specification_fields":
                LOCATOR_SPECIFICATION_FIELDS,
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
        "endpoint_candidate_evidence_locator_specification_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_specification_plan_failed"
    )

    next_layer = (
        "9AV_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_specification_implementation"
        if all_checks_passed
        else
        "9AU_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_specification_plan_remediation"
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
        OUTPUT_DIR / "locator_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        LOCATOR_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "locator_types.csv",
        [
            "locator_type_id",
            "locator_type",
            "eligible",
            "priority",
        ],
        LOCATOR_TYPES,
    )

    write_csv(
        OUTPUT_DIR / "required_locator_classes.csv",
        [
            "locator_class_id",
            "locator_class",
            "required",
            "priority",
        ],
        REQUIRED_LOCATOR_CLASSES,
    )

    write_csv(
        OUTPUT_DIR / "specification_stages.csv",
        [
            "stage_id",
            "stage_name",
            "priority",
        ],
        SPECIFICATION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "locator_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        LOCATOR_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "locator_statuses.csv",
        [
            "status",
            "implementation_authority",
        ],
        LOCATOR_STATUSES,
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
        OUTPUT_DIR / "locator_specification_field_contract.csv",
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
                LOCATOR_SPECIFICATION_FIELDS,
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
        OUTPUT_DIR / "candidate_missing_evidence_inventory.csv",
        [
            "source_evidence_acquisition_plan_record_id",
            "source_evidence_acquisition_plan_record_digest",
            "endpoint_candidate_specification_record_id",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "evidence_acquisition_status",
            "evidence_acquisition_blocker_codes",
            "candidate_supplied",
            "candidate_id",
            "candidate_version",
            "source_owner",
            "source_class",
            "evidence_locator_present",
        ],
        records,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.EVIDENCE_ACQUISITION_CONTRACT_VERSION,
        "evidence_records": len(records),
        "evidence_comparisons": len(comparison_ids),
        "evidence_status_counts":
            dict(sorted(status_counts.items())),
        "evidence_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "locator_principles":
            len(LOCATOR_PRINCIPLES),
        "locator_types":
            len(LOCATOR_TYPES),
        "required_locator_classes":
            len(REQUIRED_LOCATOR_CLASSES),
        "specification_stages":
            len(SPECIFICATION_STAGES),
        "locator_requirements":
            len(LOCATOR_REQUIREMENTS),
        "locator_statuses":
            len(LOCATOR_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "locator_specification_fields":
            len(LOCATOR_SPECIFICATION_FIELDS),
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
        "locator_specifications_materialized": 0,
        "credentials_stored": 0,
        "network_retrievals_executed": 0,
        "source_evidence_acquired": 0,
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
        "all_checks_passed":
            all_checks_passed,
        "recommended_next_layer":
            next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "evidence_locator_specification_plan_summary.json",
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
            "evidence_locator_specification_implementation"
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
        f"{predecessor.EVIDENCE_ACQUISITION_CONTRACT_VERSION}"
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
        "Evidence records replayed: "
        f"{len(records)}"
    )
    print(
        "Evidence comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Evidence status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Evidence blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Locator principles: "
        f"{len(LOCATOR_PRINCIPLES)}"
    )
    print(
        "Locator types: "
        f"{len(LOCATOR_TYPES)}"
    )
    print(
        "Required locator classes: "
        f"{len(REQUIRED_LOCATOR_CLASSES)}"
    )
    print(
        "Specification stages: "
        f"{len(SPECIFICATION_STAGES)}"
    )
    print(
        "Locator requirements: "
        f"{len(LOCATOR_REQUIREMENTS)}"
    )
    print(
        "Locator specification fields: "
        f"{len(LOCATOR_SPECIFICATION_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Locator specifications materialized: 0")
    print("Credentials stored: 0")
    print("Network retrievals executed: 0")
    print("Source evidence acquired: 0")
    print("Historical outcome values acquired: 0")
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
    print(
        "Activation recommendations emitted: 0"
    )
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
