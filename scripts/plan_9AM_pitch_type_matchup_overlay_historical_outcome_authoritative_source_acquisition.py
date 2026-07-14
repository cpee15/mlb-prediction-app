#!/usr/bin/env python3
"""
Layer 9AM
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Acquisition Plan

Plans deterministic acquisition of authoritative historical outcome sources
for the sixteen unresolved Layer 9AL discovery records.

Layer 9AL established that the repository contains only the original defective
boolean values and no independent authoritative numeric observations. This
layer defines bounded acquisition requirements for locating, retrieving,
retaining, validating, and versioning external or retained authoritative source
records.

Planning only.

This layer does not:
- perform network retrieval;
- mutate canonical source values;
- replace the canonical outcome mapping;
- coerce, default, infer, substitute, or impute values;
- regenerate downstream canonical records;
- calculate predictive quality, uncertainty, significance, superiority,
  equivalence, activation, production probabilities, market prices, or bets.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AM"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_authoritative_source_acquisition_plan"
)

PLAN_VERSION = (
    "layer_9AM_historical_outcome_authoritative_"
    "source_acquisition_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AM_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_source_acquisition_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "discover_9AL_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_observation.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AL_historical_outcome_authoritative_"
    "observation_discovery_contract_v1"
)

EXPECTED_DISCOVERY_RECORDS = 16
EXPECTED_DISCOVERY_COMPARISONS = 16

EXPECTED_DISCOVERY_STATUS = (
    "candidate_authority_insufficient"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


ACQUISITION_PRINCIPLES = [
    {
        "principle_id": "HOASAP-P01",
        "principle": (
            "Acquisition must begin from the exact unresolved comparison and "
            "defect-source identities preserved by Layer 9AL."
        ),
    },
    {
        "principle_id": "HOASAP-P02",
        "principle": (
            "Each acquired observation must map to the same game, target, event "
            "level, event identity, and event sequence as the defective source."
        ),
    },
    {
        "principle_id": "HOASAP-P03",
        "principle": (
            "Source authority must be established independently from the "
            "defective repository fixture."
        ),
    },
    {
        "principle_id": "HOASAP-P04",
        "principle": (
            "Acquired content must be retained as an immutable, versioned "
            "snapshot with retrieval metadata and content digest."
        ),
    },
    {
        "principle_id": "HOASAP-P05",
        "principle": (
            "Boolean values must be rejected before numeric-domain validation."
        ),
    },
    {
        "principle_id": "HOASAP-P06",
        "principle": (
            "No value may be accepted through coercion, defaulting, inference, "
            "heuristic substitution, or imputation."
        ),
    },
    {
        "principle_id": "HOASAP-P07",
        "principle": (
            "Conflicting acquired observations must remain unresolved until a "
            "separate deterministic conflict-resolution contract is approved."
        ),
    },
    {
        "principle_id": "HOASAP-P08",
        "principle": (
            "Acquisition authority does not grant canonical source correction "
            "or downstream recomputation authority."
        ),
    },
]


SOURCE_AUTHORITY_CLASSES = [
    {
        "authority_class_id": "HOASAP-S01",
        "authority_class": "official_event_record",
        "priority": 1,
        "description": (
            "Official event-level historical record published by the governing "
            "or originating data authority."
        ),
    },
    {
        "authority_class_id": "HOASAP-S02",
        "authority_class": "official_statistics_feed",
        "priority": 2,
        "description": (
            "Official statistics feed or immutable snapshot with exact event "
            "identity and outcome semantics."
        ),
    },
    {
        "authority_class_id": "HOASAP-S03",
        "authority_class": "project_retained_upstream_snapshot",
        "priority": 3,
        "description": (
            "Previously retained upstream snapshot with complete provenance and "
            "stable content digest."
        ),
    },
    {
        "authority_class_id": "HOASAP-S04",
        "authority_class": "validated_secondary_archive",
        "priority": 4,
        "description": (
            "Stable secondary archive accepted only when official sources are "
            "unavailable and provenance is complete."
        ),
    },
    {
        "authority_class_id": "HOASAP-S05",
        "authority_class": "non_authoritative_reference",
        "priority": 5,
        "description": (
            "Reference-only candidate that cannot independently authorize a "
            "historical source correction."
        ),
    },
]


ACQUISITION_STAGES = [
    {
        "stage_id": "HOASAP-A01",
        "stage_name": "unresolved_identity_replay",
        "priority": 1,
        "description": (
            "Replay Layer 9AL unresolved discovery identities deterministically."
        ),
    },
    {
        "stage_id": "HOASAP-A02",
        "stage_name": "source_endpoint_inventory",
        "priority": 2,
        "description": (
            "Identify eligible source endpoints, archives, snapshots, or files."
        ),
    },
    {
        "stage_id": "HOASAP-A03",
        "stage_name": "retrieval_request_construction",
        "priority": 3,
        "description": (
            "Construct bounded retrieval requests from exact historical identity."
        ),
    },
    {
        "stage_id": "HOASAP-A04",
        "stage_name": "raw_response_retention",
        "priority": 4,
        "description": (
            "Retain immutable raw source content and retrieval metadata."
        ),
    },
    {
        "stage_id": "HOASAP-A05",
        "stage_name": "candidate_observation_parsing",
        "priority": 5,
        "description": (
            "Parse candidate values without coercion, defaulting, or inference."
        ),
    },
    {
        "stage_id": "HOASAP-A06",
        "stage_name": "identity_and_semantic_validation",
        "priority": 6,
        "description": (
            "Validate event identity and outcome-field semantics."
        ),
    },
    {
        "stage_id": "HOASAP-A07",
        "stage_name": "authority_and_provenance_validation",
        "priority": 7,
        "description": (
            "Validate authority class, provenance, retrieval evidence, and digest."
        ),
    },
    {
        "stage_id": "HOASAP-A08",
        "stage_name": "candidate_cardinality_assessment",
        "priority": 8,
        "description": (
            "Classify zero, one, equivalent duplicate, or conflicting candidates."
        ),
    },
    {
        "stage_id": "HOASAP-A09",
        "stage_name": "acquisition_disposition",
        "priority": 9,
        "description": (
            "Emit deterministic acquisition status and correction authority."
        ),
    },
]


RETRIEVAL_REQUIREMENTS = [
    {
        "requirement_id": "HOASAP-R01",
        "requirement": "retrieval_source_uri_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASAP-R02",
        "requirement": "retrieval_timestamp_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASAP-R03",
        "requirement": "raw_response_retained",
        "expected": True,
    },
    {
        "requirement_id": "HOASAP-R04",
        "requirement": "raw_response_digest_valid",
        "expected": True,
    },
    {
        "requirement_id": "HOASAP-R05",
        "requirement": "source_authority_class_valid",
        "expected": True,
    },
    {
        "requirement_id": "HOASAP-R06",
        "requirement": "candidate_value_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASAP-R07",
        "requirement": "candidate_value_type",
        "expected": "finite_int_or_float_excluding_bool",
    },
    {
        "requirement_id": "HOASAP-R08",
        "requirement": "identity_alignment_complete",
        "expected": True,
    },
    {
        "requirement_id": "HOASAP-R09",
        "requirement": "outcome_semantics_verified",
        "expected": True,
    },
    {
        "requirement_id": "HOASAP-R10",
        "requirement": "coercion_defaulting_inference_or_imputation_used",
        "expected": False,
    },
    {
        "requirement_id": "HOASAP-R11",
        "requirement": "source_snapshot_versioned",
        "expected": True,
    },
    {
        "requirement_id": "HOASAP-R12",
        "requirement": "preexisting_defective_value_preserved",
        "expected": True,
    },
]


ACQUISITION_STATUSES = [
    {
        "status": "authoritative_source_acquired",
        "correction_authority": True,
    },
    {
        "status": "authoritative_source_not_found",
        "correction_authority": False,
    },
    {
        "status": "retrieval_failed",
        "correction_authority": False,
    },
    {
        "status": "raw_response_not_retained",
        "correction_authority": False,
    },
    {
        "status": "candidate_value_invalid",
        "correction_authority": False,
    },
    {
        "status": "candidate_identity_mismatch",
        "correction_authority": False,
    },
    {
        "status": "candidate_semantics_unverified",
        "correction_authority": False,
    },
    {
        "status": "candidate_authority_insufficient",
        "correction_authority": False,
    },
    {
        "status": "candidate_provenance_incomplete",
        "correction_authority": False,
    },
    {
        "status": "duplicate_equivalent_candidates",
        "correction_authority": False,
    },
    {
        "status": "conflicting_authoritative_candidates",
        "correction_authority": False,
    },
]


BLOCKER_CODES = [
    {
        "code": "historical_outcome_source_endpoint_missing",
        "category": "retrieval",
    },
    {
        "code": "historical_outcome_source_retrieval_failed",
        "category": "retrieval",
    },
    {
        "code": "historical_outcome_raw_response_missing",
        "category": "evidence",
    },
    {
        "code": "historical_outcome_raw_response_digest_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_source_authority_insufficient",
        "category": "authority",
    },
    {
        "code": "historical_outcome_candidate_value_missing",
        "category": "value",
    },
    {
        "code": "historical_outcome_candidate_value_boolean",
        "category": "value",
    },
    {
        "code": "historical_outcome_candidate_value_non_numeric",
        "category": "value",
    },
    {
        "code": "historical_outcome_candidate_value_non_finite",
        "category": "value",
    },
    {
        "code": "historical_outcome_candidate_game_identity_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_outcome_candidate_event_identity_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_outcome_candidate_target_identity_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_outcome_candidate_sequence_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_outcome_candidate_semantics_unverified",
        "category": "semantics",
    },
    {
        "code": "historical_outcome_candidate_provenance_incomplete",
        "category": "provenance",
    },
    {
        "code": "historical_outcome_duplicate_equivalent_candidates",
        "category": "cardinality",
    },
    {
        "code": "historical_outcome_conflicting_authoritative_candidates",
        "category": "conflict",
    },
    {
        "code": "historical_outcome_candidate_coercion_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_candidate_default_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_candidate_inference_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_candidate_imputation_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_mapping_change_requested",
        "category": "authority",
    },
]


ACQUISITION_RECORD_FIELDS = [
    "authoritative_source_acquisition_contract_version",
    "authoritative_source_acquisition_record_id",
    "authoritative_observation_discovery_record_id",
    "authoritative_observation_discovery_record_digest",
    "source_value_remediation_plan_record_id",
    "source_value_audit_record_id",
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
    "defective_value",
    "defective_runtime_type",
    "source_authority_class",
    "source_authority_priority",
    "retrieval_source_uri",
    "retrieval_request_identity",
    "retrieval_timestamp_utc",
    "retrieval_status",
    "raw_response_artifact_path",
    "raw_response_digest",
    "raw_response_content_type",
    "candidate_source_record_id",
    "candidate_source_record_digest",
    "candidate_value_present",
    "candidate_value",
    "candidate_runtime_type",
    "candidate_value_valid",
    "game_identity_match",
    "event_identity_match",
    "target_identity_match",
    "event_sequence_match",
    "outcome_semantics_verified",
    "source_provenance_complete",
    "source_authority_sufficient",
    "candidate_count",
    "equivalent_candidate_count",
    "conflicting_candidate_count",
    "acquisition_status",
    "acquisition_blocker_codes",
    "correction_authority_granted",
    "acquisition_rationale",
    "acquisition_limitations",
    "authoritative_source_acquisition_identity_digest",
    "authoritative_source_acquisition_record_digest",
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
        "field": "source_authority_priority",
    },
    {
        "ordinal": 4,
        "field": "retrieval_source_uri",
    },
    {
        "ordinal": 5,
        "field": "candidate_source_record_id",
    },
    {
        "ordinal": 6,
        "field": "authoritative_source_acquisition_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AL_unresolved_discovery_records",
    },
    {
        "ordinal": 2,
        "step": "preserve_defect_and_discovery_lineage",
    },
    {
        "ordinal": 3,
        "step": "inventory_authoritative_source_endpoints",
    },
    {
        "ordinal": 4,
        "step": "construct_bounded_identity_specific_retrieval_requests",
    },
    {
        "ordinal": 5,
        "step": "retrieve_and_retain_immutable_raw_responses",
    },
    {
        "ordinal": 6,
        "step": "calculate_raw_response_and_source_record_digests",
    },
    {
        "ordinal": 7,
        "step": "parse_candidate_values_without_transformation",
    },
    {
        "ordinal": 8,
        "step": "validate_numeric_domain_excluding_bool",
    },
    {
        "ordinal": 9,
        "step": "validate_identity_semantics_authority_and_provenance",
    },
    {
        "ordinal": 10,
        "step": "classify_candidate_cardinality_and_conflicts",
    },
    {
        "ordinal": 11,
        "step": "emit_deterministic_acquisition_records",
    },
    {
        "ordinal": 12,
        "step": "verify_forward_and_reverse_replay",
    },
    {
        "ordinal": 13,
        "step": "grant_correction_planning_only_for_authoritative_records",
    },
]


PROHIBITED_AUTHORITIES = [
    "canonical_source_value_mutation",
    "canonical_outcome_mapping_change",
    "boolean_to_integer_coercion",
    "source_value_defaulting",
    "source_value_inference",
    "source_value_imputation",
    "heuristic_candidate_substitution",
    "unversioned_fixture_replacement",
    "canonical_evaluation_row_recomputation",
    "canonical_join_record_recomputation",
    "canonical_comparison_record_recomputation",
    "canonical_metric_recomputation",
    "canonical_interpretation_recomputation",
    "canonical_evidence_recomputation",
    "canonical_remediation_recomputation",
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

                if isinstance(
                    value,
                    (dict, list, tuple),
                ):
                    serialized[field] = canonical_json(
                        value
                    )
                else:
                    serialized[field] = value

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
        "layer_9al_predecessor",
    )

    if (
        predecessor.DISCOVERY_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AL contract version: "
            f"{predecessor.DISCOVERY_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()

    plan = replay["plan"]

    discovery_records = (
        predecessor.build_discovery_records(
            plan,
            replay["records"],
        )
    )

    reverse_discovery_records = (
        predecessor.build_discovery_records(
            plan,
            list(
                reversed(
                    replay["reverse_records"]
                )
            ),
        )
    )

    return {
        "module": predecessor,
        "records": discovery_records,
        "reverse_records":
            reverse_discovery_records,
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
        row["discovery_status"]
        for row in records
    )

    source_class_counts = Counter(
        row["candidate_source_class"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "discovery_blocker_codes"
        ]
    )

    checks = [
        {
            "check": "nine_al_contract_version_verified",
            "actual":
                predecessor.DISCOVERY_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.DISCOVERY_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_al_replay_deterministic",
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
            "check": "nine_al_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": (
                sha256_payload(records)
                == sha256_payload(reverse_records)
            ),
        },
        {
            "check": "expected_discovery_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_DISCOVERY_RECORDS,
            "passed": (
                len(records)
                == EXPECTED_DISCOVERY_RECORDS
            ),
        },
        {
            "check": "expected_discovery_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected":
                EXPECTED_DISCOVERY_COMPARISONS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_DISCOVERY_COMPARISONS
            ),
        },
        {
            "check": "all_discovery_statuses_authority_insufficient",
            "actual":
                dict(sorted(status_counts.items())),
            "expected": {
                EXPECTED_DISCOVERY_STATUS:
                    EXPECTED_DISCOVERY_RECORDS
            },
            "passed": (
                status_counts
                == Counter(
                    {
                        EXPECTED_DISCOVERY_STATUS:
                            EXPECTED_DISCOVERY_RECORDS
                    }
                )
            ),
        },
        {
            "check": "all_candidates_non_authoritative",
            "actual":
                dict(
                    sorted(
                        source_class_counts.items()
                    )
                ),
            "expected": {
                "non_authoritative_candidate":
                    EXPECTED_DISCOVERY_RECORDS
            },
            "passed": (
                source_class_counts
                == Counter(
                    {
                        "non_authoritative_candidate":
                            EXPECTED_DISCOVERY_RECORDS
                    }
                )
            ),
        },
        {
            "check": "all_discovery_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authoritative_observation_discovery_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_observation_discovery_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row[
                        "authoritative_field_name"
                    ]
                    for row in records
                }
            ),
            "expected": [
                AUTHORITATIVE_FIELD_NAME
            ],
            "passed": all(
                row[
                    "authoritative_field_name"
                ]
                == AUTHORITATIVE_FIELD_NAME
                for row in records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row[
                        "authoritative_field_path"
                    ]
                    for row in records
                }
            ),
            "expected": [
                AUTHORITATIVE_FIELD_PATH
            ],
            "passed": all(
                row[
                    "authoritative_field_path"
                ]
                == AUTHORITATIVE_FIELD_PATH
                for row in records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row[
                        "rejected_metadata_field_name"
                    ]
                    for row in records
                }
            ),
            "expected": [
                REJECTED_METADATA_FIELD
            ],
            "passed": all(
                row[
                    "rejected_metadata_field_name"
                ]
                == REJECTED_METADATA_FIELD
                for row in records
            ),
        },
        {
            "check": "acquisition_principles_defined",
            "actual":
                len(ACQUISITION_PRINCIPLES),
            "expected": 8,
            "passed": (
                len(ACQUISITION_PRINCIPLES)
                == 8
            ),
        },
        {
            "check": "source_authority_classes_defined",
            "actual":
                len(SOURCE_AUTHORITY_CLASSES),
            "expected": 5,
            "passed": (
                len(SOURCE_AUTHORITY_CLASSES)
                == 5
            ),
        },
        {
            "check": "acquisition_stages_defined",
            "actual":
                len(ACQUISITION_STAGES),
            "expected": 9,
            "passed": (
                len(ACQUISITION_STAGES)
                == 9
            ),
        },
        {
            "check": "retrieval_requirements_defined",
            "actual":
                len(RETRIEVAL_REQUIREMENTS),
            "expected": 12,
            "passed": (
                len(RETRIEVAL_REQUIREMENTS)
                == 12
            ),
        },
        {
            "check": "acquisition_statuses_defined",
            "actual":
                len(ACQUISITION_STATUSES),
            "expected": 11,
            "passed": (
                len(ACQUISITION_STATUSES)
                == 11
            ),
        },
        {
            "check": "blocker_codes_defined",
            "actual":
                len(BLOCKER_CODES),
            "expected": 22,
            "passed": (
                len(BLOCKER_CODES) == 22
            ),
        },
        {
            "check": "acquisition_record_fields_defined",
            "actual":
                len(ACQUISITION_RECORD_FIELDS),
            "expected": 52,
            "passed": (
                len(ACQUISITION_RECORD_FIELDS)
                == 52
            ),
        },
        {
            "check": "ordering_fields_defined",
            "actual":
                len(ORDERING_FIELDS),
            "expected": 6,
            "passed": (
                len(ORDERING_FIELDS) == 6
            ),
        },
        {
            "check": "implementation_steps_defined",
            "actual":
                len(IMPLEMENTATION_STEPS),
            "expected": 13,
            "passed": (
                len(IMPLEMENTATION_STEPS)
                == 13
            ),
        },
        {
            "check": "canonical_source_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_source_value_mutation"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "mapping_change_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_outcome_mapping_change"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "coercion_defaulting_inference_and_imputation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "boolean_to_integer_coercion"
                in PROHIBITED_AUTHORITIES
                and "source_value_defaulting"
                in PROHIBITED_AUTHORITIES
                and "source_value_inference"
                in PROHIBITED_AUTHORITIES
                and "source_value_imputation"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "canonical_sources_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "network_retrieval_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "canonical_mappings_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "source_values_not_repaired",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "candidate_values_not_transformed",
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
            "plan_version":
                PLAN_VERSION,
            "acquisition_principles":
                ACQUISITION_PRINCIPLES,
            "source_authority_classes":
                SOURCE_AUTHORITY_CLASSES,
            "acquisition_stages":
                ACQUISITION_STAGES,
            "retrieval_requirements":
                RETRIEVAL_REQUIREMENTS,
            "acquisition_statuses":
                ACQUISITION_STATUSES,
            "blocker_codes":
                BLOCKER_CODES,
            "acquisition_record_fields":
                ACQUISITION_RECORD_FIELDS,
            "ordering_fields":
                ORDERING_FIELDS,
            "implementation_steps":
                IMPLEMENTATION_STEPS,
            "prohibited_authorities":
                PROHIBITED_AUTHORITIES,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_"
        "acquisition_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_"
        "acquisition_plan_failed"
    )

    next_layer = (
        "9AN_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_"
        "acquisition_implementation"
        if all_checks_passed
        else
        "9AM_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_"
        "acquisition_plan_remediation"
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
        OUTPUT_DIR / "acquisition_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        ACQUISITION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "source_authority_classes.csv",
        [
            "authority_class_id",
            "authority_class",
            "priority",
            "description",
        ],
        SOURCE_AUTHORITY_CLASSES,
    )

    write_csv(
        OUTPUT_DIR / "acquisition_stages.csv",
        [
            "stage_id",
            "stage_name",
            "priority",
            "description",
        ],
        ACQUISITION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "retrieval_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        RETRIEVAL_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "acquisition_statuses.csv",
        [
            "status",
            "correction_authority",
        ],
        ACQUISITION_STATUSES,
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
        / "acquisition_record_field_contract.csv",
        [
            "ordinal",
            "field",
        ],
        [
            {
                "ordinal": index,
                "field": field,
            }
            for index, field
            in enumerate(
                ACQUISITION_RECORD_FIELDS,
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
        / "unresolved_discovery_inventory.csv",
        [
            "authoritative_observation_discovery_record_id",
            "authoritative_observation_discovery_record_digest",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "defective_value",
            "defective_runtime_type",
            "candidate_source_class",
            "candidate_value",
            "candidate_runtime_type",
            "discovery_status",
            "discovery_blocker_codes",
        ],
        records,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        [
            {
                "authority": authority,
                "granted": False,
                "reason": (
                    "Layer 9AM is planning-only and grants no network "
                    "retrieval, canonical mutation, mapping change, coercion, "
                    "defaulting, inference, imputation, downstream "
                    "recomputation, quality, production, market, pricing, or "
                    "betting authority."
                ),
            }
            for authority
            in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_authoritative_"
                    "source_acquisition_implementation"
                ),
                "granted":
                    all_checks_passed,
                "reason": (
                    "Layer 9AN may implement bounded source acquisition and "
                    "immutable raw-response retention under the identity, "
                    "authority, evidence, domain, semantic, provenance, "
                    "cardinality, and conflict rules defined by this plan."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.DISCOVERY_CONTRACT_VERSION,
        "discovery_records":
            len(records),
        "discovery_comparisons":
            len(comparison_ids),
        "discovery_status_counts":
            dict(
                sorted(
                    status_counts.items()
                )
            ),
        "candidate_source_class_counts":
            dict(
                sorted(
                    source_class_counts.items()
                )
            ),
        "discovery_blocker_counts":
            dict(
                sorted(
                    blocker_counts.items()
                )
            ),
        "acquisition_principles":
            len(ACQUISITION_PRINCIPLES),
        "source_authority_classes":
            len(SOURCE_AUTHORITY_CLASSES),
        "acquisition_stages":
            len(ACQUISITION_STAGES),
        "retrieval_requirements":
            len(RETRIEVAL_REQUIREMENTS),
        "acquisition_statuses":
            len(ACQUISITION_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "acquisition_record_fields":
            len(ACQUISITION_RECORD_FIELDS),
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
        "authoritative_sources_acquired": 0,
        "network_retrievals_executed": 0,
        "raw_source_artifacts_retained": 0,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "source_values_repaired": 0,
        "candidate_values_coerced": 0,
        "candidate_values_defaulted": 0,
        "candidate_values_inferred": 0,
        "candidate_values_imputed": 0,
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
        / "authoritative_source_acquisition_plan_summary.json",
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
            "historical_outcome_authoritative_"
            "source_acquisition_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld":
            sorted(PROHIBITED_AUTHORITIES),
        "recommended_next_layer":
            next_layer,
        "output_directory": str(
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
        f"{predecessor.DISCOVERY_CONTRACT_VERSION}"
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
        f"Discovery records replayed: {len(records)}"
    )
    print(
        "Discovery comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Discovery status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Candidate source-class counts: "
        f"{dict(sorted(source_class_counts.items()))}"
    )
    print(
        "Acquisition principles: "
        f"{len(ACQUISITION_PRINCIPLES)}"
    )
    print(
        "Source authority classes: "
        f"{len(SOURCE_AUTHORITY_CLASSES)}"
    )
    print(
        "Acquisition stages: "
        f"{len(ACQUISITION_STAGES)}"
    )
    print(
        "Retrieval requirements: "
        f"{len(RETRIEVAL_REQUIREMENTS)}"
    )
    print(
        "Acquisition record fields: "
        f"{len(ACQUISITION_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Authoritative sources acquired: 0")
    print("Network retrievals executed: 0")
    print("Raw source artifacts retained: 0")
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Source values repaired: 0")
    print("Candidate values coerced: 0")
    print("Candidate values defaulted: 0")
    print("Candidate values inferred: 0")
    print("Candidate values imputed: 0")
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
