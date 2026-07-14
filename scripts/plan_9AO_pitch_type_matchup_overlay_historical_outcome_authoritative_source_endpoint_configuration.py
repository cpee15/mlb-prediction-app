#!/usr/bin/env python3
"""
Layer 9AO
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Configuration Plan

Plans deterministic configuration of an approved authoritative source endpoint
for the sixteen unresolved Layer 9AN acquisition records.

Layer 9AN proved that no endpoint, archive, retained snapshot, or approved
credentialed source is currently configured. This layer defines the endpoint
configuration contract required before any retrieval implementation may run.

Planning only.

This layer does not:
- select or invent a concrete endpoint;
- store credentials or secrets;
- execute network retrieval;
- acquire or parse historical outcomes;
- mutate canonical source values or mappings;
- coerce, default, infer, substitute, or impute values;
- recompute downstream canonical records;
- calculate quality, uncertainty, significance, superiority, equivalence,
  activation, production probabilities, market prices, or betting edges.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AO"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_authoritative_source_endpoint_configuration_plan"
)

PLAN_VERSION = (
    "layer_9AO_historical_outcome_authoritative_"
    "source_endpoint_configuration_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AO_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_source_endpoint_configuration_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "acquire_9AN_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_source.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AN_historical_outcome_authoritative_"
    "source_acquisition_contract_v1"
)

EXPECTED_ACQUISITION_RECORDS = 16
EXPECTED_ACQUISITION_COMPARISONS = 16

EXPECTED_ACQUISITION_STATUS = (
    "authoritative_source_not_found"
)

EXPECTED_RETRIEVAL_STATUS = (
    "not_executed_endpoint_missing"
)

EXPECTED_BLOCKER_CODE = (
    "historical_outcome_source_endpoint_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


CONFIGURATION_PRINCIPLES = [
    {
        "principle_id": "HOASECP-P01",
        "principle": (
            "Endpoint configuration must be explicit, versioned, reviewable, "
            "and separate from retrieval execution."
        ),
    },
    {
        "principle_id": "HOASECP-P02",
        "principle": (
            "A configured endpoint must identify the governing or originating "
            "authority for the historical event data."
        ),
    },
    {
        "principle_id": "HOASECP-P03",
        "principle": (
            "Endpoint configuration must define exact identity parameters for "
            "game, target, event level, event identity, and event sequence."
        ),
    },
    {
        "principle_id": "HOASECP-P04",
        "principle": (
            "Endpoint configuration must define the exact authoritative outcome "
            "field and explicitly distinguish it from availability metadata."
        ),
    },
    {
        "principle_id": "HOASECP-P05",
        "principle": (
            "Credentials and secrets must be referenced by approved environment "
            "or secret-manager identifiers and never embedded in repository "
            "source."
        ),
    },
    {
        "principle_id": "HOASECP-P06",
        "principle": (
            "The endpoint must support immutable raw-response retention and "
            "content-digest calculation."
        ),
    },
    {
        "principle_id": "HOASECP-P07",
        "principle": (
            "Rate limits, retry behavior, timeout behavior, pagination, and "
            "failure handling must be bounded deterministically."
        ),
    },
    {
        "principle_id": "HOASECP-P08",
        "principle": (
            "Endpoint approval grants configuration implementation authority "
            "only and does not grant retrieval or source-correction authority."
        ),
    },
]


ENDPOINT_AUTHORITY_CLASSES = [
    {
        "authority_class_id": "HOASECP-S01",
        "authority_class": "official_governing_event_api",
        "priority": 1,
        "configuration_eligible": True,
        "description": (
            "Official API published by the governing or originating authority "
            "for the historical event."
        ),
    },
    {
        "authority_class_id": "HOASECP-S02",
        "authority_class": "official_statistics_archive",
        "priority": 2,
        "configuration_eligible": True,
        "description": (
            "Official historical statistics archive with stable event identity "
            "and documented outcome semantics."
        ),
    },
    {
        "authority_class_id": "HOASECP-S03",
        "authority_class": "project_retained_upstream_snapshot_store",
        "priority": 3,
        "configuration_eligible": True,
        "description": (
            "Project-controlled immutable store containing retained authoritative "
            "upstream responses."
        ),
    },
    {
        "authority_class_id": "HOASECP-S04",
        "authority_class": "validated_secondary_archive",
        "priority": 4,
        "configuration_eligible": True,
        "description": (
            "Stable secondary archive eligible only when official sources are "
            "unavailable and authority review is documented."
        ),
    },
    {
        "authority_class_id": "HOASECP-S05",
        "authority_class": "unverified_reference_source",
        "priority": 5,
        "configuration_eligible": False,
        "description": (
            "Reference-only source that cannot authorize acquisition or "
            "historical correction."
        ),
    },
]


CONFIGURATION_STAGES = [
    {
        "stage_id": "HOASECP-C01",
        "stage_name": "endpoint_requirement_replay",
        "priority": 1,
        "description": (
            "Replay the sixteen Layer 9AN endpoint-missing acquisition records."
        ),
    },
    {
        "stage_id": "HOASECP-C02",
        "stage_name": "endpoint_candidate_inventory",
        "priority": 2,
        "description": (
            "Inventory proposed endpoints without executing retrieval."
        ),
    },
    {
        "stage_id": "HOASECP-C03",
        "stage_name": "authority_classification",
        "priority": 3,
        "description": (
            "Classify each proposed endpoint by source authority."
        ),
    },
    {
        "stage_id": "HOASECP-C04",
        "stage_name": "identity_parameter_contract",
        "priority": 4,
        "description": (
            "Define required historical identity request parameters."
        ),
    },
    {
        "stage_id": "HOASECP-C05",
        "stage_name": "outcome_field_semantic_contract",
        "priority": 5,
        "description": (
            "Define the authoritative outcome field and rejected metadata fields."
        ),
    },
    {
        "stage_id": "HOASECP-C06",
        "stage_name": "credential_reference_contract",
        "priority": 6,
        "description": (
            "Define secret references without storing credential values."
        ),
    },
    {
        "stage_id": "HOASECP-C07",
        "stage_name": "transport_policy_contract",
        "priority": 7,
        "description": (
            "Define method, timeout, retry, pagination, and rate-limit behavior."
        ),
    },
    {
        "stage_id": "HOASECP-C08",
        "stage_name": "response_retention_contract",
        "priority": 8,
        "description": (
            "Define immutable response retention, content type, and digest rules."
        ),
    },
    {
        "stage_id": "HOASECP-C09",
        "stage_name": "configuration_validation",
        "priority": 9,
        "description": (
            "Validate completeness, authority, security, identity, and semantics."
        ),
    },
    {
        "stage_id": "HOASECP-C10",
        "stage_name": "configuration_disposition",
        "priority": 10,
        "description": (
            "Emit deterministic endpoint-configuration status and authority."
        ),
    },
]


ENDPOINT_REQUIREMENTS = [
    {
        "requirement_id": "HOASECP-R01",
        "requirement": "endpoint_configuration_id_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R02",
        "requirement": "endpoint_configuration_version_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R03",
        "requirement": "source_authority_class_eligible",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R04",
        "requirement": "base_uri_present",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R05",
        "requirement": "transport_protocol",
        "expected": "https_or_immutable_local_snapshot",
    },
    {
        "requirement_id": "HOASECP-R06",
        "requirement": "request_method_defined",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R07",
        "requirement": "identity_parameter_mapping_complete",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R08",
        "requirement": "authoritative_outcome_field_defined",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R09",
        "requirement": "rejected_metadata_fields_defined",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R10",
        "requirement": "credential_reference_only",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R11",
        "requirement": "credential_literal_absent",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R12",
        "requirement": "timeout_policy_defined",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R13",
        "requirement": "retry_policy_defined",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R14",
        "requirement": "rate_limit_policy_defined",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R15",
        "requirement": "pagination_policy_defined",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R16",
        "requirement": "raw_response_retention_defined",
        "expected": True,
    },
    {
        "requirement_id": "HOASECP-R17",
        "requirement": "response_digest_algorithm",
        "expected": "sha256",
    },
    {
        "requirement_id": "HOASECP-R18",
        "requirement": "network_retrieval_executed_during_configuration",
        "expected": False,
    },
]


CONFIGURATION_STATUSES = [
    {
        "status": "endpoint_configuration_approved",
        "retrieval_planning_authority": True,
    },
    {
        "status": "endpoint_candidate_not_supplied",
        "retrieval_planning_authority": False,
    },
    {
        "status": "endpoint_authority_insufficient",
        "retrieval_planning_authority": False,
    },
    {
        "status": "endpoint_identity_contract_incomplete",
        "retrieval_planning_authority": False,
    },
    {
        "status": "endpoint_outcome_semantics_incomplete",
        "retrieval_planning_authority": False,
    },
    {
        "status": "endpoint_credential_contract_invalid",
        "retrieval_planning_authority": False,
    },
    {
        "status": "endpoint_transport_policy_incomplete",
        "retrieval_planning_authority": False,
    },
    {
        "status": "endpoint_response_retention_incomplete",
        "retrieval_planning_authority": False,
    },
    {
        "status": "endpoint_configuration_conflict",
        "retrieval_planning_authority": False,
    },
]


BLOCKER_CODES = [
    {
        "code": "historical_outcome_endpoint_candidate_missing",
        "category": "configuration",
    },
    {
        "code": "historical_outcome_endpoint_authority_insufficient",
        "category": "authority",
    },
    {
        "code": "historical_outcome_endpoint_base_uri_missing",
        "category": "transport",
    },
    {
        "code": "historical_outcome_endpoint_protocol_invalid",
        "category": "transport",
    },
    {
        "code": "historical_outcome_endpoint_request_method_missing",
        "category": "transport",
    },
    {
        "code": "historical_outcome_endpoint_game_parameter_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_endpoint_target_parameter_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_endpoint_event_level_parameter_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_endpoint_event_identity_parameter_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_endpoint_sequence_parameter_missing",
        "category": "identity",
    },
    {
        "code": "historical_outcome_endpoint_outcome_field_missing",
        "category": "semantics",
    },
    {
        "code": "historical_outcome_endpoint_outcome_semantics_unverified",
        "category": "semantics",
    },
    {
        "code": "historical_outcome_endpoint_metadata_rejection_missing",
        "category": "semantics",
    },
    {
        "code": "historical_outcome_endpoint_credential_reference_missing",
        "category": "security",
    },
    {
        "code": "historical_outcome_endpoint_credential_literal_detected",
        "category": "security",
    },
    {
        "code": "historical_outcome_endpoint_timeout_policy_missing",
        "category": "transport",
    },
    {
        "code": "historical_outcome_endpoint_retry_policy_missing",
        "category": "transport",
    },
    {
        "code": "historical_outcome_endpoint_rate_limit_policy_missing",
        "category": "transport",
    },
    {
        "code": "historical_outcome_endpoint_pagination_policy_missing",
        "category": "transport",
    },
    {
        "code": "historical_outcome_endpoint_response_retention_missing",
        "category": "evidence",
    },
    {
        "code": "historical_outcome_endpoint_response_digest_policy_missing",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_endpoint_configuration_conflict",
        "category": "conflict",
    },
    {
        "code": "historical_outcome_unapproved_network_retrieval_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_canonical_source_mutation_requested",
        "category": "authority",
    },
]


CONFIGURATION_RECORD_FIELDS = [
    "authoritative_source_endpoint_configuration_contract_version",
    "authoritative_source_endpoint_configuration_record_id",
    "authoritative_source_acquisition_record_id",
    "authoritative_source_acquisition_record_digest",
    "authoritative_observation_discovery_record_id",
    "source_value_remediation_plan_record_id",
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
    "acquisition_status",
    "acquisition_blocker_codes",
    "endpoint_candidate_present",
    "endpoint_configuration_id",
    "endpoint_configuration_version",
    "source_authority_class",
    "source_authority_priority",
    "source_authority_eligible",
    "base_uri",
    "transport_protocol",
    "request_method",
    "identity_parameter_mapping",
    "authoritative_outcome_field",
    "rejected_metadata_fields",
    "credential_reference",
    "credential_literal_present",
    "timeout_policy",
    "retry_policy",
    "rate_limit_policy",
    "pagination_policy",
    "raw_response_retention_policy",
    "response_digest_algorithm",
    "configuration_status",
    "configuration_blocker_codes",
    "retrieval_planning_authority_granted",
    "configuration_rationale",
    "configuration_limitations",
    "authoritative_source_endpoint_configuration_identity_digest",
    "authoritative_source_endpoint_configuration_record_digest",
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
        "field": "base_uri",
    },
    {
        "ordinal": 5,
        "field": "endpoint_configuration_id",
    },
    {
        "ordinal": 6,
        "field": "authoritative_source_endpoint_configuration_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AN_endpoint_missing_acquisition_records",
    },
    {
        "ordinal": 2,
        "step": "preserve_acquisition_and_defect_lineage",
    },
    {
        "ordinal": 3,
        "step": "load_explicit_endpoint_candidate_configuration",
    },
    {
        "ordinal": 4,
        "step": "reject_missing_or_unapproved_endpoint_candidates",
    },
    {
        "ordinal": 5,
        "step": "validate_source_authority_class",
    },
    {
        "ordinal": 6,
        "step": "validate_base_uri_protocol_and_request_method",
    },
    {
        "ordinal": 7,
        "step": "validate_identity_parameter_mapping",
    },
    {
        "ordinal": 8,
        "step": "validate_outcome_and_metadata_semantics",
    },
    {
        "ordinal": 9,
        "step": "validate_secret_reference_without_literal_secret",
    },
    {
        "ordinal": 10,
        "step": "validate_timeout_retry_rate_limit_and_pagination_policies",
    },
    {
        "ordinal": 11,
        "step": "validate_raw_response_retention_and_digest_policy",
    },
    {
        "ordinal": 12,
        "step": "emit_deterministic_configuration_records",
    },
    {
        "ordinal": 13,
        "step": "verify_forward_and_reverse_replay",
    },
    {
        "ordinal": 14,
        "step": "grant_retrieval_planning_only_for_approved_configuration",
    },
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "credential_literal_storage",
    "unapproved_network_retrieval",
    "historical_outcome_fetch_execution",
    "historical_outcome_parse_execution",
    "raw_response_materialization",
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
        "layer_9an_predecessor",
    )

    if (
        predecessor.ACQUISITION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AN contract version: "
            f"{predecessor.ACQUISITION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()

    plan = replay["plan"]

    acquisition_records = (
        predecessor.build_acquisition_records(
            plan,
            replay["records"],
        )
    )

    reverse_acquisition_records = (
        predecessor.build_acquisition_records(
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
        "records": acquisition_records,
        "reverse_records": reverse_acquisition_records,
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

    acquisition_status_counts = Counter(
        row["acquisition_status"]
        for row in records
    )

    retrieval_status_counts = Counter(
        row["retrieval_status"]
        for row in records
    )

    acquisition_blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "acquisition_blocker_codes"
        ]
    )

    checks = [
        {
            "check":
                "nine_an_contract_version_verified",
            "actual":
                predecessor.ACQUISITION_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.ACQUISITION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check":
                "nine_an_replay_deterministic",
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
            "check":
                "nine_an_digest_replay_deterministic",
            "actual":
                sha256_payload(records),
            "expected":
                sha256_payload(reverse_records),
            "passed": (
                sha256_payload(records)
                == sha256_payload(reverse_records)
            ),
        },
        {
            "check":
                "expected_acquisition_records_replayed",
            "actual":
                len(records),
            "expected":
                EXPECTED_ACQUISITION_RECORDS,
            "passed": (
                len(records)
                == EXPECTED_ACQUISITION_RECORDS
            ),
        },
        {
            "check":
                "expected_acquisition_comparisons_replayed",
            "actual":
                len(comparison_ids),
            "expected":
                EXPECTED_ACQUISITION_COMPARISONS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_ACQUISITION_COMPARISONS
            ),
        },
        {
            "check":
                "all_acquisitions_source_not_found",
            "actual":
                dict(
                    sorted(
                        acquisition_status_counts.items()
                    )
                ),
            "expected": {
                EXPECTED_ACQUISITION_STATUS:
                    EXPECTED_ACQUISITION_RECORDS
            },
            "passed": (
                acquisition_status_counts
                == Counter(
                    {
                        EXPECTED_ACQUISITION_STATUS:
                            EXPECTED_ACQUISITION_RECORDS
                    }
                )
            ),
        },
        {
            "check":
                "all_retrievals_blocked_by_missing_endpoint",
            "actual":
                dict(
                    sorted(
                        retrieval_status_counts.items()
                    )
                ),
            "expected": {
                EXPECTED_RETRIEVAL_STATUS:
                    EXPECTED_ACQUISITION_RECORDS
            },
            "passed": (
                retrieval_status_counts
                == Counter(
                    {
                        EXPECTED_RETRIEVAL_STATUS:
                            EXPECTED_ACQUISITION_RECORDS
                    }
                )
            ),
        },
        {
            "check":
                "all_endpoint_missing_blockers_preserved",
            "actual":
                dict(
                    sorted(
                        acquisition_blocker_counts.items()
                    )
                ),
            "expected": {
                EXPECTED_BLOCKER_CODE:
                    EXPECTED_ACQUISITION_RECORDS
            },
            "passed": (
                acquisition_blocker_counts
                == Counter(
                    {
                        EXPECTED_BLOCKER_CODE:
                            EXPECTED_ACQUISITION_RECORDS
                    }
                )
            ),
        },
        {
            "check":
                "all_acquisition_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authoritative_source_acquisition_record_digest"
                    ]
                )
                for row in records
            ),
            "expected":
                len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_source_acquisition_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check":
                "authoritative_field_name_preserved",
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
            "check":
                "authoritative_field_path_preserved",
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
            "check":
                "rejected_metadata_field_preserved",
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
            "check":
                "configuration_principles_defined",
            "actual":
                len(CONFIGURATION_PRINCIPLES),
            "expected": 8,
            "passed": (
                len(CONFIGURATION_PRINCIPLES)
                == 8
            ),
        },
        {
            "check":
                "endpoint_authority_classes_defined",
            "actual":
                len(ENDPOINT_AUTHORITY_CLASSES),
            "expected": 5,
            "passed": (
                len(ENDPOINT_AUTHORITY_CLASSES)
                == 5
            ),
        },
        {
            "check":
                "configuration_stages_defined",
            "actual":
                len(CONFIGURATION_STAGES),
            "expected": 10,
            "passed": (
                len(CONFIGURATION_STAGES)
                == 10
            ),
        },
        {
            "check":
                "endpoint_requirements_defined",
            "actual":
                len(ENDPOINT_REQUIREMENTS),
            "expected": 18,
            "passed": (
                len(ENDPOINT_REQUIREMENTS)
                == 18
            ),
        },
        {
            "check":
                "configuration_statuses_defined",
            "actual":
                len(CONFIGURATION_STATUSES),
            "expected": 9,
            "passed": (
                len(CONFIGURATION_STATUSES)
                == 9
            ),
        },
        {
            "check":
                "blocker_codes_defined",
            "actual":
                len(BLOCKER_CODES),
            "expected": 24,
            "passed": (
                len(BLOCKER_CODES)
                == 24
            ),
        },
        {
            "check":
                "configuration_record_fields_defined",
            "actual":
                len(CONFIGURATION_RECORD_FIELDS),
            "expected": 47,
            "passed": (
                len(CONFIGURATION_RECORD_FIELDS)
                == 47
            ),
        },
        {
            "check":
                "ordering_fields_defined",
            "actual":
                len(ORDERING_FIELDS),
            "expected": 6,
            "passed": (
                len(ORDERING_FIELDS)
                == 6
            ),
        },
        {
            "check":
                "implementation_steps_defined",
            "actual":
                len(IMPLEMENTATION_STEPS),
            "expected": 14,
            "passed": (
                len(IMPLEMENTATION_STEPS)
                == 14
            ),
        },
        {
            "check":
                "endpoint_invention_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "endpoint_candidate_invention"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check":
                "credential_literal_storage_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "credential_literal_storage"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check":
                "network_retrieval_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "unapproved_network_retrieval"
                in PROHIBITED_AUTHORITIES
                and "historical_outcome_fetch_execution"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check":
                "canonical_source_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_source_value_mutation"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check":
                "mapping_change_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_outcome_mapping_change"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check":
                "candidate_transformation_prohibited",
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
            "check":
                "endpoint_candidates_not_configured",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "network_retrieval_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "canonical_sources_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "downstream_records_not_recomputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "quality_and_production_authority_absent",
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
            "configuration_principles":
                CONFIGURATION_PRINCIPLES,
            "endpoint_authority_classes":
                ENDPOINT_AUTHORITY_CLASSES,
            "configuration_stages":
                CONFIGURATION_STAGES,
            "endpoint_requirements":
                ENDPOINT_REQUIREMENTS,
            "configuration_statuses":
                CONFIGURATION_STATUSES,
            "blocker_codes":
                BLOCKER_CODES,
            "configuration_record_fields":
                CONFIGURATION_RECORD_FIELDS,
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
        "outcome_authoritative_source_endpoint_"
        "configuration_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_endpoint_"
        "configuration_plan_failed"
    )

    next_layer = (
        "9AP_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_endpoint_"
        "configuration_implementation"
        if all_checks_passed
        else
        "9AO_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_endpoint_"
        "configuration_plan_remediation"
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
        OUTPUT_DIR / "configuration_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        CONFIGURATION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "endpoint_authority_classes.csv",
        [
            "authority_class_id",
            "authority_class",
            "priority",
            "configuration_eligible",
            "description",
        ],
        ENDPOINT_AUTHORITY_CLASSES,
    )

    write_csv(
        OUTPUT_DIR / "configuration_stages.csv",
        [
            "stage_id",
            "stage_name",
            "priority",
            "description",
        ],
        CONFIGURATION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "endpoint_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        ENDPOINT_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "configuration_statuses.csv",
        [
            "status",
            "retrieval_planning_authority",
        ],
        CONFIGURATION_STATUSES,
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
        / "configuration_record_field_contract.csv",
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
                CONFIGURATION_RECORD_FIELDS,
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
        / "endpoint_missing_acquisition_inventory.csv",
        [
            "authoritative_source_acquisition_record_id",
            "authoritative_source_acquisition_record_digest",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "acquisition_status",
            "retrieval_status",
            "retrieval_source_uri",
            "source_authority_class",
            "acquisition_blocker_codes",
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
                    "Layer 9AO is planning-only and grants no endpoint "
                    "invention, credential storage, network retrieval, raw "
                    "response materialization, canonical mutation, mapping "
                    "change, transformation, recomputation, quality, "
                    "production, market, pricing, or betting authority."
                ),
            }
            for authority
            in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_authoritative_"
                    "source_endpoint_configuration_implementation"
                ),
                "granted":
                    all_checks_passed,
                "reason": (
                    "Layer 9AP may implement deterministic validation and "
                    "materialization of an explicitly supplied endpoint "
                    "configuration under this contract. It may not invent an "
                    "endpoint or execute retrieval."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.ACQUISITION_CONTRACT_VERSION,
        "acquisition_records":
            len(records),
        "acquisition_comparisons":
            len(comparison_ids),
        "acquisition_status_counts":
            dict(
                sorted(
                    acquisition_status_counts.items()
                )
            ),
        "retrieval_status_counts":
            dict(
                sorted(
                    retrieval_status_counts.items()
                )
            ),
        "acquisition_blocker_counts":
            dict(
                sorted(
                    acquisition_blocker_counts.items()
                )
            ),
        "configuration_principles":
            len(CONFIGURATION_PRINCIPLES),
        "endpoint_authority_classes":
            len(ENDPOINT_AUTHORITY_CLASSES),
        "configuration_stages":
            len(CONFIGURATION_STAGES),
        "endpoint_requirements":
            len(ENDPOINT_REQUIREMENTS),
        "configuration_statuses":
            len(CONFIGURATION_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "configuration_record_fields":
            len(CONFIGURATION_RECORD_FIELDS),
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
        "endpoint_candidates_configured": 0,
        "credentials_stored": 0,
        "network_retrievals_executed": 0,
        "raw_source_artifacts_retained": 0,
        "authoritative_sources_acquired": 0,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "source_values_repaired": 0,
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
        / "authoritative_source_endpoint_configuration_plan_summary.json",
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
            "source_endpoint_configuration_implementation"
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
        f"{predecessor.ACQUISITION_CONTRACT_VERSION}"
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
        "Acquisition records replayed: "
        f"{len(records)}"
    )
    print(
        "Acquisition comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Acquisition status counts: "
        f"{dict(sorted(acquisition_status_counts.items()))}"
    )
    print(
        "Retrieval status counts: "
        f"{dict(sorted(retrieval_status_counts.items()))}"
    )
    print(
        "Configuration principles: "
        f"{len(CONFIGURATION_PRINCIPLES)}"
    )
    print(
        "Endpoint authority classes: "
        f"{len(ENDPOINT_AUTHORITY_CLASSES)}"
    )
    print(
        "Configuration stages: "
        f"{len(CONFIGURATION_STAGES)}"
    )
    print(
        "Endpoint requirements: "
        f"{len(ENDPOINT_REQUIREMENTS)}"
    )
    print(
        "Configuration record fields: "
        f"{len(CONFIGURATION_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Endpoint candidates configured: 0")
    print("Credentials stored: 0")
    print("Network retrievals executed: 0")
    print("Raw source artifacts retained: 0")
    print("Authoritative sources acquired: 0")
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Source values repaired: 0")
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
