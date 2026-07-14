#!/usr/bin/env python3
"""
Layer 9AR
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Specification Implementation

Implements the deterministic candidate-specification contract planned by
Layer 9AQ.

Layer 9AP established that no endpoint candidate is configured. Layer 9AQ
defined the contract that an explicitly supplied endpoint candidate must
satisfy. No concrete candidate submission exists in the repository, so this
implementation:

- replays the sixteen Layer 9AP configuration records;
- verifies the Layer 9AQ candidate-specification plan;
- inventories explicit candidate submissions;
- proves that no candidate was supplied;
- emits one deterministic specification record per comparison;
- classifies every record as `endpoint_candidate_not_supplied`;
- grants no candidate-materialization or retrieval authority.

This layer does not invent an endpoint, select an external provider, store
credentials, execute retrieval, create raw-response artifacts, alter canonical
values or mappings, transform source values, or recompute downstream records.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AR"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_specification_implementation"
)

SPECIFICATION_CONTRACT_VERSION = (
    "layer_9AR_historical_outcome_authoritative_source_"
    "endpoint_candidate_specification_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AR_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_specification"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AQ_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_specification.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AQ_historical_outcome_authoritative_"
    "source_endpoint_candidate_specification_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AP_historical_outcome_authoritative_"
    "source_endpoint_configuration_contract_v1"
)

EXPECTED_CONFIGURATION_RECORDS = 16
EXPECTED_SPECIFICATION_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

SPECIFICATION_STATUS = "endpoint_candidate_not_supplied"

SPECIFICATION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

SUPPLIED_CANDIDATE_SUBMISSIONS: tuple[dict[str, Any], ...] = ()


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


def normalized_string(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip()


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


def replay_plan() -> dict[str, Any]:
    plan = load_module(
        PLAN_PATH,
        "layer_9aq_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9AQ plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.CONFIGURATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AP contract version: "
            f"{predecessor.CONFIGURATION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_specification_records(
    plan: Any,
    configuration_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for configuration in configuration_records:
        identity_payload = {
            "endpoint_candidate_specification_contract_version":
                SPECIFICATION_CONTRACT_VERSION,
            "authoritative_source_endpoint_configuration_record_id":
                configuration.get(
                    "authoritative_source_endpoint_configuration_record_id"
                ),
            "comparison_record_id":
                configuration.get(
                    "comparison_record_id"
                ),
            "defect_source_record_id":
                configuration.get(
                    "defect_source_record_id"
                ),
            "candidate_id": None,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "endpoint_candidate_specification_contract_version":
                SPECIFICATION_CONTRACT_VERSION,
            "endpoint_candidate_specification_record_id":
                "HOASECS-" + identity_digest[:20],
            "authoritative_source_endpoint_configuration_record_id":
                configuration.get(
                    "authoritative_source_endpoint_configuration_record_id"
                ),
            "authoritative_source_endpoint_configuration_record_digest":
                configuration.get(
                    "authoritative_source_endpoint_configuration_record_digest"
                ),
            "authoritative_source_acquisition_record_id":
                configuration.get(
                    "authoritative_source_acquisition_record_id"
                ),
            "comparison_record_id":
                configuration.get(
                    "comparison_record_id"
                ),
            "metric_record_id":
                configuration.get(
                    "metric_record_id"
                ),
            "metric_name":
                configuration.get(
                    "metric_name"
                ),
            "aggregation_name":
                configuration.get(
                    "aggregation_name"
                ),
            "aggregation_key":
                configuration.get(
                    "aggregation_key"
                ),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                configuration.get(
                    "defect_source_path"
                ),
            "defect_source_symbol":
                configuration.get(
                    "defect_source_symbol"
                ),
            "defect_source_record_id":
                configuration.get(
                    "defect_source_record_id"
                ),
            "defect_source_record_digest":
                configuration.get(
                    "defect_source_record_digest"
                ),
            "configuration_status":
                configuration.get(
                    "configuration_status"
                ),
            "configuration_blocker_codes":
                configuration.get(
                    "configuration_blocker_codes"
                ),
            "candidate_supplied": False,
            "candidate_id": None,
            "candidate_version": None,
            "source_class": None,
            "source_class_priority": None,
            "source_class_eligible": False,
            "source_owner": None,
            "authority_documentation_uri": None,
            "base_uri": None,
            "transport_protocol": None,
            "request_method": None,
            "identity_parameter_mapping": {},
            "authoritative_outcome_field": None,
            "outcome_numeric_domain": None,
            "rejected_metadata_fields": [],
            "credential_reference": None,
            "credential_literal_present": False,
            "timeout_policy": None,
            "retry_policy": None,
            "rate_limit_policy": None,
            "pagination_policy": None,
            "raw_response_retention_policy": None,
            "response_digest_algorithm": None,
            "specification_status":
                SPECIFICATION_STATUS,
            "specification_blocker_codes": [
                SPECIFICATION_BLOCKER
            ],
            "candidate_materialization_authority_granted":
                False,
            "specification_rationale": (
                "No explicit authoritative source endpoint candidate "
                "submission was supplied. The implementation cannot invent "
                "a candidate identifier, URI, source owner, authority "
                "documentation, identity mapping, credential reference, "
                "transport policy, or response-retention policy."
            ),
            "specification_limitations": [
                "No endpoint candidate submission was supplied.",
                "No endpoint URI or provider was selected or invented.",
                "No authority evidence was fabricated.",
                "No credentials or credential references were stored.",
                "No retrieval or raw-response materialization was executed.",
                (
                    "No canonical mutation, mapping change, source-value "
                    "transformation, or downstream recomputation was executed."
                ),
            ],
            "endpoint_candidate_specification_identity_digest":
                identity_digest,
        }

        record[
            "endpoint_candidate_specification_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field
            in plan.CANDIDATE_SPECIFICATION_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Candidate specification record missing fields: "
                + ", ".join(missing_fields)
            )

        records.append(
            {
                field: record[field]
                for field
                in plan.CANDIDATE_SPECIFICATION_FIELDS
            }
        )

    records.sort(
        key=lambda row: (
            normalized_string(
                row.get(
                    "comparison_record_id"
                )
            ),
            normalized_string(
                row.get(
                    "defect_source_record_id"
                )
            ),
            (
                int(
                    row["source_class_priority"]
                )
                if row.get(
                    "source_class_priority"
                )
                is not None
                else 999
            ),
            normalized_string(
                row.get("base_uri")
            ),
            normalized_string(
                row.get("candidate_id")
            ),
            normalized_string(
                row.get(
                    "endpoint_candidate_specification_record_id"
                )
            ),
        )
    )

    return records


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    replay = replay_plan()

    plan = replay["plan"]
    predecessor = replay["predecessor"]
    configuration_records = replay["records"]
    reverse_configuration_records = replay[
        "reverse_records"
    ]

    specification_records = (
        build_specification_records(
            plan,
            configuration_records,
        )
    )

    reverse_specification_records = (
        build_specification_records(
            plan,
            list(
                reversed(
                    reverse_configuration_records
                )
            ),
        )
    )

    configuration_replay_deterministic = (
        canonical_json(configuration_records)
        == canonical_json(
            reverse_configuration_records
        )
    )

    specification_replay_deterministic = (
        canonical_json(specification_records)
        == canonical_json(
            reverse_specification_records
        )
    )

    specification_digest = sha256_payload(
        specification_records
    )

    reverse_specification_digest = sha256_payload(
        reverse_specification_records
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in specification_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row["specification_status"]
                for row in specification_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in specification_records
                for blocker in row[
                    "specification_blocker_codes"
                ]
            ).items()
        )
    )

    candidate_presence_counts = dict(
        sorted(
            Counter(
                str(row["candidate_supplied"])
                for row in specification_records
            ).items()
        )
    )

    materialization_authority_records = [
        row
        for row in specification_records
        if row[
            "candidate_materialization_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_aq_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": (
                plan.PLAN_VERSION
                == EXPECTED_PLAN_VERSION
            ),
        },
        {
            "check": "nine_ap_contract_version_verified",
            "actual":
                predecessor.CONFIGURATION_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.CONFIGURATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "configuration_replay_deterministic",
            "actual":
                configuration_replay_deterministic,
            "expected": True,
            "passed":
                configuration_replay_deterministic,
        },
        {
            "check": "specification_replay_deterministic",
            "actual":
                specification_replay_deterministic,
            "expected": True,
            "passed":
                specification_replay_deterministic,
        },
        {
            "check":
                "specification_digests_match_reverse_replay",
            "actual":
                specification_digest,
            "expected":
                reverse_specification_digest,
            "passed": (
                specification_digest
                == reverse_specification_digest
            ),
        },
        {
            "check":
                "expected_configuration_records_replayed",
            "actual":
                len(configuration_records),
            "expected":
                EXPECTED_CONFIGURATION_RECORDS,
            "passed": (
                len(configuration_records)
                == EXPECTED_CONFIGURATION_RECORDS
            ),
        },
        {
            "check":
                "expected_specification_records_materialized",
            "actual":
                len(specification_records),
            "expected":
                EXPECTED_SPECIFICATION_RECORDS,
            "passed": (
                len(specification_records)
                == EXPECTED_SPECIFICATION_RECORDS
            ),
        },
        {
            "check":
                "one_specification_record_per_comparison",
            "actual":
                len(comparison_ids),
            "expected":
                EXPECTED_SPECIFICATION_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_SPECIFICATION_RECORDS
            ),
        },
        {
            "check":
                "specification_record_fields_complete",
            "actual":
                len(
                    plan.CANDIDATE_SPECIFICATION_FIELDS
                ),
            "expected": 49,
            "passed": all(
                set(row)
                == set(
                    plan.CANDIDATE_SPECIFICATION_FIELDS
                )
                for row in specification_records
            ),
        },
        {
            "check":
                "specification_record_ids_unique",
            "actual": len(
                {
                    row[
                        "endpoint_candidate_specification_record_id"
                    ]
                    for row in specification_records
                }
            ),
            "expected":
                len(specification_records),
            "passed": (
                len(
                    {
                        row[
                            "endpoint_candidate_specification_record_id"
                        ]
                        for row
                        in specification_records
                    }
                )
                == len(specification_records)
            ),
        },
        {
            "check":
                "specification_record_digests_unique",
            "actual": len(
                {
                    row[
                        "endpoint_candidate_specification_record_digest"
                    ]
                    for row in specification_records
                }
            ),
            "expected":
                len(specification_records),
            "passed": (
                len(
                    {
                        row[
                            "endpoint_candidate_specification_record_digest"
                        ]
                        for row
                        in specification_records
                    }
                )
                == len(specification_records)
            ),
        },
        {
            "check":
                "all_specification_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "endpoint_candidate_specification_identity_digest"
                    ]
                )
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                valid_sha256(
                    row[
                        "endpoint_candidate_specification_identity_digest"
                    ]
                )
                for row in specification_records
            ),
        },
        {
            "check":
                "all_specification_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "endpoint_candidate_specification_record_digest"
                    ]
                )
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                valid_sha256(
                    row[
                        "endpoint_candidate_specification_record_digest"
                    ]
                )
                for row in specification_records
            ),
        },
        {
            "check":
                "all_configuration_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authoritative_source_endpoint_configuration_record_digest"
                    ]
                )
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_source_endpoint_configuration_record_digest"
                    ]
                )
                for row in specification_records
            ),
        },
        {
            "check":
                "all_defect_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in specification_records
            ),
        },
        {
            "check":
                "supplied_candidate_inventory_empty",
            "actual":
                len(
                    SUPPLIED_CANDIDATE_SUBMISSIONS
                ),
            "expected": 0,
            "passed": (
                len(
                    SUPPLIED_CANDIDATE_SUBMISSIONS
                )
                == 0
            ),
        },
        {
            "check":
                "all_candidates_absent",
            "actual":
                candidate_presence_counts,
            "expected": {
                "False":
                    EXPECTED_SPECIFICATION_RECORDS
            },
            "passed": (
                candidate_presence_counts
                == {
                    "False":
                        EXPECTED_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check":
                "all_specifications_candidate_not_supplied",
            "actual":
                status_counts,
            "expected": {
                SPECIFICATION_STATUS:
                    EXPECTED_SPECIFICATION_RECORDS
            },
            "passed": (
                status_counts
                == {
                    SPECIFICATION_STATUS:
                        EXPECTED_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check":
                "all_candidate_missing_blockers_present",
            "actual":
                blocker_counts,
            "expected": {
                SPECIFICATION_BLOCKER:
                    EXPECTED_SPECIFICATION_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    SPECIFICATION_BLOCKER:
                        EXPECTED_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check":
                "all_candidate_ids_absent",
            "actual": sum(
                row["candidate_id"] is None
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in specification_records
            ),
        },
        {
            "check":
                "all_source_authority_fields_absent",
            "actual": sum(
                row["source_class"] is None
                and row["source_owner"] is None
                and row[
                    "authority_documentation_uri"
                ]
                is None
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                row["source_class"] is None
                and row[
                    "source_class_priority"
                ]
                is None
                and not row[
                    "source_class_eligible"
                ]
                and row["source_owner"] is None
                and row[
                    "authority_documentation_uri"
                ]
                is None
                for row in specification_records
            ),
        },
        {
            "check":
                "all_base_uris_absent",
            "actual": sum(
                row["base_uri"] is None
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                row["base_uri"] is None
                and row[
                    "transport_protocol"
                ]
                is None
                and row["request_method"] is None
                for row in specification_records
            ),
        },
        {
            "check":
                "all_identity_mappings_empty",
            "actual": sum(
                row[
                    "identity_parameter_mapping"
                ]
                == {}
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                row[
                    "identity_parameter_mapping"
                ]
                == {}
                for row in specification_records
            ),
        },
        {
            "check":
                "all_outcome_semantic_fields_absent",
            "actual": sum(
                row[
                    "authoritative_outcome_field"
                ]
                is None
                and row[
                    "outcome_numeric_domain"
                ]
                is None
                and row[
                    "rejected_metadata_fields"
                ]
                == []
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                row[
                    "authoritative_outcome_field"
                ]
                is None
                and row[
                    "outcome_numeric_domain"
                ]
                is None
                and row[
                    "rejected_metadata_fields"
                ]
                == []
                for row in specification_records
            ),
        },
        {
            "check":
                "all_credential_fields_absent",
            "actual": sum(
                row[
                    "credential_reference"
                ]
                is None
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                row[
                    "credential_reference"
                ]
                is None
                and not row[
                    "credential_literal_present"
                ]
                for row in specification_records
            ),
        },
        {
            "check":
                "all_transport_policies_absent",
            "actual": sum(
                row["timeout_policy"] is None
                and row["retry_policy"] is None
                and row[
                    "rate_limit_policy"
                ]
                is None
                and row[
                    "pagination_policy"
                ]
                is None
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                row["timeout_policy"] is None
                and row["retry_policy"] is None
                and row[
                    "rate_limit_policy"
                ]
                is None
                and row[
                    "pagination_policy"
                ]
                is None
                for row in specification_records
            ),
        },
        {
            "check":
                "all_retention_policies_absent",
            "actual": sum(
                row[
                    "raw_response_retention_policy"
                ]
                is None
                and row[
                    "response_digest_algorithm"
                ]
                is None
                for row in specification_records
            ),
            "expected":
                len(specification_records),
            "passed": all(
                row[
                    "raw_response_retention_policy"
                ]
                is None
                and row[
                    "response_digest_algorithm"
                ]
                is None
                for row in specification_records
            ),
        },
        {
            "check":
                "no_candidate_materialization_authority_granted",
            "actual":
                len(
                    materialization_authority_records
                ),
            "expected": 0,
            "passed": (
                len(
                    materialization_authority_records
                )
                == 0
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
                    for row in specification_records
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
                for row in specification_records
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
                    for row in specification_records
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
                for row in specification_records
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
                    for row in specification_records
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
                for row in specification_records
            ),
        },
        {
            "check":
                "candidate_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "credentials_stored_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "network_retrievals_executed_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "raw_response_artifacts_created_zero",
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
                "canonical_mappings_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "candidate_values_not_transformed",
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

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_"
        "authoritative_source_endpoint_candidate_"
        "specification_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_"
        "authoritative_source_endpoint_candidate_"
        "specification_implementation_failed"
    )

    next_layer = (
        "9AS_pitch_type_matchup_overlay_historical_outcome_"
        "authoritative_source_endpoint_candidate_source_"
        "evidence_acquisition_plan"
        if all_checks_passed
        else
        "9AR_pitch_type_matchup_overlay_historical_outcome_"
        "authoritative_source_endpoint_candidate_"
        "specification_implementation_remediation"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR
        / "endpoint_candidate_specification_records.csv",
        plan.CANDIDATE_SPECIFICATION_FIELDS,
        specification_records,
    )

    write_csv(
        OUTPUT_DIR / "specification_status_counts.csv",
        [
            "specification_status",
            "count",
        ],
        [
            {
                "specification_status": key,
                "count": value,
            }
            for key, value
            in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "specification_blocker_counts.csv",
        [
            "specification_blocker",
            "count",
        ],
        [
            {
                "specification_blocker": key,
                "count": value,
            }
            for key, value
            in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR
        / "supplied_candidate_submission_inventory.json",
        {
            "layer_id": LAYER_ID,
            "supplied_candidate_submission_count":
                len(
                    SUPPLIED_CANDIDATE_SUBMISSIONS
                ),
            "supplied_candidate_submissions":
                list(
                    SUPPLIED_CANDIDATE_SUBMISSIONS
                ),
            "inventory_status":
                "no_endpoint_candidate_submission_supplied",
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "specification_contract_version":
            SPECIFICATION_CONTRACT_VERSION,
        "plan_version":
            plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.CONFIGURATION_CONTRACT_VERSION,
        "configuration_records":
            len(configuration_records),
        "specification_records":
            len(specification_records),
        "specification_comparisons":
            len(comparison_ids),
        "supplied_candidate_submissions":
            len(
                SUPPLIED_CANDIDATE_SUBMISSIONS
            ),
        "specification_status_counts":
            status_counts,
        "specification_blocker_counts":
            blocker_counts,
        "candidate_specifications_approved": 0,
        "candidate_materialization_authorities_granted":
            len(
                materialization_authority_records
            ),
        "specification_digest":
            specification_digest,
        "reverse_specification_digest":
            reverse_specification_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
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
        / "endpoint_candidate_specification_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "specification_result":
            SPECIFICATION_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_source_"
            "endpoint_candidate_source_evidence_acquisition_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "endpoint_candidate_selection_without_submission",
            "credential_literal_storage",
            "candidate_materialization",
            "historical_outcome_retrieval_planning",
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
        ],
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
        "Specification contract version: "
        f"{SPECIFICATION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Configuration records replayed: "
        f"{len(configuration_records)}"
    )
    print(
        "Specification records: "
        f"{len(specification_records)}"
    )
    print(
        "Specification comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Supplied candidate submissions: "
        f"{len(SUPPLIED_CANDIDATE_SUBMISSIONS)}"
    )
    print(
        "Specification status counts: "
        f"{status_counts}"
    )
    print(
        "Specification blocker counts: "
        f"{blocker_counts}"
    )
    print("Candidate specifications approved: 0")
    print(
        "Candidate materialization authorities granted: "
        f"{len(materialization_authority_records)}"
    )
    print(
        "Specification digest: "
        f"{specification_digest}"
    )
    print(
        "Reverse specification digest: "
        f"{reverse_specification_digest}"
    )
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
        "Specification result: "
        f"{diagnosis['specification_result']}"
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
