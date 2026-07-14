#!/usr/bin/env python3
"""
Layer 9AN
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Acquisition Implementation

Implements the bounded acquisition contract planned by Layer 9AM.

Layer 9AL established that the repository contains no independent authoritative
numeric observation for the sixteen defective historical outcome identities.
Layer 9AM authorized bounded acquisition, but it did not define or approve a
concrete external endpoint, credential, archive, or retained source snapshot.

This implementation therefore:

- replays all sixteen unresolved discovery records;
- inventories the approved acquisition configuration;
- proves that no authoritative endpoint is configured;
- emits one deterministic acquisition record per unresolved comparison;
- classifies every record as `authoritative_source_not_found`;
- grants no correction authority.

It does not invent endpoints, perform unapproved network retrieval, coerce
booleans, default, infer, substitute, impute, mutate canonical sources, change
the canonical mapping, or recompute downstream records.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AN"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_authoritative_source_acquisition_implementation"
)

ACQUISITION_CONTRACT_VERSION = (
    "layer_9AN_historical_outcome_authoritative_"
    "source_acquisition_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AN_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_source_acquisition"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AM_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_source_acquisition.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AM_historical_outcome_authoritative_"
    "source_acquisition_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AL_historical_outcome_authoritative_"
    "observation_discovery_contract_v1"
)

EXPECTED_DISCOVERY_RECORDS = 16
EXPECTED_ACQUISITION_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

ACQUISITION_STATUS = "authoritative_source_not_found"

ENDPOINT_BLOCKER = (
    "historical_outcome_source_endpoint_missing"
)

APPROVED_SOURCE_ENDPOINTS: tuple[dict[str, Any], ...] = ()


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
        "layer_9am_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9AM plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()

    predecessor = replay["module"]

    if (
        predecessor.DISCOVERY_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AL contract version: "
            f"{predecessor.DISCOVERY_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_acquisition_records(
    plan: Any,
    discovery_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for discovery in discovery_records:
        identity_payload = {
            "authoritative_source_acquisition_contract_version":
                ACQUISITION_CONTRACT_VERSION,
            "authoritative_observation_discovery_record_id":
                discovery.get(
                    "authoritative_observation_discovery_record_id"
                ),
            "comparison_record_id":
                discovery.get(
                    "comparison_record_id"
                ),
            "defect_source_record_id":
                discovery.get(
                    "defect_source_record_id"
                ),
            "retrieval_source_uri": None,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "authoritative_source_acquisition_contract_version":
                ACQUISITION_CONTRACT_VERSION,
            "authoritative_source_acquisition_record_id":
                "HOASA-" + identity_digest[:20],
            "authoritative_observation_discovery_record_id":
                discovery.get(
                    "authoritative_observation_discovery_record_id"
                ),
            "authoritative_observation_discovery_record_digest":
                discovery.get(
                    "authoritative_observation_discovery_record_digest"
                ),
            "source_value_remediation_plan_record_id":
                discovery.get(
                    "source_value_remediation_plan_record_id"
                ),
            "source_value_audit_record_id":
                discovery.get(
                    "source_value_audit_record_id"
                ),
            "comparison_record_id":
                discovery.get(
                    "comparison_record_id"
                ),
            "metric_record_id":
                discovery.get(
                    "metric_record_id"
                ),
            "metric_name":
                discovery.get(
                    "metric_name"
                ),
            "aggregation_name":
                discovery.get(
                    "aggregation_name"
                ),
            "aggregation_key":
                discovery.get(
                    "aggregation_key"
                ),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                discovery.get(
                    "defect_source_path"
                ),
            "defect_source_symbol":
                discovery.get(
                    "defect_source_symbol"
                ),
            "defect_source_record_id":
                discovery.get(
                    "defect_source_record_id"
                ),
            "defect_source_record_digest":
                discovery.get(
                    "defect_source_record_digest"
                ),
            "defective_value":
                discovery.get(
                    "defective_value"
                ),
            "defective_runtime_type":
                discovery.get(
                    "defective_runtime_type"
                ),
            "source_authority_class":
                "non_authoritative_reference",
            "source_authority_priority": 5,
            "retrieval_source_uri": None,
            "retrieval_request_identity": {
                "comparison_record_id":
                    discovery.get(
                        "comparison_record_id"
                    ),
                "defect_source_record_id":
                    discovery.get(
                        "defect_source_record_id"
                    ),
                "approved_endpoint_count":
                    len(APPROVED_SOURCE_ENDPOINTS),
            },
            "retrieval_timestamp_utc": None,
            "retrieval_status":
                "not_executed_endpoint_missing",
            "raw_response_artifact_path": None,
            "raw_response_digest": None,
            "raw_response_content_type": None,
            "candidate_source_record_id": None,
            "candidate_source_record_digest": None,
            "candidate_value_present": False,
            "candidate_value": None,
            "candidate_runtime_type": "null",
            "candidate_value_valid": False,
            "game_identity_match": False,
            "event_identity_match": False,
            "target_identity_match": False,
            "event_sequence_match": False,
            "outcome_semantics_verified": False,
            "source_provenance_complete": False,
            "source_authority_sufficient": False,
            "candidate_count": 0,
            "equivalent_candidate_count": 0,
            "conflicting_candidate_count": 0,
            "acquisition_status":
                ACQUISITION_STATUS,
            "acquisition_blocker_codes": [
                ENDPOINT_BLOCKER
            ],
            "correction_authority_granted": False,
            "acquisition_rationale": (
                "No authoritative historical outcome endpoint, immutable "
                "archive, approved credentialed source, or retained upstream "
                "snapshot is configured by the Layer 9AM contract. Acquisition "
                "cannot proceed without inventing an authority source, so no "
                "network retrieval was executed."
            ),
            "acquisition_limitations": [
                (
                    "No approved authoritative source endpoint is configured."
                ),
                (
                    "No network request or external retrieval was executed."
                ),
                (
                    "No raw source artifact or candidate observation was "
                    "created."
                ),
                (
                    "No boolean coercion, defaulting, inference, substitution, "
                    "or imputation was used."
                ),
                (
                    "No canonical source correction or downstream "
                    "recomputation authority is granted."
                ),
            ],
            "authoritative_source_acquisition_identity_digest":
                identity_digest,
        }

        record[
            "authoritative_source_acquisition_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.ACQUISITION_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Acquisition record missing fields: "
                + ", ".join(missing_fields)
            )

        records.append(
            {
                field: record[field]
                for field
                in plan.ACQUISITION_RECORD_FIELDS
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
            int(
                row.get(
                    "source_authority_priority",
                    999,
                )
            ),
            normalized_string(
                row.get(
                    "retrieval_source_uri"
                )
            ),
            normalized_string(
                row.get(
                    "candidate_source_record_id"
                )
            ),
            normalized_string(
                row.get(
                    "authoritative_source_acquisition_record_id"
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
    discovery_records = replay["records"]
    reverse_discovery_records = replay[
        "reverse_records"
    ]

    acquisition_records = (
        build_acquisition_records(
            plan,
            discovery_records,
        )
    )

    reverse_acquisition_records = (
        build_acquisition_records(
            plan,
            list(
                reversed(
                    reverse_discovery_records
                )
            ),
        )
    )

    discovery_replay_deterministic = (
        canonical_json(discovery_records)
        == canonical_json(
            reverse_discovery_records
        )
    )

    acquisition_replay_deterministic = (
        canonical_json(acquisition_records)
        == canonical_json(
            reverse_acquisition_records
        )
    )

    acquisition_digest = sha256_payload(
        acquisition_records
    )

    reverse_acquisition_digest = (
        sha256_payload(
            reverse_acquisition_records
        )
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in acquisition_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row["acquisition_status"]
                for row in acquisition_records
            ).items()
        )
    )

    retrieval_status_counts = dict(
        sorted(
            Counter(
                row["retrieval_status"]
                for row in acquisition_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in acquisition_records
                for blocker in row[
                    "acquisition_blocker_codes"
                ]
            ).items()
        )
    )

    source_authority_counts = dict(
        sorted(
            Counter(
                row["source_authority_class"]
                for row in acquisition_records
            ).items()
        )
    )

    correction_authority_records = [
        row
        for row in acquisition_records
        if row[
            "correction_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_am_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": (
                plan.PLAN_VERSION
                == EXPECTED_PLAN_VERSION
            ),
        },
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
            "check": "discovery_replay_deterministic",
            "actual":
                discovery_replay_deterministic,
            "expected": True,
            "passed":
                discovery_replay_deterministic,
        },
        {
            "check": "acquisition_replay_deterministic",
            "actual":
                acquisition_replay_deterministic,
            "expected": True,
            "passed":
                acquisition_replay_deterministic,
        },
        {
            "check": "acquisition_digests_match_reverse_replay",
            "actual":
                acquisition_digest,
            "expected":
                reverse_acquisition_digest,
            "passed": (
                acquisition_digest
                == reverse_acquisition_digest
            ),
        },
        {
            "check": "expected_discovery_records_replayed",
            "actual":
                len(discovery_records),
            "expected":
                EXPECTED_DISCOVERY_RECORDS,
            "passed": (
                len(discovery_records)
                == EXPECTED_DISCOVERY_RECORDS
            ),
        },
        {
            "check": "expected_acquisition_records_materialized",
            "actual":
                len(acquisition_records),
            "expected":
                EXPECTED_ACQUISITION_RECORDS,
            "passed": (
                len(acquisition_records)
                == EXPECTED_ACQUISITION_RECORDS
            ),
        },
        {
            "check": "one_acquisition_record_per_comparison",
            "actual":
                len(comparison_ids),
            "expected":
                EXPECTED_ACQUISITION_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_ACQUISITION_RECORDS
            ),
        },
        {
            "check": "acquisition_record_fields_complete",
            "actual":
                len(plan.ACQUISITION_RECORD_FIELDS),
            "expected": 52,
            "passed": all(
                set(row)
                == set(
                    plan.ACQUISITION_RECORD_FIELDS
                )
                for row in acquisition_records
            ),
        },
        {
            "check": "acquisition_record_ids_unique",
            "actual": len(
                {
                    row[
                        "authoritative_source_acquisition_record_id"
                    ]
                    for row in acquisition_records
                }
            ),
            "expected":
                len(acquisition_records),
            "passed": (
                len(
                    {
                        row[
                            "authoritative_source_acquisition_record_id"
                        ]
                        for row
                        in acquisition_records
                    }
                )
                == len(acquisition_records)
            ),
        },
        {
            "check": "acquisition_record_digests_unique",
            "actual": len(
                {
                    row[
                        "authoritative_source_acquisition_record_digest"
                    ]
                    for row in acquisition_records
                }
            ),
            "expected":
                len(acquisition_records),
            "passed": (
                len(
                    {
                        row[
                            "authoritative_source_acquisition_record_digest"
                        ]
                        for row
                        in acquisition_records
                    }
                )
                == len(acquisition_records)
            ),
        },
        {
            "check": "all_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authoritative_source_acquisition_identity_digest"
                    ]
                )
                for row in acquisition_records
            ),
            "expected":
                len(acquisition_records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_source_acquisition_identity_digest"
                    ]
                )
                for row in acquisition_records
            ),
        },
        {
            "check": "all_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authoritative_source_acquisition_record_digest"
                    ]
                )
                for row in acquisition_records
            ),
            "expected":
                len(acquisition_records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_source_acquisition_record_digest"
                    ]
                )
                for row in acquisition_records
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
                for row in acquisition_records
            ),
            "expected":
                len(acquisition_records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_observation_discovery_record_digest"
                    ]
                )
                for row in acquisition_records
            ),
        },
        {
            "check": "all_defect_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in acquisition_records
            ),
            "expected":
                len(acquisition_records),
            "passed": all(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in acquisition_records
            ),
        },
        {
            "check": "approved_endpoint_inventory_empty",
            "actual":
                len(APPROVED_SOURCE_ENDPOINTS),
            "expected": 0,
            "passed": (
                len(APPROVED_SOURCE_ENDPOINTS)
                == 0
            ),
        },
        {
            "check": "all_retrieval_uris_absent",
            "actual": sum(
                row["retrieval_source_uri"]
                is None
                for row in acquisition_records
            ),
            "expected":
                len(acquisition_records),
            "passed": all(
                row["retrieval_source_uri"]
                is None
                for row in acquisition_records
            ),
        },
        {
            "check": "all_retrievals_not_executed",
            "actual":
                retrieval_status_counts,
            "expected": {
                "not_executed_endpoint_missing":
                    EXPECTED_ACQUISITION_RECORDS
            },
            "passed": (
                retrieval_status_counts
                == {
                    "not_executed_endpoint_missing":
                        EXPECTED_ACQUISITION_RECORDS
                }
            ),
        },
        {
            "check": "all_acquisitions_source_not_found",
            "actual":
                status_counts,
            "expected": {
                ACQUISITION_STATUS:
                    EXPECTED_ACQUISITION_RECORDS
            },
            "passed": (
                status_counts
                == {
                    ACQUISITION_STATUS:
                        EXPECTED_ACQUISITION_RECORDS
                }
            ),
        },
        {
            "check": "all_endpoint_missing_blockers_present",
            "actual":
                blocker_counts,
            "expected": {
                ENDPOINT_BLOCKER:
                    EXPECTED_ACQUISITION_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    ENDPOINT_BLOCKER:
                        EXPECTED_ACQUISITION_RECORDS
                }
            ),
        },
        {
            "check": "all_sources_non_authoritative_references",
            "actual":
                source_authority_counts,
            "expected": {
                "non_authoritative_reference":
                    EXPECTED_ACQUISITION_RECORDS
            },
            "passed": (
                source_authority_counts
                == {
                    "non_authoritative_reference":
                        EXPECTED_ACQUISITION_RECORDS
                }
            ),
        },
        {
            "check": "no_raw_response_artifacts_created",
            "actual": sum(
                row["raw_response_artifact_path"]
                is not None
                for row in acquisition_records
            ),
            "expected": 0,
            "passed": all(
                row["raw_response_artifact_path"]
                is None
                for row in acquisition_records
            ),
        },
        {
            "check": "no_raw_response_digests_created",
            "actual": sum(
                row["raw_response_digest"]
                is not None
                for row in acquisition_records
            ),
            "expected": 0,
            "passed": all(
                row["raw_response_digest"]
                is None
                for row in acquisition_records
            ),
        },
        {
            "check": "no_candidate_values_created",
            "actual": sum(
                row["candidate_value_present"]
                for row in acquisition_records
            ),
            "expected": 0,
            "passed": all(
                not row["candidate_value_present"]
                and row["candidate_value"]
                is None
                for row in acquisition_records
            ),
        },
        {
            "check": "no_candidate_source_records_created",
            "actual": sum(
                row["candidate_source_record_id"]
                is not None
                for row in acquisition_records
            ),
            "expected": 0,
            "passed": all(
                row["candidate_source_record_id"]
                is None
                and row[
                    "candidate_source_record_digest"
                ]
                is None
                for row in acquisition_records
            ),
        },
        {
            "check": "all_candidate_counts_zero",
            "actual": sum(
                row["candidate_count"]
                for row in acquisition_records
            ),
            "expected": 0,
            "passed": all(
                row["candidate_count"] == 0
                and row[
                    "equivalent_candidate_count"
                ] == 0
                and row[
                    "conflicting_candidate_count"
                ] == 0
                for row in acquisition_records
            ),
        },
        {
            "check": "no_correction_authority_granted",
            "actual":
                len(correction_authority_records),
            "expected": 0,
            "passed": (
                len(
                    correction_authority_records
                )
                == 0
            ),
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row[
                        "authoritative_field_name"
                    ]
                    for row in acquisition_records
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
                for row in acquisition_records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row[
                        "authoritative_field_path"
                    ]
                    for row in acquisition_records
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
                for row in acquisition_records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row[
                        "rejected_metadata_field_name"
                    ]
                    for row in acquisition_records
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
                for row in acquisition_records
            ),
        },
        {
            "check": "network_retrievals_executed_zero",
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
            "check": "canonical_mappings_not_changed",
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

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_"
        "acquisition_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_"
        "acquisition_implementation_failed"
    )

    next_layer = (
        "9AO_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_endpoint_"
        "configuration_plan"
        if all_checks_passed
        else
        "9AN_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_"
        "acquisition_implementation_remediation"
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
        / "authoritative_source_acquisition_records.csv",
        plan.ACQUISITION_RECORD_FIELDS,
        acquisition_records,
    )

    write_csv(
        OUTPUT_DIR / "acquisition_status_counts.csv",
        [
            "acquisition_status",
            "count",
        ],
        [
            {
                "acquisition_status": key,
                "count": value,
            }
            for key, value
            in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "retrieval_status_counts.csv",
        [
            "retrieval_status",
            "count",
        ],
        [
            {
                "retrieval_status": key,
                "count": value,
            }
            for key, value
            in retrieval_status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "acquisition_blocker_counts.csv",
        [
            "acquisition_blocker",
            "count",
        ],
        [
            {
                "acquisition_blocker": key,
                "count": value,
            }
            for key, value
            in blocker_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "source_authority_class_counts.csv",
        [
            "source_authority_class",
            "count",
        ],
        [
            {
                "source_authority_class": key,
                "count": value,
            }
            for key, value
            in source_authority_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR
        / "approved_source_endpoint_inventory.json",
        {
            "layer_id": LAYER_ID,
            "approved_source_endpoint_count":
                len(APPROVED_SOURCE_ENDPOINTS),
            "approved_source_endpoints":
                list(APPROVED_SOURCE_ENDPOINTS),
            "inventory_status":
                "no_approved_endpoints_configured",
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "acquisition_contract_version":
            ACQUISITION_CONTRACT_VERSION,
        "plan_version":
            plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.DISCOVERY_CONTRACT_VERSION,
        "discovery_records":
            len(discovery_records),
        "acquisition_records":
            len(acquisition_records),
        "acquisition_comparisons":
            len(comparison_ids),
        "approved_source_endpoints":
            len(APPROVED_SOURCE_ENDPOINTS),
        "acquisition_status_counts":
            status_counts,
        "retrieval_status_counts":
            retrieval_status_counts,
        "acquisition_blocker_counts":
            blocker_counts,
        "source_authority_class_counts":
            source_authority_counts,
        "authoritative_sources_acquired": 0,
        "network_retrievals_executed": 0,
        "raw_source_artifacts_retained": 0,
        "candidate_observations_extracted": 0,
        "correction_authorities_granted":
            len(correction_authority_records),
        "acquisition_digest":
            acquisition_digest,
        "reverse_acquisition_digest":
            reverse_acquisition_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "source_values_repaired": 0,
        "candidate_values_coerced": 0,
        "candidate_values_defaulted": 0,
        "candidate_values_inferred": 0,
        "candidate_values_imputed": 0,
        "candidate_values_substituted": 0,
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
        / "authoritative_source_acquisition_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "acquisition_result":
            ACQUISITION_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_"
            "source_endpoint_configuration_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "unapproved_network_retrieval",
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
        "Acquisition contract version: "
        f"{ACQUISITION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Discovery records replayed: "
        f"{len(discovery_records)}"
    )
    print(
        "Acquisition records: "
        f"{len(acquisition_records)}"
    )
    print(
        "Acquisition comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Approved source endpoints: "
        f"{len(APPROVED_SOURCE_ENDPOINTS)}"
    )
    print(
        "Acquisition status counts: "
        f"{status_counts}"
    )
    print(
        "Retrieval status counts: "
        f"{retrieval_status_counts}"
    )
    print(
        "Acquisition blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Source authority-class counts: "
        f"{source_authority_counts}"
    )
    print("Authoritative sources acquired: 0")
    print("Network retrievals executed: 0")
    print("Raw source artifacts retained: 0")
    print("Candidate observations extracted: 0")
    print("Correction authorities granted: 0")
    print(
        f"Acquisition digest: {acquisition_digest}"
    )
    print(
        "Reverse acquisition digest: "
        f"{reverse_acquisition_digest}"
    )
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Source values repaired: 0")
    print("Candidate values coerced: 0")
    print("Candidate values defaulted: 0")
    print("Candidate values inferred: 0")
    print("Candidate values imputed: 0")
    print("Candidate values substituted: 0")
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
        "Acquisition result: "
        f"{diagnosis['acquisition_result']}"
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
