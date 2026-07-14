#!/usr/bin/env python3
"""
Layer 9AP
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Configuration Implementation

Implements the deterministic endpoint-configuration contract planned by
Layer 9AO.

Layer 9AN established that no authoritative source endpoint is configured.
Layer 9AO defined how an explicitly supplied endpoint candidate must be
validated, but it did not supply or authorize a concrete endpoint candidate.

This implementation therefore:

- replays all sixteen endpoint-missing acquisition records;
- verifies the Layer 9AO configuration contract;
- inventories the explicitly supplied endpoint-candidate configuration;
- proves that no endpoint candidate was supplied;
- emits one deterministic configuration record per comparison;
- classifies all records as `endpoint_candidate_not_supplied`;
- grants no retrieval-planning or retrieval-execution authority.

It does not invent an endpoint, store credentials, execute retrieval, create
raw responses, mutate canonical sources or mappings, transform values, or
recompute downstream records.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AP"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_authoritative_source_endpoint_configuration_implementation"
)

CONFIGURATION_CONTRACT_VERSION = (
    "layer_9AP_historical_outcome_authoritative_"
    "source_endpoint_configuration_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AP_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_source_endpoint_configuration"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AO_pitch_type_matchup_overlay_historical_"
    "outcome_authoritative_source_endpoint_configuration.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AO_historical_outcome_authoritative_"
    "source_endpoint_configuration_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AN_historical_outcome_authoritative_"
    "source_acquisition_contract_v1"
)

EXPECTED_ACQUISITION_RECORDS = 16
EXPECTED_CONFIGURATION_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

CONFIGURATION_STATUS = "endpoint_candidate_not_supplied"

CONFIGURATION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

SUPPLIED_ENDPOINT_CANDIDATES: tuple[dict[str, Any], ...] = ()


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
        "layer_9ao_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9AO plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()

    predecessor = replay["module"]

    if (
        predecessor.ACQUISITION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AN contract version: "
            f"{predecessor.ACQUISITION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_configuration_records(
    plan: Any,
    acquisition_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for acquisition in acquisition_records:
        identity_payload = {
            "authoritative_source_endpoint_configuration_contract_version":
                CONFIGURATION_CONTRACT_VERSION,
            "authoritative_source_acquisition_record_id":
                acquisition.get(
                    "authoritative_source_acquisition_record_id"
                ),
            "comparison_record_id":
                acquisition.get(
                    "comparison_record_id"
                ),
            "defect_source_record_id":
                acquisition.get(
                    "defect_source_record_id"
                ),
            "endpoint_configuration_id": None,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "authoritative_source_endpoint_configuration_contract_version":
                CONFIGURATION_CONTRACT_VERSION,
            "authoritative_source_endpoint_configuration_record_id":
                "HOASEC-" + identity_digest[:20],
            "authoritative_source_acquisition_record_id":
                acquisition.get(
                    "authoritative_source_acquisition_record_id"
                ),
            "authoritative_source_acquisition_record_digest":
                acquisition.get(
                    "authoritative_source_acquisition_record_digest"
                ),
            "authoritative_observation_discovery_record_id":
                acquisition.get(
                    "authoritative_observation_discovery_record_id"
                ),
            "source_value_remediation_plan_record_id":
                acquisition.get(
                    "source_value_remediation_plan_record_id"
                ),
            "comparison_record_id":
                acquisition.get(
                    "comparison_record_id"
                ),
            "metric_record_id":
                acquisition.get(
                    "metric_record_id"
                ),
            "metric_name":
                acquisition.get(
                    "metric_name"
                ),
            "aggregation_name":
                acquisition.get(
                    "aggregation_name"
                ),
            "aggregation_key":
                acquisition.get(
                    "aggregation_key"
                ),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                acquisition.get(
                    "defect_source_path"
                ),
            "defect_source_symbol":
                acquisition.get(
                    "defect_source_symbol"
                ),
            "defect_source_record_id":
                acquisition.get(
                    "defect_source_record_id"
                ),
            "defect_source_record_digest":
                acquisition.get(
                    "defect_source_record_digest"
                ),
            "acquisition_status":
                acquisition.get(
                    "acquisition_status"
                ),
            "acquisition_blocker_codes":
                acquisition.get(
                    "acquisition_blocker_codes"
                ),
            "endpoint_candidate_present": False,
            "endpoint_configuration_id": None,
            "endpoint_configuration_version": None,
            "source_authority_class": None,
            "source_authority_priority": None,
            "source_authority_eligible": False,
            "base_uri": None,
            "transport_protocol": None,
            "request_method": None,
            "identity_parameter_mapping": {},
            "authoritative_outcome_field": None,
            "rejected_metadata_fields": [],
            "credential_reference": None,
            "credential_literal_present": False,
            "timeout_policy": None,
            "retry_policy": None,
            "rate_limit_policy": None,
            "pagination_policy": None,
            "raw_response_retention_policy": None,
            "response_digest_algorithm": None,
            "configuration_status":
                CONFIGURATION_STATUS,
            "configuration_blocker_codes": [
                CONFIGURATION_BLOCKER
            ],
            "retrieval_planning_authority_granted": False,
            "configuration_rationale": (
                "No explicit authoritative source endpoint candidate was "
                "supplied by the Layer 9AO plan or repository configuration. "
                "The implementation cannot invent a URI, authority class, "
                "identity mapping, credential reference, transport policy, "
                "or response-retention policy."
            ),
            "configuration_limitations": [
                (
                    "No endpoint candidate configuration was supplied."
                ),
                (
                    "No endpoint URI or source authority was invented."
                ),
                (
                    "No credential literal or credential reference was stored."
                ),
                (
                    "No network retrieval or raw-response materialization was "
                    "executed."
                ),
                (
                    "No canonical mutation, mapping change, value "
                    "transformation, or downstream recomputation was executed."
                ),
            ],
            "authoritative_source_endpoint_configuration_identity_digest":
                identity_digest,
        }

        record[
            "authoritative_source_endpoint_configuration_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.CONFIGURATION_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Configuration record missing fields: "
                + ", ".join(missing_fields)
            )

        records.append(
            {
                field: record[field]
                for field
                in plan.CONFIGURATION_RECORD_FIELDS
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
                    row[
                        "source_authority_priority"
                    ]
                )
                if row.get(
                    "source_authority_priority"
                )
                is not None
                else 999
            ),
            normalized_string(
                row.get("base_uri")
            ),
            normalized_string(
                row.get(
                    "endpoint_configuration_id"
                )
            ),
            normalized_string(
                row.get(
                    "authoritative_source_endpoint_configuration_record_id"
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
    acquisition_records = replay["records"]
    reverse_acquisition_records = replay[
        "reverse_records"
    ]

    configuration_records = (
        build_configuration_records(
            plan,
            acquisition_records,
        )
    )

    reverse_configuration_records = (
        build_configuration_records(
            plan,
            list(
                reversed(
                    reverse_acquisition_records
                )
            ),
        )
    )

    acquisition_replay_deterministic = (
        canonical_json(acquisition_records)
        == canonical_json(
            reverse_acquisition_records
        )
    )

    configuration_replay_deterministic = (
        canonical_json(configuration_records)
        == canonical_json(
            reverse_configuration_records
        )
    )

    configuration_digest = sha256_payload(
        configuration_records
    )

    reverse_configuration_digest = (
        sha256_payload(
            reverse_configuration_records
        )
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in configuration_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row["configuration_status"]
                for row in configuration_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in configuration_records
                for blocker in row[
                    "configuration_blocker_codes"
                ]
            ).items()
        )
    )

    candidate_presence_counts = dict(
        sorted(
            Counter(
                str(
                    row[
                        "endpoint_candidate_present"
                    ]
                )
                for row in configuration_records
            ).items()
        )
    )

    retrieval_planning_authority_records = [
        row
        for row in configuration_records
        if row[
            "retrieval_planning_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_ao_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": (
                plan.PLAN_VERSION
                == EXPECTED_PLAN_VERSION
            ),
        },
        {
            "check": "nine_an_contract_version_verified",
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
            "check": "acquisition_replay_deterministic",
            "actual":
                acquisition_replay_deterministic,
            "expected": True,
            "passed":
                acquisition_replay_deterministic,
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
            "check":
                "configuration_digests_match_reverse_replay",
            "actual":
                configuration_digest,
            "expected":
                reverse_configuration_digest,
            "passed": (
                configuration_digest
                == reverse_configuration_digest
            ),
        },
        {
            "check": "expected_acquisition_records_replayed",
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
            "check":
                "expected_configuration_records_materialized",
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
                "one_configuration_record_per_comparison",
            "actual":
                len(comparison_ids),
            "expected":
                EXPECTED_CONFIGURATION_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_CONFIGURATION_RECORDS
            ),
        },
        {
            "check":
                "configuration_record_fields_complete",
            "actual":
                len(plan.CONFIGURATION_RECORD_FIELDS),
            "expected": 47,
            "passed": all(
                set(row)
                == set(
                    plan.CONFIGURATION_RECORD_FIELDS
                )
                for row in configuration_records
            ),
        },
        {
            "check":
                "configuration_record_ids_unique",
            "actual": len(
                {
                    row[
                        "authoritative_source_endpoint_configuration_record_id"
                    ]
                    for row in configuration_records
                }
            ),
            "expected":
                len(configuration_records),
            "passed": (
                len(
                    {
                        row[
                            "authoritative_source_endpoint_configuration_record_id"
                        ]
                        for row
                        in configuration_records
                    }
                )
                == len(configuration_records)
            ),
        },
        {
            "check":
                "configuration_record_digests_unique",
            "actual": len(
                {
                    row[
                        "authoritative_source_endpoint_configuration_record_digest"
                    ]
                    for row in configuration_records
                }
            ),
            "expected":
                len(configuration_records),
            "passed": (
                len(
                    {
                        row[
                            "authoritative_source_endpoint_configuration_record_digest"
                        ]
                        for row
                        in configuration_records
                    }
                )
                == len(configuration_records)
            ),
        },
        {
            "check": "all_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authoritative_source_endpoint_configuration_identity_digest"
                    ]
                )
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_source_endpoint_configuration_identity_digest"
                    ]
                )
                for row in configuration_records
            ),
        },
        {
            "check": "all_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authoritative_source_endpoint_configuration_record_digest"
                    ]
                )
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_source_endpoint_configuration_record_digest"
                    ]
                )
                for row in configuration_records
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
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_source_acquisition_record_digest"
                    ]
                )
                for row in configuration_records
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
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in configuration_records
            ),
        },
        {
            "check":
                "supplied_endpoint_candidate_inventory_empty",
            "actual":
                len(SUPPLIED_ENDPOINT_CANDIDATES),
            "expected": 0,
            "passed": (
                len(SUPPLIED_ENDPOINT_CANDIDATES)
                == 0
            ),
        },
        {
            "check":
                "all_endpoint_candidates_absent",
            "actual":
                candidate_presence_counts,
            "expected": {
                "False":
                    EXPECTED_CONFIGURATION_RECORDS
            },
            "passed": (
                candidate_presence_counts
                == {
                    "False":
                        EXPECTED_CONFIGURATION_RECORDS
                }
            ),
        },
        {
            "check":
                "all_configurations_candidate_not_supplied",
            "actual":
                status_counts,
            "expected": {
                CONFIGURATION_STATUS:
                    EXPECTED_CONFIGURATION_RECORDS
            },
            "passed": (
                status_counts
                == {
                    CONFIGURATION_STATUS:
                        EXPECTED_CONFIGURATION_RECORDS
                }
            ),
        },
        {
            "check":
                "all_candidate_missing_blockers_present",
            "actual":
                blocker_counts,
            "expected": {
                CONFIGURATION_BLOCKER:
                    EXPECTED_CONFIGURATION_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    CONFIGURATION_BLOCKER:
                        EXPECTED_CONFIGURATION_RECORDS
                }
            ),
        },
        {
            "check":
                "all_endpoint_configuration_ids_absent",
            "actual": sum(
                row[
                    "endpoint_configuration_id"
                ]
                is None
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                row[
                    "endpoint_configuration_id"
                ]
                is None
                for row in configuration_records
            ),
        },
        {
            "check": "all_base_uris_absent",
            "actual": sum(
                row["base_uri"] is None
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                row["base_uri"] is None
                for row in configuration_records
            ),
        },
        {
            "check":
                "all_authority_classes_absent",
            "actual": sum(
                row[
                    "source_authority_class"
                ]
                is None
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                row[
                    "source_authority_class"
                ]
                is None
                and not row[
                    "source_authority_eligible"
                ]
                for row in configuration_records
            ),
        },
        {
            "check":
                "all_identity_parameter_mappings_empty",
            "actual": sum(
                row[
                    "identity_parameter_mapping"
                ]
                == {}
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                row[
                    "identity_parameter_mapping"
                ]
                == {}
                for row in configuration_records
            ),
        },
        {
            "check":
                "all_credential_references_absent",
            "actual": sum(
                row[
                    "credential_reference"
                ]
                is None
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                row[
                    "credential_reference"
                ]
                is None
                and not row[
                    "credential_literal_present"
                ]
                for row in configuration_records
            ),
        },
        {
            "check":
                "all_transport_policies_absent",
            "actual": sum(
                row["timeout_policy"] is None
                and row["retry_policy"] is None
                and row["rate_limit_policy"] is None
                and row["pagination_policy"] is None
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                row["timeout_policy"] is None
                and row["retry_policy"] is None
                and row["rate_limit_policy"] is None
                and row["pagination_policy"] is None
                for row in configuration_records
            ),
        },
        {
            "check":
                "all_response_retention_policies_absent",
            "actual": sum(
                row[
                    "raw_response_retention_policy"
                ]
                is None
                and row[
                    "response_digest_algorithm"
                ]
                is None
                for row in configuration_records
            ),
            "expected":
                len(configuration_records),
            "passed": all(
                row[
                    "raw_response_retention_policy"
                ]
                is None
                and row[
                    "response_digest_algorithm"
                ]
                is None
                for row in configuration_records
            ),
        },
        {
            "check":
                "no_retrieval_planning_authority_granted",
            "actual":
                len(
                    retrieval_planning_authority_records
                ),
            "expected": 0,
            "passed": (
                len(
                    retrieval_planning_authority_records
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
                    for row in configuration_records
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
                for row in configuration_records
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
                    for row in configuration_records
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
                for row in configuration_records
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
                    for row in configuration_records
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
                for row in configuration_records
            ),
        },
        {
            "check":
                "endpoint_invention_not_executed",
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
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_endpoint_"
        "configuration_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_endpoint_"
        "configuration_implementation_failed"
    )

    next_layer = (
        "9AQ_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_endpoint_"
        "candidate_specification_plan"
        if all_checks_passed
        else
        "9AP_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_endpoint_"
        "configuration_implementation_remediation"
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
        / "authoritative_source_endpoint_configuration_records.csv",
        plan.CONFIGURATION_RECORD_FIELDS,
        configuration_records,
    )

    write_csv(
        OUTPUT_DIR / "configuration_status_counts.csv",
        [
            "configuration_status",
            "count",
        ],
        [
            {
                "configuration_status": key,
                "count": value,
            }
            for key, value
            in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "configuration_blocker_counts.csv",
        [
            "configuration_blocker",
            "count",
        ],
        [
            {
                "configuration_blocker": key,
                "count": value,
            }
            for key, value
            in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR
        / "supplied_endpoint_candidate_inventory.json",
        {
            "layer_id": LAYER_ID,
            "supplied_endpoint_candidate_count":
                len(SUPPLIED_ENDPOINT_CANDIDATES),
            "supplied_endpoint_candidates":
                list(SUPPLIED_ENDPOINT_CANDIDATES),
            "inventory_status":
                "no_endpoint_candidate_supplied",
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "configuration_contract_version":
            CONFIGURATION_CONTRACT_VERSION,
        "plan_version":
            plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.ACQUISITION_CONTRACT_VERSION,
        "acquisition_records":
            len(acquisition_records),
        "configuration_records":
            len(configuration_records),
        "configuration_comparisons":
            len(comparison_ids),
        "supplied_endpoint_candidates":
            len(SUPPLIED_ENDPOINT_CANDIDATES),
        "configuration_status_counts":
            status_counts,
        "configuration_blocker_counts":
            blocker_counts,
        "endpoint_candidates_configured": 0,
        "retrieval_planning_authorities_granted":
            len(
                retrieval_planning_authority_records
            ),
        "configuration_digest":
            configuration_digest,
        "reverse_configuration_digest":
            reverse_configuration_digest,
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
        / "authoritative_source_endpoint_configuration_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "configuration_result":
            CONFIGURATION_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_"
            "source_endpoint_candidate_specification_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "credential_literal_storage",
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
        "Configuration contract version: "
        f"{CONFIGURATION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Acquisition records replayed: "
        f"{len(acquisition_records)}"
    )
    print(
        "Configuration records: "
        f"{len(configuration_records)}"
    )
    print(
        "Configuration comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Supplied endpoint candidates: "
        f"{len(SUPPLIED_ENDPOINT_CANDIDATES)}"
    )
    print(
        "Configuration status counts: "
        f"{status_counts}"
    )
    print(
        "Configuration blocker counts: "
        f"{blocker_counts}"
    )
    print("Endpoint candidates configured: 0")
    print(
        "Retrieval planning authorities granted: "
        f"{len(retrieval_planning_authority_records)}"
    )
    print(
        "Configuration digest: "
        f"{configuration_digest}"
    )
    print(
        "Reverse configuration digest: "
        f"{reverse_configuration_digest}"
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
        "Configuration result: "
        f"{diagnosis['configuration_result']}"
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
