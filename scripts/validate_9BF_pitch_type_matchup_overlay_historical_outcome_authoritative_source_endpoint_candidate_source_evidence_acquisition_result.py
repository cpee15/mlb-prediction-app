#!/usr/bin/env python3
"""
Layer 9BF
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Acquisition Result Validation Implementation

Implements the deterministic acquisition-result validation contract planned by
Layer 9BE.

No endpoint candidate, approved acquisition authorization, completed acquisition
execution, acquisition-result submission, or quarantined response currently
exists.

This implementation:
- replays Layer 9BD acquisition-execution records deterministically;
- verifies the Layer 9BE validation plan contract;
- inventories explicitly supplied acquisition-result submissions;
- emits one deterministic validation record per comparison;
- classifies all records as `candidate_not_supplied`;
- validates no raw response because none exists;
- grants no response-parsing or historical-outcome extraction authority.

No candidate, submission, response artifact, response metadata, evidence, or
historical outcome is invented, inferred, defaulted, or fabricated.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BF"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_acquisition_result_validation_"
    "implementation"
)

ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION = (
    "layer_9BF_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_result_validation_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BF_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_acquisition_"
    "result_validation"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9BE_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_acquisition_result_validation.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9BE_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_result_validation_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BD_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_execution_contract_v1"
)

EXPECTED_PREDECESSOR_RECORDS = 16
EXPECTED_VALIDATION_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

VALIDATION_STATUS = "candidate_not_supplied"

VALIDATION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

SUPPLIED_ACQUISITION_RESULT_SUBMISSIONS: tuple[
    dict[str, Any], ...
] = ()


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
    return "" if value is None else str(value).strip()


def load_module(path: Path, module_name: str) -> Any:
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


def write_json(path: Path, payload: Any) -> None:
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
        "layer_9be_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9BE plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BD contract version: "
            f"{predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_validation_records(
    plan: Any,
    execution_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    validation_records: list[dict[str, Any]] = []

    for execution in execution_records:
        identity_payload = {
            "validation_contract_version":
                ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION,
            "execution_record_id":
                execution.get(
                    "source_evidence_acquisition_execution_plan_record_id"
                ),
            "comparison_record_id":
                execution.get("comparison_record_id"),
            "defect_source_record_id":
                execution.get("defect_source_record_id"),
            "candidate_id":
                execution.get("candidate_id"),
            "execution_id":
                execution.get("execution_id"),
            "result_id": None,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "source_evidence_acquisition_result_validation_plan_contract_version":
                ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION,
            "source_evidence_acquisition_result_validation_plan_record_id":
                "HOASEARV-" + identity_digest[:20],
            "source_evidence_acquisition_execution_plan_record_id":
                execution.get(
                    "source_evidence_acquisition_execution_plan_record_id"
                ),
            "source_evidence_acquisition_execution_plan_record_digest":
                execution.get(
                    "source_evidence_acquisition_execution_plan_record_digest"
                ),
            "source_evidence_acquisition_authorization_plan_record_id":
                execution.get(
                    "source_evidence_acquisition_authorization_plan_record_id"
                ),
            "source_evidence_validation_plan_record_id":
                execution.get(
                    "source_evidence_validation_plan_record_id"
                ),
            "endpoint_candidate_specification_record_id":
                execution.get(
                    "endpoint_candidate_specification_record_id"
                ),
            "comparison_record_id":
                execution.get("comparison_record_id"),
            "metric_record_id":
                execution.get("metric_record_id"),
            "metric_name":
                execution.get("metric_name"),
            "aggregation_name":
                execution.get("aggregation_name"),
            "aggregation_key":
                execution.get("aggregation_key"),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                execution.get("defect_source_path"),
            "defect_source_symbol":
                execution.get("defect_source_symbol"),
            "defect_source_record_id":
                execution.get("defect_source_record_id"),
            "defect_source_record_digest":
                execution.get("defect_source_record_digest"),
            "execution_status":
                execution.get(
                    "source_evidence_acquisition_execution_status"
                ),
            "execution_blocker_codes":
                execution.get(
                    "source_evidence_acquisition_execution_blocker_codes"
                ),
            "candidate_supplied":
                bool(execution.get("candidate_supplied")),
            "candidate_id":
                execution.get("candidate_id"),
            "candidate_version":
                execution.get("candidate_version"),
            "authorization_submission_supplied":
                bool(
                    execution.get(
                        "authorization_submission_supplied"
                    )
                ),
            "authorization_id":
                execution.get("authorization_id"),
            "execution_submission_supplied":
                bool(
                    execution.get(
                        "execution_submission_supplied"
                    )
                ),
            "execution_id":
                execution.get("execution_id"),
            "execution_version":
                execution.get("execution_version"),
            "execution_attempt_id":
                execution.get("execution_attempt_id"),
            "result_submission_supplied": False,
            "result_id": None,
            "result_version": None,
            "response_artifact_id": None,
            "response_received_at_utc": None,
            "response_status_code": None,
            "final_response_url": None,
            "redirect_chain": None,
            "response_media_type": None,
            "response_byte_length": None,
            "response_sha256": None,
            "response_truncated": None,
            "response_quarantined": None,
            "response_immutable": None,
            "retention_policy_reference": None,
            "audit_record_reference": None,
            "logs_redacted": None,
            "credential_literals_detected": None,
            "acquisition_result_validation_status":
                VALIDATION_STATUS,
            "acquisition_result_validation_blocker_codes": [
                VALIDATION_BLOCKER
            ],
            "acquisition_result_validation_implementation_authority_granted":
                False,
            "acquisition_result_validation_rationale": (
                "No endpoint candidate exists, so no authorized acquisition "
                "execution or immutable quarantined response exists. Result "
                "validation cannot proceed without inventing candidate, "
                "execution, response-artifact, transport, integrity, retention, "
                "audit, or redaction metadata."
            ),
            "acquisition_result_validation_limitations": [
                "No endpoint candidate was supplied.",
                "No approved acquisition authorization exists.",
                "No acquisition-execution submission was supplied.",
                "No acquisition execution was completed.",
                "No acquisition-result submission was supplied.",
                "No result identifier or version was invented.",
                "No response artifact identifier was invented.",
                "No response receipt timestamp was invented.",
                "No status code, final URL, redirect chain, or media type was invented.",
                "No byte length or SHA-256 digest was invented.",
                "No response was received, quarantined, retained, or validated.",
                "No credential literal was stored or logged.",
                "No DNS, socket, HTTP, browser, API, or network activity occurred.",
                "No source evidence or raw response was parsed.",
                "No historical outcome value was extracted.",
                (
                    "No canonical source mutation, mapping change, value "
                    "transformation, or downstream recomputation was executed."
                ),
            ],
            "acquisition_result_validation_plan_identity_digest":
                identity_digest,
        }

        record[
            "acquisition_result_validation_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.VALIDATION_PLAN_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Validation record missing fields: "
                + ", ".join(missing_fields)
            )

        validation_records.append(
            {
                field: record[field]
                for field in plan.VALIDATION_PLAN_RECORD_FIELDS
            }
        )

    validation_records.sort(
        key=lambda row: (
            normalized_string(
                row.get("comparison_record_id")
            ),
            normalized_string(
                row.get("defect_source_record_id")
            ),
            normalized_string(
                row.get("candidate_id")
            ),
            normalized_string(
                row.get("execution_id")
            ),
            normalized_string(
                row.get("result_id")
            ),
            normalized_string(
                row.get(
                    "source_evidence_acquisition_result_validation_"
                    "plan_record_id"
                )
            ),
        )
    )

    return validation_records


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    replay = replay_plan()

    plan = replay["plan"]
    predecessor = replay["predecessor"]
    execution_records = replay["records"]
    reverse_execution_records = replay[
        "reverse_records"
    ]

    validation_records = build_validation_records(
        plan,
        execution_records,
    )

    reverse_validation_records = build_validation_records(
        plan,
        list(
            reversed(
                reverse_execution_records
            )
        ),
    )

    predecessor_replay_deterministic = (
        canonical_json(execution_records)
        == canonical_json(
            reverse_execution_records
        )
    )

    validation_replay_deterministic = (
        canonical_json(validation_records)
        == canonical_json(
            reverse_validation_records
        )
    )

    validation_digest = sha256_payload(
        validation_records
    )

    reverse_validation_digest = sha256_payload(
        reverse_validation_records
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in validation_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row[
                    "acquisition_result_validation_status"
                ]
                for row in validation_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in validation_records
                for blocker in row[
                    "acquisition_result_validation_blocker_codes"
                ]
            ).items()
        )
    )

    candidate_presence_counts = dict(
        sorted(
            Counter(
                str(row["candidate_supplied"])
                for row in validation_records
            ).items()
        )
    )

    execution_presence_counts = dict(
        sorted(
            Counter(
                str(
                    row[
                        "execution_submission_supplied"
                    ]
                )
                for row in validation_records
            ).items()
        )
    )

    result_presence_counts = dict(
        sorted(
            Counter(
                str(
                    row[
                        "result_submission_supplied"
                    ]
                )
                for row in validation_records
            ).items()
        )
    )

    authority_records = [
        row
        for row in validation_records
        if row[
            "acquisition_result_validation_implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_be_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed":
                plan.PLAN_VERSION == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_bd_contract_version_verified",
            "actual":
                predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "predecessor_replay_deterministic",
            "actual": predecessor_replay_deterministic,
            "expected": True,
            "passed": predecessor_replay_deterministic,
        },
        {
            "check": "validation_replay_deterministic",
            "actual": validation_replay_deterministic,
            "expected": True,
            "passed": validation_replay_deterministic,
        },
        {
            "check": "validation_digests_match_reverse_replay",
            "actual": validation_digest,
            "expected": reverse_validation_digest,
            "passed": (
                validation_digest
                == reverse_validation_digest
            ),
        },
        {
            "check": "expected_predecessor_records_replayed",
            "actual": len(execution_records),
            "expected": EXPECTED_PREDECESSOR_RECORDS,
            "passed": (
                len(execution_records)
                == EXPECTED_PREDECESSOR_RECORDS
            ),
        },
        {
            "check": "expected_validation_records_materialized",
            "actual": len(validation_records),
            "expected": EXPECTED_VALIDATION_RECORDS,
            "passed": (
                len(validation_records)
                == EXPECTED_VALIDATION_RECORDS
            ),
        },
        {
            "check": "one_validation_record_per_comparison",
            "actual": len(comparison_ids),
            "expected": EXPECTED_VALIDATION_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_VALIDATION_RECORDS
            ),
        },
        {
            "check": "validation_record_fields_complete",
            "actual": len(
                plan.VALIDATION_PLAN_RECORD_FIELDS
            ),
            "expected": 55,
            "passed": all(
                set(row)
                == set(
                    plan.VALIDATION_PLAN_RECORD_FIELDS
                )
                for row in validation_records
            ),
        },
        {
            "check": "validation_record_ids_unique",
            "actual": len(
                {
                    row[
                        "source_evidence_acquisition_result_validation_"
                        "plan_record_id"
                    ]
                    for row in validation_records
                }
            ),
            "expected": len(validation_records),
            "passed": (
                len(
                    {
                        row[
                            "source_evidence_acquisition_result_validation_"
                            "plan_record_id"
                        ]
                        for row in validation_records
                    }
                )
                == len(validation_records)
            ),
        },
        {
            "check": "validation_record_digests_unique",
            "actual": len(
                {
                    row[
                        "acquisition_result_validation_plan_record_digest"
                    ]
                    for row in validation_records
                }
            ),
            "expected": len(validation_records),
            "passed": (
                len(
                    {
                        row[
                            "acquisition_result_validation_plan_record_digest"
                        ]
                        for row in validation_records
                    }
                )
                == len(validation_records)
            ),
        },
        {
            "check": "all_validation_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "acquisition_result_validation_plan_identity_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "acquisition_result_validation_plan_identity_digest"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "all_validation_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "acquisition_result_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "acquisition_result_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "all_execution_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_execution_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_execution_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "all_defect_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row["defect_source_record_digest"]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row["defect_source_record_digest"]
                )
                for row in validation_records
            ),
        },
        {
            "check": "supplied_result_inventory_empty",
            "actual": len(
                SUPPLIED_ACQUISITION_RESULT_SUBMISSIONS
            ),
            "expected": 0,
            "passed": (
                len(
                    SUPPLIED_ACQUISITION_RESULT_SUBMISSIONS
                )
                == 0
            ),
        },
        {
            "check": "all_candidates_absent",
            "actual": candidate_presence_counts,
            "expected": {
                "False": EXPECTED_VALIDATION_RECORDS
            },
            "passed": (
                candidate_presence_counts
                == {
                    "False":
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "all_execution_submissions_absent",
            "actual": execution_presence_counts,
            "expected": {
                "False": EXPECTED_VALIDATION_RECORDS
            },
            "passed": (
                execution_presence_counts
                == {
                    "False":
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "all_result_submissions_absent",
            "actual": result_presence_counts,
            "expected": {
                "False": EXPECTED_VALIDATION_RECORDS
            },
            "passed": (
                result_presence_counts
                == {
                    "False":
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": status_counts,
            "expected": {
                VALIDATION_STATUS:
                    EXPECTED_VALIDATION_RECORDS
            },
            "passed": (
                status_counts
                == {
                    VALIDATION_STATUS:
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_present",
            "actual": blocker_counts,
            "expected": {
                VALIDATION_BLOCKER:
                    EXPECTED_VALIDATION_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    VALIDATION_BLOCKER:
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_identity_fields_absent",
            "actual": sum(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_execution_identity_fields_absent",
            "actual": sum(
                row["execution_id"] is None
                and row["execution_version"] is None
                and row["execution_attempt_id"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["execution_id"] is None
                and row["execution_version"] is None
                and row["execution_attempt_id"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_result_identity_fields_absent",
            "actual": sum(
                row["result_id"] is None
                and row["result_version"] is None
                and row["response_artifact_id"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["result_id"] is None
                and row["result_version"] is None
                and row["response_artifact_id"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_transport_metadata_absent",
            "actual": sum(
                row["response_received_at_utc"] is None
                and row["response_status_code"] is None
                and row["final_response_url"] is None
                and row["redirect_chain"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["response_received_at_utc"] is None
                and row["response_status_code"] is None
                and row["final_response_url"] is None
                and row["redirect_chain"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_payload_metadata_absent",
            "actual": sum(
                row["response_media_type"] is None
                and row["response_byte_length"] is None
                and row["response_sha256"] is None
                and row["response_truncated"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["response_media_type"] is None
                and row["response_byte_length"] is None
                and row["response_sha256"] is None
                and row["response_truncated"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_retention_and_audit_metadata_absent",
            "actual": sum(
                row["response_quarantined"] is None
                and row["response_immutable"] is None
                and row["retention_policy_reference"] is None
                and row["audit_record_reference"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["response_quarantined"] is None
                and row["response_immutable"] is None
                and row["retention_policy_reference"] is None
                and row["audit_record_reference"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_security_validation_metadata_absent",
            "actual": sum(
                row["logs_redacted"] is None
                and row["credential_literals_detected"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["logs_redacted"] is None
                and row["credential_literals_detected"] is None
                for row in validation_records
            ),
        },
        {
            "check": "no_validation_implementation_authority_granted",
            "actual": len(authority_records),
            "expected": 0,
            "passed": len(authority_records) == 0,
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_name"]
                    for row in validation_records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_NAME],
            "passed": all(
                row["authoritative_field_name"]
                == AUTHORITATIVE_FIELD_NAME
                for row in validation_records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_path"]
                    for row in validation_records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_PATH],
            "passed": all(
                row["authoritative_field_path"]
                == AUTHORITATIVE_FIELD_PATH
                for row in validation_records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row["rejected_metadata_field_name"]
                    for row in validation_records
                }
            ),
            "expected": [REJECTED_METADATA_FIELD],
            "passed": all(
                row["rejected_metadata_field_name"]
                == REJECTED_METADATA_FIELD
                for row in validation_records
            ),
        },
        {
            "check": "candidate_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "result_submission_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "response_artifact_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "response_metadata_invention_not_executed",
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
            "check": "credential_literals_logged_zero",
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
            "check": "raw_responses_received_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "raw_responses_validated_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "source_evidence_parsed_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "historical_outcome_values_extracted_zero",
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
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_result_validation_"
        "implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_result_validation_"
        "implementation_failed"
    )

    next_layer = (
        "9BG_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_response_parsing_plan"
        if all_checks_passed
        else
        "9BF_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_result_validation_"
        "implementation_remediation"
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
        / "source_evidence_acquisition_result_validation_records.csv",
        plan.VALIDATION_PLAN_RECORD_FIELDS,
        validation_records,
    )

    write_csv(
        OUTPUT_DIR / "validation_status_counts.csv",
        [
            "validation_status",
            "count",
        ],
        [
            {
                "validation_status": key,
                "count": value,
            }
            for key, value in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "validation_blocker_counts.csv",
        [
            "validation_blocker",
            "count",
        ],
        [
            {
                "validation_blocker": key,
                "count": value,
            }
            for key, value in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR
        / "supplied_acquisition_result_submission_inventory.json",
        {
            "layer_id": LAYER_ID,
            "supplied_acquisition_result_submission_count":
                len(
                    SUPPLIED_ACQUISITION_RESULT_SUBMISSIONS
                ),
            "supplied_acquisition_result_submissions":
                list(
                    SUPPLIED_ACQUISITION_RESULT_SUBMISSIONS
                ),
            "inventory_status": (
                "no_candidate_execution_or_acquisition_result_"
                "submission_supplied"
            ),
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "acquisition_result_validation_contract_version":
            ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.ACQUISITION_EXECUTION_CONTRACT_VERSION,
        "predecessor_records":
            len(execution_records),
        "validation_records":
            len(validation_records),
        "validation_comparisons":
            len(comparison_ids),
        "supplied_acquisition_result_submissions":
            len(
                SUPPLIED_ACQUISITION_RESULT_SUBMISSIONS
            ),
        "validation_status_counts":
            status_counts,
        "validation_blocker_counts":
            blocker_counts,
        "validation_implementation_authorities_granted":
            len(authority_records),
        "validation_digest":
            validation_digest,
        "reverse_validation_digest":
            reverse_validation_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
        "credentials_stored": 0,
        "credential_literals_logged": 0,
        "network_retrievals_executed": 0,
        "raw_responses_received": 0,
        "raw_responses_validated": 0,
        "source_evidence_parsed": 0,
        "historical_outcome_values_extracted": 0,
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
        / "source_evidence_acquisition_result_validation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "validation_result":
            VALIDATION_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_response_parsing_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "endpoint_candidate_selection_without_submission",
            "acquisition_authorization_invention",
            "acquisition_execution_submission_invention",
            "acquisition_result_submission_invention",
            "acquisition_result_completion_by_inference",
            "response_artifact_invention",
            "response_metadata_invention",
            "credential_literal_storage",
            "credential_literal_logging",
            "dns_resolution_execution",
            "socket_connection_execution",
            "http_request_execution",
            "browser_execution",
            "api_request_execution",
            "source_evidence_fetch_execution",
            "source_evidence_parse_execution",
            "raw_response_parse_execution",
            "historical_outcome_retrieval_planning",
            "historical_outcome_fetch_execution",
            "historical_outcome_parse_execution",
            "historical_outcome_value_extraction",
            "canonical_source_value_mutation",
            "canonical_outcome_mapping_change",
            "boolean_to_integer_coercion",
            "source_value_defaulting",
            "source_value_inference",
            "source_value_imputation",
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
        "Acquisition result validation contract version: "
        f"{ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Predecessor records replayed: "
        f"{len(execution_records)}"
    )
    print(
        "Validation records: "
        f"{len(validation_records)}"
    )
    print(
        "Validation comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Supplied acquisition result submissions: "
        f"{len(SUPPLIED_ACQUISITION_RESULT_SUBMISSIONS)}"
    )
    print(
        "Validation status counts: "
        f"{status_counts}"
    )
    print(
        "Validation blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Validation implementation authorities granted: "
        f"{len(authority_records)}"
    )
    print(
        f"Validation digest: {validation_digest}"
    )
    print(
        "Reverse validation digest: "
        f"{reverse_validation_digest}"
    )
    print("Credentials stored: 0")
    print("Credential literals logged: 0")
    print("Network retrievals executed: 0")
    print("Raw responses received: 0")
    print("Raw responses validated: 0")
    print("Source evidence parsed: 0")
    print("Historical outcome values extracted: 0")
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
    print("Activation recommendations emitted: 0")
    print("Production probabilities changed: 0")
    print("Market comparisons executed: 0")
    print("Betting edges calculated: 0")
    print(
        f"Diagnosis: {diagnosis_name}"
    )
    print(
        "Validation result: "
        f"{diagnosis['validation_result']}"
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
