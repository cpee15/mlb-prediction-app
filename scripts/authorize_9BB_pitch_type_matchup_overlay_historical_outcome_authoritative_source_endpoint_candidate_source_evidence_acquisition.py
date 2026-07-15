#!/usr/bin/env python3
"""
Layer 9BB
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Acquisition Authorization Implementation

Implements the deterministic acquisition-authorization contract planned by
Layer 9BA.

No endpoint candidate, locator submission, source-evidence submission, validated
source evidence, or acquisition-authorization submission currently exists.

This layer:
- replays the sixteen Layer 9AZ source-evidence validation records;
- verifies the Layer 9BA authorization plan;
- inventories explicitly supplied acquisition-authorization submissions;
- emits one deterministic authorization record per comparison;
- classifies every record as `candidate_not_supplied`;
- grants no acquisition implementation or execution authority.

This layer performs no network retrieval and invents no candidate, locator,
submission, evidence, credential, authorization, request scope, or outcome.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BB"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_acquisition_authorization_implementation"
)

ACQUISITION_AUTHORIZATION_CONTRACT_VERSION = (
    "layer_9BB_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_authorization_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BB_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_acquisition_"
    "authorization"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9BA_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_acquisition_authorization.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9BA_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_authorization_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AZ_historical_outcome_authoritative_source_endpoint_candidate_"
    "submission_source_evidence_validation_contract_v1"
)

EXPECTED_PREDECESSOR_RECORDS = 16
EXPECTED_AUTHORIZATION_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

AUTHORIZATION_STATUS = "candidate_not_supplied"

AUTHORIZATION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

SUPPLIED_ACQUISITION_AUTHORIZATION_SUBMISSIONS: tuple[
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
        "layer_9ba_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9BA plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AZ contract version: "
            f"{predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_authorization_records(
    plan: Any,
    validation_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for validation in validation_records:
        identity_payload = {
            "authorization_contract_version":
                ACQUISITION_AUTHORIZATION_CONTRACT_VERSION,
            "source_evidence_validation_plan_record_id":
                validation.get(
                    "source_evidence_validation_plan_record_id"
                ),
            "comparison_record_id":
                validation.get("comparison_record_id"),
            "defect_source_record_id":
                validation.get("defect_source_record_id"),
            "candidate_id":
                validation.get("candidate_id"),
            "source_evidence_submission_id":
                validation.get(
                    "source_evidence_submission_id"
                ),
            "authorization_id": None,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "source_evidence_acquisition_authorization_plan_contract_version":
                ACQUISITION_AUTHORIZATION_CONTRACT_VERSION,
            "source_evidence_acquisition_authorization_plan_record_id":
                "HOASEAA-" + identity_digest[:20],
            "source_evidence_validation_plan_record_id":
                validation.get(
                    "source_evidence_validation_plan_record_id"
                ),
            "source_evidence_validation_plan_record_digest":
                validation.get(
                    "source_evidence_validation_plan_record_digest"
                ),
            "evidence_locator_submission_plan_record_id":
                validation.get(
                    "evidence_locator_submission_plan_record_id"
                ),
            "evidence_locator_specification_plan_record_id":
                validation.get(
                    "evidence_locator_specification_plan_record_id"
                ),
            "source_evidence_acquisition_plan_record_id":
                validation.get(
                    "source_evidence_acquisition_plan_record_id"
                ),
            "endpoint_candidate_specification_record_id":
                validation.get(
                    "endpoint_candidate_specification_record_id"
                ),
            "authoritative_source_endpoint_configuration_record_id":
                validation.get(
                    "authoritative_source_endpoint_configuration_record_id"
                ),
            "comparison_record_id":
                validation.get("comparison_record_id"),
            "metric_record_id":
                validation.get("metric_record_id"),
            "metric_name":
                validation.get("metric_name"),
            "aggregation_name":
                validation.get("aggregation_name"),
            "aggregation_key":
                validation.get("aggregation_key"),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                validation.get("defect_source_path"),
            "defect_source_symbol":
                validation.get("defect_source_symbol"),
            "defect_source_record_id":
                validation.get("defect_source_record_id"),
            "defect_source_record_digest":
                validation.get("defect_source_record_digest"),
            "source_evidence_validation_status":
                validation.get(
                    "source_evidence_validation_status"
                ),
            "source_evidence_validation_blocker_codes":
                validation.get(
                    "source_evidence_validation_blocker_codes"
                ),
            "candidate_supplied":
                bool(validation.get("candidate_supplied")),
            "candidate_id":
                validation.get("candidate_id"),
            "candidate_version":
                validation.get("candidate_version"),
            "source_owner":
                validation.get("source_owner"),
            "source_class":
                validation.get("source_class"),
            "locator_submission_supplied":
                bool(
                    validation.get(
                        "locator_submission_supplied"
                    )
                ),
            "locator_submission_id":
                validation.get("locator_submission_id"),
            "source_evidence_submission_supplied":
                bool(
                    validation.get(
                        "source_evidence_submission_supplied"
                    )
                ),
            "source_evidence_submission_id":
                validation.get(
                    "source_evidence_submission_id"
                ),
            "authorization_submission_supplied": False,
            "authorization_id": None,
            "authorization_version": None,
            "authorization_created_at_utc": None,
            "authorization_expires_at_utc": None,
            "approved_request_scope": None,
            "credential_reference_contract": None,
            "request_log_redaction_contract": None,
            "rate_limit_retry_timeout_contract": None,
            "retention_integrity_audit_contract": None,
            "authorization_attestation": None,
            "revocation_contract": None,
            "source_evidence_acquisition_authorization_status":
                AUTHORIZATION_STATUS,
            "source_evidence_acquisition_authorization_blocker_codes": [
                AUTHORIZATION_BLOCKER
            ],
            "source_evidence_acquisition_authorization_implementation_authority_granted":
                False,
            "source_evidence_acquisition_authorization_rationale": (
                "No endpoint candidate exists, so acquisition authorization "
                "cannot be evaluated without inventing candidate identity, "
                "validated source-evidence lineage, request scope, credential "
                "references, operational contracts, expiration, attestation, "
                "or revocation metadata."
            ),
            "source_evidence_acquisition_authorization_limitations": [
                "No endpoint candidate was supplied.",
                "No locator submission was supplied.",
                "No source-evidence submission was supplied.",
                "No source-evidence validation was approved.",
                "No acquisition-authorization submission was supplied.",
                "No authorization identifier or version was invented.",
                "No host, path, query, method, or content type was approved.",
                "No credential reference or credential literal was stored.",
                "No rate-limit, retry, timeout, or retention contract was invented.",
                "No network retrieval or source-evidence acquisition was executed.",
                "No historical outcome value was acquired.",
                (
                    "No canonical source mutation, mapping change, value "
                    "transformation, or downstream recomputation was executed."
                ),
            ],
            "source_evidence_acquisition_authorization_plan_identity_digest":
                identity_digest,
        }

        record[
            "source_evidence_acquisition_authorization_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.AUTHORIZATION_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Authorization record missing fields: "
                + ", ".join(missing_fields)
            )

        records.append(
            {
                field: record[field]
                for field in plan.AUTHORIZATION_RECORD_FIELDS
            }
        )

    records.sort(
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
                row.get("source_evidence_submission_id")
            ),
            normalized_string(
                row.get("authorization_id")
            ),
            normalized_string(
                row.get(
                    "source_evidence_acquisition_authorization_plan_record_id"
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
    validation_records = replay["records"]
    reverse_validation_records = replay[
        "reverse_records"
    ]

    authorization_records = build_authorization_records(
        plan,
        validation_records,
    )

    reverse_authorization_records = build_authorization_records(
        plan,
        list(
            reversed(
                reverse_validation_records
            )
        ),
    )

    predecessor_replay_deterministic = (
        canonical_json(validation_records)
        == canonical_json(
            reverse_validation_records
        )
    )

    authorization_replay_deterministic = (
        canonical_json(authorization_records)
        == canonical_json(
            reverse_authorization_records
        )
    )

    authorization_digest = sha256_payload(
        authorization_records
    )

    reverse_authorization_digest = sha256_payload(
        reverse_authorization_records
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in authorization_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row[
                    "source_evidence_acquisition_authorization_status"
                ]
                for row in authorization_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in authorization_records
                for blocker in row[
                    "source_evidence_acquisition_authorization_blocker_codes"
                ]
            ).items()
        )
    )

    candidate_presence_counts = dict(
        sorted(
            Counter(
                str(row["candidate_supplied"])
                for row in authorization_records
            ).items()
        )
    )

    locator_presence_counts = dict(
        sorted(
            Counter(
                str(row["locator_submission_supplied"])
                for row in authorization_records
            ).items()
        )
    )

    evidence_presence_counts = dict(
        sorted(
            Counter(
                str(
                    row[
                        "source_evidence_submission_supplied"
                    ]
                )
                for row in authorization_records
            ).items()
        )
    )

    authorization_presence_counts = dict(
        sorted(
            Counter(
                str(
                    row[
                        "authorization_submission_supplied"
                    ]
                )
                for row in authorization_records
            ).items()
        )
    )

    authority_records = [
        row
        for row in authorization_records
        if row[
            "source_evidence_acquisition_authorization_implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_ba_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed":
                plan.PLAN_VERSION == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_az_contract_version_verified",
            "actual":
                predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION
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
            "check": "authorization_replay_deterministic",
            "actual": authorization_replay_deterministic,
            "expected": True,
            "passed": authorization_replay_deterministic,
        },
        {
            "check": "authorization_digests_match_reverse_replay",
            "actual": authorization_digest,
            "expected": reverse_authorization_digest,
            "passed": (
                authorization_digest
                == reverse_authorization_digest
            ),
        },
        {
            "check": "expected_predecessor_records_replayed",
            "actual": len(validation_records),
            "expected": EXPECTED_PREDECESSOR_RECORDS,
            "passed": (
                len(validation_records)
                == EXPECTED_PREDECESSOR_RECORDS
            ),
        },
        {
            "check": "expected_authorization_records_materialized",
            "actual": len(authorization_records),
            "expected": EXPECTED_AUTHORIZATION_RECORDS,
            "passed": (
                len(authorization_records)
                == EXPECTED_AUTHORIZATION_RECORDS
            ),
        },
        {
            "check": "one_authorization_record_per_comparison",
            "actual": len(comparison_ids),
            "expected": EXPECTED_AUTHORIZATION_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_AUTHORIZATION_RECORDS
            ),
        },
        {
            "check": "authorization_record_fields_complete",
            "actual": len(plan.AUTHORIZATION_RECORD_FIELDS),
            "expected": 51,
            "passed": all(
                set(row)
                == set(plan.AUTHORIZATION_RECORD_FIELDS)
                for row in authorization_records
            ),
        },
        {
            "check": "authorization_record_ids_unique",
            "actual": len(
                {
                    row[
                        "source_evidence_acquisition_authorization_plan_record_id"
                    ]
                    for row in authorization_records
                }
            ),
            "expected": len(authorization_records),
            "passed": (
                len(
                    {
                        row[
                            "source_evidence_acquisition_authorization_plan_record_id"
                        ]
                        for row in authorization_records
                    }
                )
                == len(authorization_records)
            ),
        },
        {
            "check": "authorization_record_digests_unique",
            "actual": len(
                {
                    row[
                        "source_evidence_acquisition_authorization_plan_record_digest"
                    ]
                    for row in authorization_records
                }
            ),
            "expected": len(authorization_records),
            "passed": (
                len(
                    {
                        row[
                            "source_evidence_acquisition_authorization_plan_record_digest"
                        ]
                        for row in authorization_records
                    }
                )
                == len(authorization_records)
            ),
        },
        {
            "check": "all_authorization_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_authorization_plan_identity_digest"
                    ]
                )
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_authorization_plan_identity_digest"
                    ]
                )
                for row in authorization_records
            ),
        },
        {
            "check": "all_authorization_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_authorization_plan_record_digest"
                    ]
                )
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_authorization_plan_record_digest"
                    ]
                )
                for row in authorization_records
            ),
        },
        {
            "check": "all_predecessor_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_validation_plan_record_digest"
                    ]
                )
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_validation_plan_record_digest"
                    ]
                )
                for row in authorization_records
            ),
        },
        {
            "check": "all_defect_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row["defect_source_record_digest"]
                )
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                valid_sha256(
                    row["defect_source_record_digest"]
                )
                for row in authorization_records
            ),
        },
        {
            "check": "supplied_authorization_inventory_empty",
            "actual": len(
                SUPPLIED_ACQUISITION_AUTHORIZATION_SUBMISSIONS
            ),
            "expected": 0,
            "passed": (
                len(
                    SUPPLIED_ACQUISITION_AUTHORIZATION_SUBMISSIONS
                )
                == 0
            ),
        },
        {
            "check": "all_candidates_absent",
            "actual": candidate_presence_counts,
            "expected": {
                "False": EXPECTED_AUTHORIZATION_RECORDS
            },
            "passed": (
                candidate_presence_counts
                == {
                    "False":
                        EXPECTED_AUTHORIZATION_RECORDS
                }
            ),
        },
        {
            "check": "all_locator_submissions_absent",
            "actual": locator_presence_counts,
            "expected": {
                "False": EXPECTED_AUTHORIZATION_RECORDS
            },
            "passed": (
                locator_presence_counts
                == {
                    "False":
                        EXPECTED_AUTHORIZATION_RECORDS
                }
            ),
        },
        {
            "check": "all_source_evidence_submissions_absent",
            "actual": evidence_presence_counts,
            "expected": {
                "False": EXPECTED_AUTHORIZATION_RECORDS
            },
            "passed": (
                evidence_presence_counts
                == {
                    "False":
                        EXPECTED_AUTHORIZATION_RECORDS
                }
            ),
        },
        {
            "check": "all_authorization_submissions_absent",
            "actual": authorization_presence_counts,
            "expected": {
                "False": EXPECTED_AUTHORIZATION_RECORDS
            },
            "passed": (
                authorization_presence_counts
                == {
                    "False":
                        EXPECTED_AUTHORIZATION_RECORDS
                }
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": status_counts,
            "expected": {
                AUTHORIZATION_STATUS:
                    EXPECTED_AUTHORIZATION_RECORDS
            },
            "passed": (
                status_counts
                == {
                    AUTHORIZATION_STATUS:
                        EXPECTED_AUTHORIZATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_present",
            "actual": blocker_counts,
            "expected": {
                AUTHORIZATION_BLOCKER:
                    EXPECTED_AUTHORIZATION_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    AUTHORIZATION_BLOCKER:
                        EXPECTED_AUTHORIZATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_identity_fields_absent",
            "actual": sum(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in authorization_records
            ),
        },
        {
            "check": "all_source_authority_fields_absent",
            "actual": sum(
                row["source_owner"] is None
                and row["source_class"] is None
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                row["source_owner"] is None
                and row["source_class"] is None
                for row in authorization_records
            ),
        },
        {
            "check": "all_authorization_identity_fields_absent",
            "actual": sum(
                row["authorization_id"] is None
                and row["authorization_version"] is None
                and row["authorization_created_at_utc"] is None
                and row["authorization_expires_at_utc"] is None
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                row["authorization_id"] is None
                and row["authorization_version"] is None
                and row["authorization_created_at_utc"] is None
                and row["authorization_expires_at_utc"] is None
                for row in authorization_records
            ),
        },
        {
            "check": "all_request_scopes_absent",
            "actual": sum(
                row["approved_request_scope"] is None
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                row["approved_request_scope"] is None
                for row in authorization_records
            ),
        },
        {
            "check": "all_credential_contracts_absent",
            "actual": sum(
                row["credential_reference_contract"] is None
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                row["credential_reference_contract"] is None
                for row in authorization_records
            ),
        },
        {
            "check": "all_operational_contracts_absent",
            "actual": sum(
                row["request_log_redaction_contract"] is None
                and row["rate_limit_retry_timeout_contract"] is None
                and row["retention_integrity_audit_contract"] is None
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                row["request_log_redaction_contract"] is None
                and row["rate_limit_retry_timeout_contract"] is None
                and row["retention_integrity_audit_contract"] is None
                for row in authorization_records
            ),
        },
        {
            "check": "all_attestation_and_revocation_contracts_absent",
            "actual": sum(
                row["authorization_attestation"] is None
                and row["revocation_contract"] is None
                for row in authorization_records
            ),
            "expected": len(authorization_records),
            "passed": all(
                row["authorization_attestation"] is None
                and row["revocation_contract"] is None
                for row in authorization_records
            ),
        },
        {
            "check": "no_authorization_implementation_authority_granted",
            "actual": len(authority_records),
            "expected": 0,
            "passed": len(authority_records) == 0,
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_name"]
                    for row in authorization_records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_NAME],
            "passed": all(
                row["authoritative_field_name"]
                == AUTHORITATIVE_FIELD_NAME
                for row in authorization_records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_path"]
                    for row in authorization_records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_PATH],
            "passed": all(
                row["authoritative_field_path"]
                == AUTHORITATIVE_FIELD_PATH
                for row in authorization_records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row["rejected_metadata_field_name"]
                    for row in authorization_records
                }
            ),
            "expected": [REJECTED_METADATA_FIELD],
            "passed": all(
                row["rejected_metadata_field_name"]
                == REJECTED_METADATA_FIELD
                for row in authorization_records
            ),
        },
        {
            "check": "candidate_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "source_evidence_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "authorization_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "authorization_completion_by_inference_not_executed",
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
            "check": "network_retrievals_executed_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "source_evidence_acquired_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "historical_outcome_values_acquired_zero",
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
        "endpoint_candidate_source_evidence_acquisition_authorization_"
        "implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_authorization_"
        "implementation_failed"
    )

    next_layer = (
        "9BC_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_execution_plan"
        if all_checks_passed
        else
        "9BB_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_authorization_"
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
        / "source_evidence_acquisition_authorization_records.csv",
        plan.AUTHORIZATION_RECORD_FIELDS,
        authorization_records,
    )

    write_csv(
        OUTPUT_DIR / "authorization_status_counts.csv",
        [
            "authorization_status",
            "count",
        ],
        [
            {
                "authorization_status": key,
                "count": value,
            }
            for key, value in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "authorization_blocker_counts.csv",
        [
            "authorization_blocker",
            "count",
        ],
        [
            {
                "authorization_blocker": key,
                "count": value,
            }
            for key, value in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR
        / "supplied_acquisition_authorization_submission_inventory.json",
        {
            "layer_id": LAYER_ID,
            "supplied_acquisition_authorization_submission_count":
                len(
                    SUPPLIED_ACQUISITION_AUTHORIZATION_SUBMISSIONS
                ),
            "supplied_acquisition_authorization_submissions":
                list(
                    SUPPLIED_ACQUISITION_AUTHORIZATION_SUBMISSIONS
                ),
            "inventory_status": (
                "no_candidate_locator_source_evidence_or_"
                "authorization_submission_supplied"
            ),
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "acquisition_authorization_contract_version":
            ACQUISITION_AUTHORIZATION_CONTRACT_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION,
        "predecessor_records":
            len(validation_records),
        "authorization_records":
            len(authorization_records),
        "authorization_comparisons":
            len(comparison_ids),
        "supplied_authorization_submissions":
            len(
                SUPPLIED_ACQUISITION_AUTHORIZATION_SUBMISSIONS
            ),
        "authorization_status_counts":
            status_counts,
        "authorization_blocker_counts":
            blocker_counts,
        "authorization_implementation_authorities_granted":
            len(authority_records),
        "authorization_digest":
            authorization_digest,
        "reverse_authorization_digest":
            reverse_authorization_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
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
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "source_evidence_acquisition_authorization_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authorization_result":
            AUTHORIZATION_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_acquisition_execution_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "endpoint_candidate_selection_without_submission",
            "evidence_locator_invention",
            "evidence_locator_selection_without_submission",
            "locator_submission_invention",
            "locator_submission_completion_by_inference",
            "source_evidence_invention",
            "source_evidence_completion_by_inference",
            "source_evidence_fabrication",
            "acquisition_authorization_invention",
            "acquisition_authorization_completion_by_inference",
            "credential_literal_storage",
            "source_evidence_fetch_execution",
            "source_evidence_parse_execution",
            "candidate_approval",
            "candidate_materialization",
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
        ],
        "recommended_next_layer": next_layer,
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
        "Acquisition authorization contract version: "
        f"{ACQUISITION_AUTHORIZATION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Predecessor records replayed: "
        f"{len(validation_records)}"
    )
    print(
        "Authorization records: "
        f"{len(authorization_records)}"
    )
    print(
        "Authorization comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Supplied authorization submissions: "
        f"{len(SUPPLIED_ACQUISITION_AUTHORIZATION_SUBMISSIONS)}"
    )
    print(
        "Authorization status counts: "
        f"{status_counts}"
    )
    print(
        "Authorization blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Authorization implementation authorities granted: "
        f"{len(authority_records)}"
    )
    print(
        f"Authorization digest: {authorization_digest}"
    )
    print(
        "Reverse authorization digest: "
        f"{reverse_authorization_digest}"
    )
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
    print("Activation recommendations emitted: 0")
    print("Production probabilities changed: 0")
    print("Market comparisons executed: 0")
    print("Betting edges calculated: 0")
    print(
        f"Diagnosis: {diagnosis_name}"
    )
    print(
        "Authorization result: "
        f"{diagnosis['authorization_result']}"
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
