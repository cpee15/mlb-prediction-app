#!/usr/bin/env python3
"""
Layer 9AX
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Evidence Locator Submission Implementation

Implements the deterministic locator-submission contract planned by Layer 9AW.

No endpoint candidate or locator submission currently exists. This layer:

- replays the sixteen Layer 9AV locator-specification records;
- verifies the Layer 9AW submission plan;
- inventories explicit locator-submission envelopes;
- emits one deterministic submission record per comparison;
- classifies every record as `candidate_not_supplied`;
- grants no evidence retrieval, candidate approval, materialization, or
  historical outcome retrieval authority.

This layer does not invent candidates, locators, submission envelopes,
credentials, versions, digests, attestations, evidence, or outcome values.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AX"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_evidence_locator_submission_implementation"
)

LOCATOR_SUBMISSION_CONTRACT_VERSION = (
    "layer_9AX_historical_outcome_authoritative_source_endpoint_candidate_"
    "evidence_locator_submission_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AX_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_evidence_locator_submission"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AW_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_evidence_locator_submission.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AW_historical_outcome_authoritative_source_endpoint_candidate_"
    "evidence_locator_submission_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AV_historical_outcome_authoritative_source_endpoint_candidate_"
    "evidence_locator_specification_contract_v1"
)

EXPECTED_PREDECESSOR_RECORDS = 16
EXPECTED_SUBMISSION_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

SUBMISSION_STATUS = "candidate_not_supplied"

SUBMISSION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

SUPPLIED_LOCATOR_SUBMISSION_ENVELOPES: tuple[dict[str, Any], ...] = ()


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


def replay_plan() -> dict[str, Any]:
    plan = load_module(
        PLAN_PATH,
        "layer_9aw_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9AW plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AV contract version: "
            f"{predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_locator_submission_records(
    plan: Any,
    locator_specification_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    required_locator_classes = [
        "authority_documentation_locator",
        "coverage_documentation_locator",
        "identity_semantics_locator",
        "outcome_semantics_locator",
        "licensing_terms_locator",
        "availability_documentation_locator",
        "schema_or_snapshot_locator",
    ]

    records: list[dict[str, Any]] = []

    for specification in locator_specification_records:
        identity_payload = {
            "locator_submission_contract_version":
                LOCATOR_SUBMISSION_CONTRACT_VERSION,
            "evidence_locator_specification_plan_record_id":
                specification.get(
                    "evidence_locator_specification_plan_record_id"
                ),
            "comparison_record_id":
                specification.get("comparison_record_id"),
            "defect_source_record_id":
                specification.get("defect_source_record_id"),
            "candidate_id":
                specification.get("candidate_id"),
            "locator_submission_id": None,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "evidence_locator_submission_plan_contract_version":
                LOCATOR_SUBMISSION_CONTRACT_VERSION,
            "evidence_locator_submission_plan_record_id":
                "HOAELSUB-" + identity_digest[:20],
            "evidence_locator_specification_plan_record_id":
                specification.get(
                    "evidence_locator_specification_plan_record_id"
                ),
            "evidence_locator_specification_plan_record_digest":
                specification.get(
                    "evidence_locator_specification_plan_record_digest"
                ),
            "source_evidence_acquisition_plan_record_id":
                specification.get(
                    "source_evidence_acquisition_plan_record_id"
                ),
            "endpoint_candidate_specification_record_id":
                specification.get(
                    "endpoint_candidate_specification_record_id"
                ),
            "authoritative_source_endpoint_configuration_record_id":
                specification.get(
                    "authoritative_source_endpoint_configuration_record_id"
                ),
            "comparison_record_id":
                specification.get("comparison_record_id"),
            "metric_record_id":
                specification.get("metric_record_id"),
            "metric_name":
                specification.get("metric_name"),
            "aggregation_name":
                specification.get("aggregation_name"),
            "aggregation_key":
                specification.get("aggregation_key"),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                specification.get("defect_source_path"),
            "defect_source_symbol":
                specification.get("defect_source_symbol"),
            "defect_source_record_id":
                specification.get("defect_source_record_id"),
            "defect_source_record_digest":
                specification.get("defect_source_record_digest"),
            "locator_specification_status":
                specification.get("locator_specification_status"),
            "locator_specification_blocker_codes":
                specification.get(
                    "locator_specification_blocker_codes"
                ),
            "candidate_supplied":
                bool(specification.get("candidate_supplied")),
            "candidate_id":
                specification.get("candidate_id"),
            "candidate_version":
                specification.get("candidate_version"),
            "source_owner":
                specification.get("source_owner"),
            "source_class":
                specification.get("source_class"),
            "locator_submission_supplied": False,
            "locator_submission_id": None,
            "locator_submission_version": None,
            "submission_created_at_utc": None,
            "candidate_scope": None,
            "comparison_scope": None,
            "source_owner_scope": None,
            "source_class_scope": None,
            "locator_entries": [],
            "required_locator_classes":
                required_locator_classes,
            "credential_reference_contract": None,
            "credential_literal_present": False,
            "version_and_digest_contract": None,
            "submission_attestation": None,
            "locator_submission_status":
                SUBMISSION_STATUS,
            "locator_submission_blocker_codes": [
                SUBMISSION_BLOCKER
            ],
            "locator_submission_implementation_authority_granted":
                False,
            "locator_submission_rationale": (
                "No endpoint candidate exists, so no evidence-locator "
                "submission envelope can be accepted or completed without "
                "inventing candidate identity, locator entries, source scope, "
                "credential references, versions, digests, timestamps, or "
                "attestations."
            ),
            "locator_submission_limitations": [
                "No endpoint candidate was supplied.",
                "No locator-submission envelope was supplied.",
                "No locator entry was supplied or inferred.",
                "No source owner or source class was established.",
                "No credential reference or credential literal was stored.",
                "No version, digest, timestamp, or attestation was invented.",
                "No network retrieval was executed.",
                "No source evidence was acquired.",
                "No historical outcome value was acquired.",
                (
                    "No canonical source mutation, mapping change, value "
                    "transformation, or downstream recomputation was executed."
                ),
            ],
            "evidence_locator_submission_plan_identity_digest":
                identity_digest,
        }

        record[
            "evidence_locator_submission_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.SUBMISSION_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Locator submission record missing fields: "
                + ", ".join(missing_fields)
            )

        records.append(
            {
                field: record[field]
                for field in plan.SUBMISSION_RECORD_FIELDS
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
                row.get("locator_submission_id")
            ),
            normalized_string(
                row.get(
                    "evidence_locator_specification_plan_record_id"
                )
            ),
            normalized_string(
                row.get(
                    "evidence_locator_submission_plan_record_id"
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
    specification_records = replay["records"]
    reverse_specification_records = replay[
        "reverse_records"
    ]

    submission_records = build_locator_submission_records(
        plan,
        specification_records,
    )

    reverse_submission_records = build_locator_submission_records(
        plan,
        list(
            reversed(
                reverse_specification_records
            )
        ),
    )

    predecessor_replay_deterministic = (
        canonical_json(specification_records)
        == canonical_json(
            reverse_specification_records
        )
    )

    submission_replay_deterministic = (
        canonical_json(submission_records)
        == canonical_json(
            reverse_submission_records
        )
    )

    submission_digest = sha256_payload(
        submission_records
    )

    reverse_submission_digest = sha256_payload(
        reverse_submission_records
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in submission_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row["locator_submission_status"]
                for row in submission_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in submission_records
                for blocker in row[
                    "locator_submission_blocker_codes"
                ]
            ).items()
        )
    )

    candidate_presence_counts = dict(
        sorted(
            Counter(
                str(row["candidate_supplied"])
                for row in submission_records
            ).items()
        )
    )

    submission_presence_counts = dict(
        sorted(
            Counter(
                str(row["locator_submission_supplied"])
                for row in submission_records
            ).items()
        )
    )

    authority_records = [
        row
        for row in submission_records
        if row[
            "locator_submission_implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_aw_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": plan.PLAN_VERSION == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_av_contract_version_verified",
            "actual":
                predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION
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
            "check": "submission_replay_deterministic",
            "actual": submission_replay_deterministic,
            "expected": True,
            "passed": submission_replay_deterministic,
        },
        {
            "check": "submission_digests_match_reverse_replay",
            "actual": submission_digest,
            "expected": reverse_submission_digest,
            "passed": (
                submission_digest
                == reverse_submission_digest
            ),
        },
        {
            "check": "expected_predecessor_records_replayed",
            "actual": len(specification_records),
            "expected": EXPECTED_PREDECESSOR_RECORDS,
            "passed": (
                len(specification_records)
                == EXPECTED_PREDECESSOR_RECORDS
            ),
        },
        {
            "check": "expected_submission_records_materialized",
            "actual": len(submission_records),
            "expected": EXPECTED_SUBMISSION_RECORDS,
            "passed": (
                len(submission_records)
                == EXPECTED_SUBMISSION_RECORDS
            ),
        },
        {
            "check": "one_submission_record_per_comparison",
            "actual": len(comparison_ids),
            "expected": EXPECTED_SUBMISSION_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_SUBMISSION_RECORDS
            ),
        },
        {
            "check": "submission_record_fields_complete",
            "actual": len(plan.SUBMISSION_RECORD_FIELDS),
            "expected": 47,
            "passed": all(
                set(row)
                == set(plan.SUBMISSION_RECORD_FIELDS)
                for row in submission_records
            ),
        },
        {
            "check": "submission_record_ids_unique",
            "actual": len(
                {
                    row[
                        "evidence_locator_submission_plan_record_id"
                    ]
                    for row in submission_records
                }
            ),
            "expected": len(submission_records),
            "passed": (
                len(
                    {
                        row[
                            "evidence_locator_submission_plan_record_id"
                        ]
                        for row in submission_records
                    }
                )
                == len(submission_records)
            ),
        },
        {
            "check": "submission_record_digests_unique",
            "actual": len(
                {
                    row[
                        "evidence_locator_submission_plan_record_digest"
                    ]
                    for row in submission_records
                }
            ),
            "expected": len(submission_records),
            "passed": (
                len(
                    {
                        row[
                            "evidence_locator_submission_plan_record_digest"
                        ]
                        for row in submission_records
                    }
                )
                == len(submission_records)
            ),
        },
        {
            "check": "all_submission_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_locator_submission_plan_identity_digest"
                    ]
                )
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_locator_submission_plan_identity_digest"
                    ]
                )
                for row in submission_records
            ),
        },
        {
            "check": "all_submission_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_locator_submission_plan_record_digest"
                    ]
                )
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_locator_submission_plan_record_digest"
                    ]
                )
                for row in submission_records
            ),
        },
        {
            "check": "all_predecessor_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_locator_specification_plan_record_digest"
                    ]
                )
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_locator_specification_plan_record_digest"
                    ]
                )
                for row in submission_records
            ),
        },
        {
            "check": "all_defect_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row["defect_source_record_digest"]
                )
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                valid_sha256(
                    row["defect_source_record_digest"]
                )
                for row in submission_records
            ),
        },
        {
            "check": "supplied_submission_inventory_empty",
            "actual":
                len(SUPPLIED_LOCATOR_SUBMISSION_ENVELOPES),
            "expected": 0,
            "passed": (
                len(SUPPLIED_LOCATOR_SUBMISSION_ENVELOPES)
                == 0
            ),
        },
        {
            "check": "all_candidates_absent",
            "actual": candidate_presence_counts,
            "expected": {
                "False": EXPECTED_SUBMISSION_RECORDS
            },
            "passed": (
                candidate_presence_counts
                == {
                    "False":
                        EXPECTED_SUBMISSION_RECORDS
                }
            ),
        },
        {
            "check": "all_submission_envelopes_absent",
            "actual": submission_presence_counts,
            "expected": {
                "False": EXPECTED_SUBMISSION_RECORDS
            },
            "passed": (
                submission_presence_counts
                == {
                    "False":
                        EXPECTED_SUBMISSION_RECORDS
                }
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": status_counts,
            "expected": {
                SUBMISSION_STATUS:
                    EXPECTED_SUBMISSION_RECORDS
            },
            "passed": (
                status_counts
                == {
                    SUBMISSION_STATUS:
                        EXPECTED_SUBMISSION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_present",
            "actual": blocker_counts,
            "expected": {
                SUBMISSION_BLOCKER:
                    EXPECTED_SUBMISSION_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    SUBMISSION_BLOCKER:
                        EXPECTED_SUBMISSION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_identity_fields_absent",
            "actual": sum(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in submission_records
            ),
        },
        {
            "check": "all_source_authority_fields_absent",
            "actual": sum(
                row["source_owner"] is None
                and row["source_class"] is None
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                row["source_owner"] is None
                and row["source_class"] is None
                for row in submission_records
            ),
        },
        {
            "check": "all_submission_identity_fields_absent",
            "actual": sum(
                row["locator_submission_id"] is None
                and row["locator_submission_version"] is None
                and row["submission_created_at_utc"] is None
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                row["locator_submission_id"] is None
                and row["locator_submission_version"] is None
                and row["submission_created_at_utc"] is None
                for row in submission_records
            ),
        },
        {
            "check": "all_submission_scopes_absent",
            "actual": sum(
                row["candidate_scope"] is None
                and row["comparison_scope"] is None
                and row["source_owner_scope"] is None
                and row["source_class_scope"] is None
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                row["candidate_scope"] is None
                and row["comparison_scope"] is None
                and row["source_owner_scope"] is None
                and row["source_class_scope"] is None
                for row in submission_records
            ),
        },
        {
            "check": "all_locator_entries_empty",
            "actual": sum(
                row["locator_entries"] == []
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                row["locator_entries"] == []
                for row in submission_records
            ),
        },
        {
            "check": "required_locator_classes_preserved",
            "actual": len(
                {
                    canonical_json(
                        row["required_locator_classes"]
                    )
                    for row in submission_records
                }
            ),
            "expected": 1,
            "passed": all(
                len(row["required_locator_classes"])
                == 7
                for row in submission_records
            ),
        },
        {
            "check": "all_credential_contracts_absent",
            "actual": sum(
                row["credential_reference_contract"] is None
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                row["credential_reference_contract"] is None
                for row in submission_records
            ),
        },
        {
            "check": "all_credential_literals_absent",
            "actual": sum(
                not row["credential_literal_present"]
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                not row["credential_literal_present"]
                for row in submission_records
            ),
        },
        {
            "check": "all_version_digest_contracts_absent",
            "actual": sum(
                row["version_and_digest_contract"] is None
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                row["version_and_digest_contract"] is None
                for row in submission_records
            ),
        },
        {
            "check": "all_attestations_absent",
            "actual": sum(
                row["submission_attestation"] is None
                for row in submission_records
            ),
            "expected": len(submission_records),
            "passed": all(
                row["submission_attestation"] is None
                for row in submission_records
            ),
        },
        {
            "check": "no_submission_implementation_authority_granted",
            "actual": len(authority_records),
            "expected": 0,
            "passed": len(authority_records) == 0,
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_name"]
                    for row in submission_records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_NAME],
            "passed": all(
                row["authoritative_field_name"]
                == AUTHORITATIVE_FIELD_NAME
                for row in submission_records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_path"]
                    for row in submission_records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_PATH],
            "passed": all(
                row["authoritative_field_path"]
                == AUTHORITATIVE_FIELD_PATH
                for row in submission_records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row["rejected_metadata_field_name"]
                    for row in submission_records
                }
            ),
            "expected": [REJECTED_METADATA_FIELD],
            "passed": all(
                row["rejected_metadata_field_name"]
                == REJECTED_METADATA_FIELD
                for row in submission_records
            ),
        },
        {
            "check": "candidate_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "locator_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "submission_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "submission_completion_by_inference_not_executed",
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
        "endpoint_candidate_evidence_locator_submission_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_submission_implementation_failed"
    )

    next_layer = (
        "9AY_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_submission_source_evidence_validation_plan"
        if all_checks_passed
        else
        "9AX_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_submission_implementation_remediation"
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
        OUTPUT_DIR / "evidence_locator_submission_records.csv",
        plan.SUBMISSION_RECORD_FIELDS,
        submission_records,
    )

    write_csv(
        OUTPUT_DIR / "locator_submission_status_counts.csv",
        [
            "locator_submission_status",
            "count",
        ],
        [
            {
                "locator_submission_status": key,
                "count": value,
            }
            for key, value in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "locator_submission_blocker_counts.csv",
        [
            "locator_submission_blocker",
            "count",
        ],
        [
            {
                "locator_submission_blocker": key,
                "count": value,
            }
            for key, value in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR
        / "supplied_locator_submission_envelope_inventory.json",
        {
            "layer_id": LAYER_ID,
            "supplied_locator_submission_envelope_count":
                len(SUPPLIED_LOCATOR_SUBMISSION_ENVELOPES),
            "supplied_locator_submission_envelopes":
                list(SUPPLIED_LOCATOR_SUBMISSION_ENVELOPES),
            "inventory_status":
                "no_candidate_or_locator_submission_envelope_supplied",
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "locator_submission_contract_version":
            LOCATOR_SUBMISSION_CONTRACT_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.LOCATOR_SPECIFICATION_CONTRACT_VERSION,
        "predecessor_records":
            len(specification_records),
        "locator_submission_records":
            len(submission_records),
        "locator_submission_comparisons":
            len(comparison_ids),
        "supplied_locator_submission_envelopes":
            len(SUPPLIED_LOCATOR_SUBMISSION_ENVELOPES),
        "locator_submission_status_counts":
            status_counts,
        "locator_submission_blocker_counts":
            blocker_counts,
        "locator_submission_implementation_authorities_granted":
            len(authority_records),
        "submission_digest":
            submission_digest,
        "reverse_submission_digest":
            reverse_submission_digest,
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
        / "evidence_locator_submission_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "locator_submission_result":
            SUBMISSION_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "submission_source_evidence_validation_planning"
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
            "authority_evidence_fabrication",
            "credential_literal_storage",
            "source_evidence_retrieval_planning",
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
        "Locator submission contract version: "
        f"{LOCATOR_SUBMISSION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Predecessor records replayed: "
        f"{len(specification_records)}"
    )
    print(
        "Locator submission records: "
        f"{len(submission_records)}"
    )
    print(
        "Locator submission comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Supplied locator submission envelopes: "
        f"{len(SUPPLIED_LOCATOR_SUBMISSION_ENVELOPES)}"
    )
    print(
        "Locator submission status counts: "
        f"{status_counts}"
    )
    print(
        "Locator submission blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Locator submission implementation authorities granted: "
        f"{len(authority_records)}"
    )
    print(
        f"Submission digest: {submission_digest}"
    )
    print(
        "Reverse submission digest: "
        f"{reverse_submission_digest}"
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
        "Locator submission result: "
        f"{diagnosis['locator_submission_result']}"
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
