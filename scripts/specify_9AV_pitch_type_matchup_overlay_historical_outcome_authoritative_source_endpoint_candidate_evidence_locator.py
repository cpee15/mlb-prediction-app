#!/usr/bin/env python3
"""
Layer 9AV
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Evidence Locator Specification

Implements the deterministic evidence-locator specification contract planned
by Layer 9AU.

Layer 9AT established that no endpoint candidate or source-evidence submission
exists. Layer 9AU defined the contract for explicit evidence locators. Because
no candidate or locator submission is supplied, this implementation:

- replays the sixteen Layer 9AT evidence-acquisition records;
- verifies the Layer 9AU locator-specification plan;
- inventories explicit locator submissions;
- emits one deterministic locator-specification record per comparison;
- classifies every record as `candidate_not_supplied`;
- grants no locator implementation, evidence retrieval, candidate approval, or
  historical outcome retrieval authority.

This layer does not invent endpoint candidates, evidence locators, source
owners, documentation URIs, credentials, versions, digests, or evidence.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AV"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_evidence_locator_specification_implementation"
)

LOCATOR_SPECIFICATION_CONTRACT_VERSION = (
    "layer_9AV_historical_outcome_authoritative_source_endpoint_candidate_"
    "evidence_locator_specification_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AV_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_evidence_locator_specification"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AU_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_evidence_locator_specification.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AU_historical_outcome_authoritative_source_endpoint_candidate_"
    "evidence_locator_specification_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AT_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_contract_v1"
)

EXPECTED_PREDECESSOR_RECORDS = 16
EXPECTED_LOCATOR_SPECIFICATION_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

LOCATOR_SPECIFICATION_STATUS = "candidate_not_supplied"

LOCATOR_SPECIFICATION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

SUPPLIED_LOCATOR_SUBMISSIONS: tuple[dict[str, Any], ...] = ()


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
        "layer_9au_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9AU plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.EVIDENCE_ACQUISITION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AT contract version: "
            f"{predecessor.EVIDENCE_ACQUISITION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_locator_specification_records(
    plan: Any,
    evidence_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for evidence in evidence_records:
        identity_payload = {
            "locator_specification_contract_version":
                LOCATOR_SPECIFICATION_CONTRACT_VERSION,
            "source_evidence_acquisition_plan_record_id":
                evidence.get(
                    "source_evidence_acquisition_plan_record_id"
                ),
            "comparison_record_id":
                evidence.get("comparison_record_id"),
            "defect_source_record_id":
                evidence.get("defect_source_record_id"),
            "candidate_id":
                evidence.get("candidate_id"),
            "locator_submission_id": None,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "evidence_locator_specification_plan_contract_version":
                LOCATOR_SPECIFICATION_CONTRACT_VERSION,
            "evidence_locator_specification_plan_record_id":
                "HOAELS-" + identity_digest[:20],
            "source_evidence_acquisition_plan_record_id":
                evidence.get(
                    "source_evidence_acquisition_plan_record_id"
                ),
            "source_evidence_acquisition_plan_record_digest":
                evidence.get(
                    "source_evidence_acquisition_plan_record_digest"
                ),
            "endpoint_candidate_specification_record_id":
                evidence.get(
                    "endpoint_candidate_specification_record_id"
                ),
            "authoritative_source_endpoint_configuration_record_id":
                evidence.get(
                    "authoritative_source_endpoint_configuration_record_id"
                ),
            "comparison_record_id":
                evidence.get("comparison_record_id"),
            "metric_record_id":
                evidence.get("metric_record_id"),
            "metric_name":
                evidence.get("metric_name"),
            "aggregation_name":
                evidence.get("aggregation_name"),
            "aggregation_key":
                evidence.get("aggregation_key"),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                evidence.get("defect_source_path"),
            "defect_source_symbol":
                evidence.get("defect_source_symbol"),
            "defect_source_record_id":
                evidence.get("defect_source_record_id"),
            "defect_source_record_digest":
                evidence.get("defect_source_record_digest"),
            "evidence_acquisition_status":
                evidence.get("evidence_acquisition_status"),
            "evidence_acquisition_blocker_codes":
                evidence.get(
                    "evidence_acquisition_blocker_codes"
                ),
            "candidate_supplied":
                bool(evidence.get("candidate_supplied")),
            "candidate_id":
                evidence.get("candidate_id"),
            "candidate_version":
                evidence.get("candidate_version"),
            "source_owner":
                evidence.get("source_owner"),
            "source_class":
                evidence.get("source_class"),
            "locator_submission_supplied": False,
            "locator_submission_id": None,
            "locator_submission_version": None,
            "locator_type": None,
            "locator_type_eligible": False,
            "locator_value": None,
            "locator_class": None,
            "candidate_scope": None,
            "comparison_scope": None,
            "source_owner_scope": None,
            "credential_reference": None,
            "credential_literal_present": False,
            "snapshot_or_schema_version": None,
            "immutable_digest_algorithm": None,
            "immutable_digest": None,
            "locator_specification_status":
                LOCATOR_SPECIFICATION_STATUS,
            "locator_specification_blocker_codes": [
                LOCATOR_SPECIFICATION_BLOCKER
            ],
            "locator_specification_implementation_authority_granted":
                False,
            "locator_specification_rationale": (
                "No endpoint candidate submission exists. Therefore no "
                "evidence-locator submission can be specified without inventing "
                "a candidate, locator class, locator value, source owner, "
                "credential reference, version, or immutable digest."
            ),
            "locator_specification_limitations": [
                "No endpoint candidate was supplied.",
                "No evidence-locator submission was supplied.",
                "No locator type or locator class was selected.",
                "No URI or immutable repository path was invented.",
                "No credential reference or credential literal was stored.",
                "No network retrieval was executed.",
                "No source evidence was acquired.",
                "No historical outcome value was acquired.",
                (
                    "No canonical source mutation, mapping change, value "
                    "transformation, or downstream recomputation was executed."
                ),
            ],
            "evidence_locator_specification_plan_identity_digest":
                identity_digest,
        }

        record[
            "evidence_locator_specification_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.LOCATOR_SPECIFICATION_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Locator specification record missing fields: "
                + ", ".join(missing_fields)
            )

        records.append(
            {
                field: record[field]
                for field in plan.LOCATOR_SPECIFICATION_FIELDS
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
                row.get("locator_class")
            ),
            normalized_string(
                row.get("locator_value")
            ),
            normalized_string(
                row.get(
                    "evidence_locator_specification_plan_record_id"
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
    evidence_records = replay["records"]
    reverse_evidence_records = replay[
        "reverse_records"
    ]

    locator_records = build_locator_specification_records(
        plan,
        evidence_records,
    )

    reverse_locator_records = build_locator_specification_records(
        plan,
        list(
            reversed(
                reverse_evidence_records
            )
        ),
    )

    predecessor_replay_deterministic = (
        canonical_json(evidence_records)
        == canonical_json(
            reverse_evidence_records
        )
    )

    locator_replay_deterministic = (
        canonical_json(locator_records)
        == canonical_json(
            reverse_locator_records
        )
    )

    locator_digest = sha256_payload(
        locator_records
    )

    reverse_locator_digest = sha256_payload(
        reverse_locator_records
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in locator_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row["locator_specification_status"]
                for row in locator_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in locator_records
                for blocker in row[
                    "locator_specification_blocker_codes"
                ]
            ).items()
        )
    )

    candidate_presence_counts = dict(
        sorted(
            Counter(
                str(row["candidate_supplied"])
                for row in locator_records
            ).items()
        )
    )

    locator_submission_presence_counts = dict(
        sorted(
            Counter(
                str(row["locator_submission_supplied"])
                for row in locator_records
            ).items()
        )
    )

    authority_records = [
        row
        for row in locator_records
        if row[
            "locator_specification_implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_au_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": (
                plan.PLAN_VERSION
                == EXPECTED_PLAN_VERSION
            ),
        },
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
            "check": "predecessor_replay_deterministic",
            "actual":
                predecessor_replay_deterministic,
            "expected": True,
            "passed":
                predecessor_replay_deterministic,
        },
        {
            "check": "locator_replay_deterministic",
            "actual":
                locator_replay_deterministic,
            "expected": True,
            "passed":
                locator_replay_deterministic,
        },
        {
            "check": "locator_digests_match_reverse_replay",
            "actual":
                locator_digest,
            "expected":
                reverse_locator_digest,
            "passed": (
                locator_digest
                == reverse_locator_digest
            ),
        },
        {
            "check": "expected_predecessor_records_replayed",
            "actual":
                len(evidence_records),
            "expected":
                EXPECTED_PREDECESSOR_RECORDS,
            "passed": (
                len(evidence_records)
                == EXPECTED_PREDECESSOR_RECORDS
            ),
        },
        {
            "check": "expected_locator_records_materialized",
            "actual":
                len(locator_records),
            "expected":
                EXPECTED_LOCATOR_SPECIFICATION_RECORDS,
            "passed": (
                len(locator_records)
                == EXPECTED_LOCATOR_SPECIFICATION_RECORDS
            ),
        },
        {
            "check": "one_locator_record_per_comparison",
            "actual":
                len(comparison_ids),
            "expected":
                EXPECTED_LOCATOR_SPECIFICATION_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_LOCATOR_SPECIFICATION_RECORDS
            ),
        },
        {
            "check": "locator_record_fields_complete",
            "actual":
                len(plan.LOCATOR_SPECIFICATION_FIELDS),
            "expected": 47,
            "passed": all(
                set(row)
                == set(
                    plan.LOCATOR_SPECIFICATION_FIELDS
                )
                for row in locator_records
            ),
        },
        {
            "check": "locator_record_ids_unique",
            "actual": len(
                {
                    row[
                        "evidence_locator_specification_plan_record_id"
                    ]
                    for row in locator_records
                }
            ),
            "expected":
                len(locator_records),
            "passed": (
                len(
                    {
                        row[
                            "evidence_locator_specification_plan_record_id"
                        ]
                        for row in locator_records
                    }
                )
                == len(locator_records)
            ),
        },
        {
            "check": "locator_record_digests_unique",
            "actual": len(
                {
                    row[
                        "evidence_locator_specification_plan_record_digest"
                    ]
                    for row in locator_records
                }
            ),
            "expected":
                len(locator_records),
            "passed": (
                len(
                    {
                        row[
                            "evidence_locator_specification_plan_record_digest"
                        ]
                        for row in locator_records
                    }
                )
                == len(locator_records)
            ),
        },
        {
            "check": "all_locator_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_locator_specification_plan_identity_digest"
                    ]
                )
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_locator_specification_plan_identity_digest"
                    ]
                )
                for row in locator_records
            ),
        },
        {
            "check": "all_locator_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_locator_specification_plan_record_digest"
                    ]
                )
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_locator_specification_plan_record_digest"
                    ]
                )
                for row in locator_records
            ),
        },
        {
            "check": "all_predecessor_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_plan_record_digest"
                    ]
                )
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_plan_record_digest"
                    ]
                )
                for row in locator_records
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
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in locator_records
            ),
        },
        {
            "check": "supplied_locator_inventory_empty",
            "actual":
                len(SUPPLIED_LOCATOR_SUBMISSIONS),
            "expected": 0,
            "passed": (
                len(SUPPLIED_LOCATOR_SUBMISSIONS)
                == 0
            ),
        },
        {
            "check": "all_candidates_absent",
            "actual":
                candidate_presence_counts,
            "expected": {
                "False":
                    EXPECTED_LOCATOR_SPECIFICATION_RECORDS
            },
            "passed": (
                candidate_presence_counts
                == {
                    "False":
                        EXPECTED_LOCATOR_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check": "all_locator_submissions_absent",
            "actual":
                locator_submission_presence_counts,
            "expected": {
                "False":
                    EXPECTED_LOCATOR_SPECIFICATION_RECORDS
            },
            "passed": (
                locator_submission_presence_counts
                == {
                    "False":
                        EXPECTED_LOCATOR_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual":
                status_counts,
            "expected": {
                LOCATOR_SPECIFICATION_STATUS:
                    EXPECTED_LOCATOR_SPECIFICATION_RECORDS
            },
            "passed": (
                status_counts
                == {
                    LOCATOR_SPECIFICATION_STATUS:
                        EXPECTED_LOCATOR_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_present",
            "actual":
                blocker_counts,
            "expected": {
                LOCATOR_SPECIFICATION_BLOCKER:
                    EXPECTED_LOCATOR_SPECIFICATION_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    LOCATOR_SPECIFICATION_BLOCKER:
                        EXPECTED_LOCATOR_SPECIFICATION_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_identity_fields_absent",
            "actual": sum(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in locator_records
            ),
        },
        {
            "check": "all_source_authority_fields_absent",
            "actual": sum(
                row["source_owner"] is None
                and row["source_class"] is None
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                row["source_owner"] is None
                and row["source_class"] is None
                for row in locator_records
            ),
        },
        {
            "check": "all_locator_identity_fields_absent",
            "actual": sum(
                row["locator_submission_id"] is None
                and row["locator_submission_version"] is None
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                row["locator_submission_id"] is None
                and row["locator_submission_version"] is None
                for row in locator_records
            ),
        },
        {
            "check": "all_locator_type_and_values_absent",
            "actual": sum(
                row["locator_type"] is None
                and row["locator_value"] is None
                and row["locator_class"] is None
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                row["locator_type"] is None
                and row["locator_value"] is None
                and row["locator_class"] is None
                for row in locator_records
            ),
        },
        {
            "check": "all_locator_scopes_absent",
            "actual": sum(
                row["candidate_scope"] is None
                and row["comparison_scope"] is None
                and row["source_owner_scope"] is None
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                row["candidate_scope"] is None
                and row["comparison_scope"] is None
                and row["source_owner_scope"] is None
                for row in locator_records
            ),
        },
        {
            "check": "all_credential_references_absent",
            "actual": sum(
                row["credential_reference"] is None
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                row["credential_reference"] is None
                for row in locator_records
            ),
        },
        {
            "check": "all_credential_literals_absent",
            "actual": sum(
                not row["credential_literal_present"]
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                not row["credential_literal_present"]
                for row in locator_records
            ),
        },
        {
            "check": "all_snapshot_versions_absent",
            "actual": sum(
                row["snapshot_or_schema_version"] is None
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                row["snapshot_or_schema_version"] is None
                for row in locator_records
            ),
        },
        {
            "check": "all_locator_digests_absent",
            "actual": sum(
                row["immutable_digest_algorithm"] is None
                and row["immutable_digest"] is None
                for row in locator_records
            ),
            "expected":
                len(locator_records),
            "passed": all(
                row["immutable_digest_algorithm"] is None
                and row["immutable_digest"] is None
                for row in locator_records
            ),
        },
        {
            "check": "no_locator_specification_authority_granted",
            "actual":
                len(authority_records),
            "expected": 0,
            "passed": (
                len(authority_records)
                == 0
            ),
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_name"]
                    for row in locator_records
                }
            ),
            "expected": [
                AUTHORITATIVE_FIELD_NAME
            ],
            "passed": all(
                row["authoritative_field_name"]
                == AUTHORITATIVE_FIELD_NAME
                for row in locator_records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_path"]
                    for row in locator_records
                }
            ),
            "expected": [
                AUTHORITATIVE_FIELD_PATH
            ],
            "passed": all(
                row["authoritative_field_path"]
                == AUTHORITATIVE_FIELD_PATH
                for row in locator_records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row["rejected_metadata_field_name"]
                    for row in locator_records
                }
            ),
            "expected": [
                REJECTED_METADATA_FIELD
            ],
            "passed": all(
                row["rejected_metadata_field_name"]
                == REJECTED_METADATA_FIELD
                for row in locator_records
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
            "check": "locator_selection_without_submission_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "authority_evidence_fabrication_not_executed",
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
        "endpoint_candidate_evidence_locator_specification_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_specification_implementation_failed"
    )

    next_layer = (
        "9AW_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_submission_plan"
        if all_checks_passed
        else
        "9AV_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_specification_implementation_remediation"
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
        OUTPUT_DIR / "evidence_locator_specification_records.csv",
        plan.LOCATOR_SPECIFICATION_FIELDS,
        locator_records,
    )

    write_csv(
        OUTPUT_DIR / "locator_specification_status_counts.csv",
        [
            "locator_specification_status",
            "count",
        ],
        [
            {
                "locator_specification_status": key,
                "count": value,
            }
            for key, value in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "locator_specification_blocker_counts.csv",
        [
            "locator_specification_blocker",
            "count",
        ],
        [
            {
                "locator_specification_blocker": key,
                "count": value,
            }
            for key, value in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR / "supplied_locator_submission_inventory.json",
        {
            "layer_id": LAYER_ID,
            "supplied_locator_submission_count":
                len(SUPPLIED_LOCATOR_SUBMISSIONS),
            "supplied_locator_submissions":
                list(SUPPLIED_LOCATOR_SUBMISSIONS),
            "inventory_status":
                "no_candidate_or_evidence_locator_submission_supplied",
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "locator_specification_contract_version":
            LOCATOR_SPECIFICATION_CONTRACT_VERSION,
        "plan_version":
            plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.EVIDENCE_ACQUISITION_CONTRACT_VERSION,
        "predecessor_records":
            len(evidence_records),
        "locator_specification_records":
            len(locator_records),
        "locator_specification_comparisons":
            len(comparison_ids),
        "supplied_locator_submissions":
            len(SUPPLIED_LOCATOR_SUBMISSIONS),
        "locator_specification_status_counts":
            status_counts,
        "locator_specification_blocker_counts":
            blocker_counts,
        "locator_specification_implementation_authorities_granted":
            len(authority_records),
        "locator_digest":
            locator_digest,
        "reverse_locator_digest":
            reverse_locator_digest,
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
        "all_checks_passed":
            all_checks_passed,
        "recommended_next_layer":
            next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "evidence_locator_specification_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "locator_specification_result":
            LOCATOR_SPECIFICATION_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "evidence_locator_submission_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "endpoint_candidate_selection_without_submission",
            "evidence_locator_invention",
            "evidence_locator_selection_without_submission",
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
        "Locator specification contract version: "
        f"{LOCATOR_SPECIFICATION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Predecessor records replayed: "
        f"{len(evidence_records)}"
    )
    print(
        "Locator specification records: "
        f"{len(locator_records)}"
    )
    print(
        "Locator specification comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Supplied locator submissions: "
        f"{len(SUPPLIED_LOCATOR_SUBMISSIONS)}"
    )
    print(
        "Locator specification status counts: "
        f"{status_counts}"
    )
    print(
        "Locator specification blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Locator specification implementation authorities granted: "
        f"{len(authority_records)}"
    )
    print(
        f"Locator digest: {locator_digest}"
    )
    print(
        "Reverse locator digest: "
        f"{reverse_locator_digest}"
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
        "Locator specification result: "
        f"{diagnosis['locator_specification_result']}"
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
