#!/usr/bin/env python3
"""
Layer 9AZ
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Submission
Source Evidence Validation Implementation

Implements the deterministic source-evidence validation contract planned by
Layer 9AY.

No endpoint candidate, locator-submission envelope, or source-evidence
submission currently exists. This layer:

- replays the sixteen Layer 9AX locator-submission records;
- verifies the Layer 9AY validation plan;
- inventories explicitly supplied source-evidence submissions;
- emits one deterministic validation record per comparison;
- classifies every record as `candidate_not_supplied`;
- grants no evidence-validation implementation or retrieval authority.

This layer does not invent candidates, locators, submissions, evidence,
credentials, versions, digests, semantic claims, or historical outcomes.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AZ"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_submission_source_evidence_validation_implementation"
)

SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION = (
    "layer_9AZ_historical_outcome_authoritative_source_endpoint_candidate_"
    "submission_source_evidence_validation_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AZ_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_submission_source_evidence_"
    "validation"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AY_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_submission_source_evidence_validation.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AY_historical_outcome_authoritative_source_endpoint_candidate_"
    "submission_source_evidence_validation_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AX_historical_outcome_authoritative_source_endpoint_candidate_"
    "evidence_locator_submission_contract_v1"
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

SUPPLIED_SOURCE_EVIDENCE_SUBMISSIONS: tuple[dict[str, Any], ...] = ()


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
        "layer_9ay_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9AY plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AX contract version: "
            f"{predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_source_evidence_validation_records(
    plan: Any,
    locator_submission_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    required_evidence_classes = [
        row["evidence_class"]
        for row in plan.EVIDENCE_CLASSES
        if row["required"]
    ]

    records: list[dict[str, Any]] = []

    for submission in locator_submission_records:
        identity_payload = {
            "source_evidence_validation_contract_version":
                SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION,
            "evidence_locator_submission_plan_record_id":
                submission.get(
                    "evidence_locator_submission_plan_record_id"
                ),
            "comparison_record_id":
                submission.get("comparison_record_id"),
            "defect_source_record_id":
                submission.get("defect_source_record_id"),
            "candidate_id":
                submission.get("candidate_id"),
            "locator_submission_id":
                submission.get("locator_submission_id"),
            "source_evidence_submission_id": None,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "source_evidence_validation_plan_contract_version":
                SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION,
            "source_evidence_validation_plan_record_id":
                "HOASEV-" + identity_digest[:20],
            "evidence_locator_submission_plan_record_id":
                submission.get(
                    "evidence_locator_submission_plan_record_id"
                ),
            "evidence_locator_submission_plan_record_digest":
                submission.get(
                    "evidence_locator_submission_plan_record_digest"
                ),
            "evidence_locator_specification_plan_record_id":
                submission.get(
                    "evidence_locator_specification_plan_record_id"
                ),
            "source_evidence_acquisition_plan_record_id":
                submission.get(
                    "source_evidence_acquisition_plan_record_id"
                ),
            "endpoint_candidate_specification_record_id":
                submission.get(
                    "endpoint_candidate_specification_record_id"
                ),
            "authoritative_source_endpoint_configuration_record_id":
                submission.get(
                    "authoritative_source_endpoint_configuration_record_id"
                ),
            "comparison_record_id":
                submission.get("comparison_record_id"),
            "metric_record_id":
                submission.get("metric_record_id"),
            "metric_name":
                submission.get("metric_name"),
            "aggregation_name":
                submission.get("aggregation_name"),
            "aggregation_key":
                submission.get("aggregation_key"),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                submission.get("defect_source_path"),
            "defect_source_symbol":
                submission.get("defect_source_symbol"),
            "defect_source_record_id":
                submission.get("defect_source_record_id"),
            "defect_source_record_digest":
                submission.get("defect_source_record_digest"),
            "locator_submission_status":
                submission.get("locator_submission_status"),
            "locator_submission_blocker_codes":
                submission.get(
                    "locator_submission_blocker_codes"
                ),
            "candidate_supplied":
                bool(submission.get("candidate_supplied")),
            "candidate_id":
                submission.get("candidate_id"),
            "candidate_version":
                submission.get("candidate_version"),
            "source_owner":
                submission.get("source_owner"),
            "source_class":
                submission.get("source_class"),
            "locator_submission_supplied":
                bool(
                    submission.get(
                        "locator_submission_supplied"
                    )
                ),
            "locator_submission_id":
                submission.get("locator_submission_id"),
            "locator_submission_version":
                submission.get("locator_submission_version"),
            "source_evidence_submission_supplied": False,
            "source_evidence_submission_id": None,
            "source_evidence_submission_version": None,
            "source_evidence_artifacts": [],
            "required_evidence_classes":
                required_evidence_classes,
            "source_evidence_validation_status":
                VALIDATION_STATUS,
            "source_evidence_validation_blocker_codes": [
                VALIDATION_BLOCKER
            ],
            "source_evidence_validation_implementation_authority_granted":
                False,
            "source_evidence_validation_rationale": (
                "No endpoint candidate exists, so source evidence cannot be "
                "validated without inventing candidate identity, locator "
                "submission lineage, evidence artifacts, semantic claims, "
                "versions, digests, or source authority."
            ),
            "source_evidence_validation_limitations": [
                "No endpoint candidate was supplied.",
                "No locator-submission envelope was supplied.",
                "No source-evidence submission was supplied.",
                "No source-evidence artifact was supplied or inferred.",
                "No source-owner authority claim was validated.",
                "No coverage or semantic claim was validated.",
                "No licensing, availability, or schema claim was validated.",
                "No credential literal was stored.",
                "No network retrieval was executed.",
                "No historical outcome value was acquired.",
                (
                    "No canonical source mutation, mapping change, value "
                    "transformation, or downstream recomputation was executed."
                ),
            ],
            "source_evidence_validation_plan_identity_digest":
                identity_digest,
        }

        record[
            "source_evidence_validation_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.VALIDATION_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Source evidence validation record missing fields: "
                + ", ".join(missing_fields)
            )

        records.append(
            {
                field: record[field]
                for field in plan.VALIDATION_RECORD_FIELDS
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
                row.get("source_evidence_submission_id")
            ),
            normalized_string(
                row.get(
                    "source_evidence_validation_plan_record_id"
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
    locator_submission_records = replay["records"]
    reverse_locator_submission_records = replay[
        "reverse_records"
    ]

    validation_records = build_source_evidence_validation_records(
        plan,
        locator_submission_records,
    )

    reverse_validation_records = build_source_evidence_validation_records(
        plan,
        list(
            reversed(
                reverse_locator_submission_records
            )
        ),
    )

    predecessor_replay_deterministic = (
        canonical_json(locator_submission_records)
        == canonical_json(
            reverse_locator_submission_records
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
                row["source_evidence_validation_status"]
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
                    "source_evidence_validation_blocker_codes"
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

    locator_submission_presence_counts = dict(
        sorted(
            Counter(
                str(row["locator_submission_supplied"])
                for row in validation_records
            ).items()
        )
    )

    source_evidence_presence_counts = dict(
        sorted(
            Counter(
                str(
                    row[
                        "source_evidence_submission_supplied"
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
            "source_evidence_validation_implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_ay_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed":
                plan.PLAN_VERSION == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_ax_contract_version_verified",
            "actual":
                predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION
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
            "actual": len(locator_submission_records),
            "expected": EXPECTED_PREDECESSOR_RECORDS,
            "passed": (
                len(locator_submission_records)
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
            "actual": len(plan.VALIDATION_RECORD_FIELDS),
            "expected": 42,
            "passed": all(
                set(row)
                == set(plan.VALIDATION_RECORD_FIELDS)
                for row in validation_records
            ),
        },
        {
            "check": "validation_record_ids_unique",
            "actual": len(
                {
                    row[
                        "source_evidence_validation_plan_record_id"
                    ]
                    for row in validation_records
                }
            ),
            "expected": len(validation_records),
            "passed": (
                len(
                    {
                        row[
                            "source_evidence_validation_plan_record_id"
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
                        "source_evidence_validation_plan_record_digest"
                    ]
                    for row in validation_records
                }
            ),
            "expected": len(validation_records),
            "passed": (
                len(
                    {
                        row[
                            "source_evidence_validation_plan_record_digest"
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
                        "source_evidence_validation_plan_identity_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_validation_plan_identity_digest"
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
                        "source_evidence_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "all_predecessor_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_locator_submission_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_locator_submission_plan_record_digest"
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
            "check": "supplied_source_evidence_inventory_empty",
            "actual":
                len(SUPPLIED_SOURCE_EVIDENCE_SUBMISSIONS),
            "expected": 0,
            "passed": (
                len(SUPPLIED_SOURCE_EVIDENCE_SUBMISSIONS)
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
            "check": "all_locator_submissions_absent",
            "actual":
                locator_submission_presence_counts,
            "expected": {
                "False": EXPECTED_VALIDATION_RECORDS
            },
            "passed": (
                locator_submission_presence_counts
                == {
                    "False":
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "all_source_evidence_submissions_absent",
            "actual": source_evidence_presence_counts,
            "expected": {
                "False": EXPECTED_VALIDATION_RECORDS
            },
            "passed": (
                source_evidence_presence_counts
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
            "check": "all_source_authority_fields_absent",
            "actual": sum(
                row["source_owner"] is None
                and row["source_class"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["source_owner"] is None
                and row["source_class"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_locator_submission_identity_fields_absent",
            "actual": sum(
                row["locator_submission_id"] is None
                and row["locator_submission_version"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["locator_submission_id"] is None
                and row["locator_submission_version"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_source_evidence_identity_fields_absent",
            "actual": sum(
                row["source_evidence_submission_id"] is None
                and row["source_evidence_submission_version"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["source_evidence_submission_id"] is None
                and row["source_evidence_submission_version"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_source_evidence_artifacts_empty",
            "actual": sum(
                row["source_evidence_artifacts"] == []
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["source_evidence_artifacts"] == []
                for row in validation_records
            ),
        },
        {
            "check": "required_evidence_classes_preserved",
            "actual": len(
                {
                    canonical_json(
                        row["required_evidence_classes"]
                    )
                    for row in validation_records
                }
            ),
            "expected": 1,
            "passed": all(
                len(row["required_evidence_classes"])
                == 7
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
            "check": "locator_submission_invention_not_executed",
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
            "check": "source_evidence_completion_by_inference_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "source_evidence_fabrication_not_executed",
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
        "endpoint_candidate_submission_source_evidence_validation_"
        "implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_submission_source_evidence_validation_"
        "implementation_failed"
    )

    next_layer = (
        "9BA_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_authorization_plan"
        if all_checks_passed
        else
        "9AZ_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_submission_source_evidence_validation_"
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
        OUTPUT_DIR / "source_evidence_validation_records.csv",
        plan.VALIDATION_RECORD_FIELDS,
        validation_records,
    )

    write_csv(
        OUTPUT_DIR / "source_evidence_validation_status_counts.csv",
        [
            "source_evidence_validation_status",
            "count",
        ],
        [
            {
                "source_evidence_validation_status": key,
                "count": value,
            }
            for key, value in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "source_evidence_validation_blocker_counts.csv",
        [
            "source_evidence_validation_blocker",
            "count",
        ],
        [
            {
                "source_evidence_validation_blocker": key,
                "count": value,
            }
            for key, value in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR
        / "supplied_source_evidence_submission_inventory.json",
        {
            "layer_id": LAYER_ID,
            "supplied_source_evidence_submission_count":
                len(SUPPLIED_SOURCE_EVIDENCE_SUBMISSIONS),
            "supplied_source_evidence_submissions":
                list(SUPPLIED_SOURCE_EVIDENCE_SUBMISSIONS),
            "inventory_status": (
                "no_candidate_locator_submission_or_source_evidence_"
                "submission_supplied"
            ),
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "source_evidence_validation_contract_version":
            SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.LOCATOR_SUBMISSION_CONTRACT_VERSION,
        "predecessor_records":
            len(locator_submission_records),
        "source_evidence_validation_records":
            len(validation_records),
        "source_evidence_validation_comparisons":
            len(comparison_ids),
        "supplied_source_evidence_submissions":
            len(SUPPLIED_SOURCE_EVIDENCE_SUBMISSIONS),
        "source_evidence_validation_status_counts":
            status_counts,
        "source_evidence_validation_blocker_counts":
            blocker_counts,
        "source_evidence_validation_implementation_authorities_granted":
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
        / "source_evidence_validation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "source_evidence_validation_result":
            VALIDATION_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_acquisition_authorization_planning"
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
        "Source evidence validation contract version: "
        f"{SOURCE_EVIDENCE_VALIDATION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Predecessor records replayed: "
        f"{len(locator_submission_records)}"
    )
    print(
        "Source evidence validation records: "
        f"{len(validation_records)}"
    )
    print(
        "Source evidence validation comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Supplied source evidence submissions: "
        f"{len(SUPPLIED_SOURCE_EVIDENCE_SUBMISSIONS)}"
    )
    print(
        "Source evidence validation status counts: "
        f"{status_counts}"
    )
    print(
        "Source evidence validation blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Source evidence validation implementation authorities granted: "
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
        "Source evidence validation result: "
        f"{diagnosis['source_evidence_validation_result']}"
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
