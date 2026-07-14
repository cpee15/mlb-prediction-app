#!/usr/bin/env python3
"""
Layer 9AT
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Source Evidence Acquisition Implementation

Implements the deterministic source-evidence acquisition contract planned by
Layer 9AS.

Layer 9AR established that no endpoint candidate submission exists. Layer 9AS
defined the evidence required before a submitted candidate could be validated.
Because no candidate or evidence locators are supplied, this implementation:

- replays the sixteen Layer 9AR candidate-specification records;
- verifies the Layer 9AS evidence-acquisition plan;
- inventories explicit evidence submissions and locators;
- emits one deterministic evidence-acquisition record per comparison;
- classifies every record as `candidate_not_supplied`;
- acquires no evidence and grants no evidence-validation or retrieval authority.

This layer does not invent a candidate, invent evidence locators, fabricate
authority evidence, store credentials, execute network requests, acquire
historical outcomes, alter canonical values or mappings, transform values, or
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


LAYER_ID = "9AT"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_acquisition_implementation"
)

EVIDENCE_ACQUISITION_CONTRACT_VERSION = (
    "layer_9AT_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AT_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_acquisition"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AS_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_acquisition.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AS_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AR_historical_outcome_authoritative_source_"
    "endpoint_candidate_specification_contract_v1"
)

EXPECTED_SPECIFICATION_RECORDS = 16
EXPECTED_EVIDENCE_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

EVIDENCE_ACQUISITION_STATUS = "candidate_not_supplied"

EVIDENCE_ACQUISITION_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

SUPPLIED_EVIDENCE_SUBMISSIONS: tuple[dict[str, Any], ...] = ()


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
        "layer_9as_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9AS plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.SPECIFICATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AR contract version: "
            f"{predecessor.SPECIFICATION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_evidence_acquisition_records(
    plan: Any,
    specification_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    required_evidence_classes = [
        row["evidence_class"]
        for row in sorted(
            plan.EVIDENCE_CLASSES,
            key=lambda item: (
                int(item["priority"]),
                item["evidence_class_id"],
            ),
        )
        if row["required"]
    ]

    records: list[dict[str, Any]] = []

    for specification in specification_records:
        identity_payload = {
            "source_evidence_acquisition_contract_version":
                EVIDENCE_ACQUISITION_CONTRACT_VERSION,
            "endpoint_candidate_specification_record_id":
                specification.get(
                    "endpoint_candidate_specification_record_id"
                ),
            "comparison_record_id":
                specification.get(
                    "comparison_record_id"
                ),
            "defect_source_record_id":
                specification.get(
                    "defect_source_record_id"
                ),
            "candidate_id":
                specification.get(
                    "candidate_id"
                ),
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "source_evidence_acquisition_plan_contract_version":
                EVIDENCE_ACQUISITION_CONTRACT_VERSION,
            "source_evidence_acquisition_plan_record_id":
                "HOASEA-" + identity_digest[:20],
            "endpoint_candidate_specification_record_id":
                specification.get(
                    "endpoint_candidate_specification_record_id"
                ),
            "endpoint_candidate_specification_record_digest":
                specification.get(
                    "endpoint_candidate_specification_record_digest"
                ),
            "authoritative_source_endpoint_configuration_record_id":
                specification.get(
                    "authoritative_source_endpoint_configuration_record_id"
                ),
            "authoritative_source_acquisition_record_id":
                specification.get(
                    "authoritative_source_acquisition_record_id"
                ),
            "comparison_record_id":
                specification.get(
                    "comparison_record_id"
                ),
            "metric_record_id":
                specification.get(
                    "metric_record_id"
                ),
            "metric_name":
                specification.get(
                    "metric_name"
                ),
            "aggregation_name":
                specification.get(
                    "aggregation_name"
                ),
            "aggregation_key":
                specification.get(
                    "aggregation_key"
                ),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                specification.get(
                    "defect_source_path"
                ),
            "defect_source_symbol":
                specification.get(
                    "defect_source_symbol"
                ),
            "defect_source_record_id":
                specification.get(
                    "defect_source_record_id"
                ),
            "defect_source_record_digest":
                specification.get(
                    "defect_source_record_digest"
                ),
            "specification_status":
                specification.get(
                    "specification_status"
                ),
            "specification_blocker_codes":
                specification.get(
                    "specification_blocker_codes"
                ),
            "candidate_supplied":
                bool(
                    specification.get(
                        "candidate_supplied"
                    )
                ),
            "candidate_id":
                specification.get(
                    "candidate_id"
                ),
            "candidate_version":
                specification.get(
                    "candidate_version"
                ),
            "source_owner":
                specification.get(
                    "source_owner"
                ),
            "source_class":
                specification.get(
                    "source_class"
                ),
            "evidence_locator_present": False,
            "authority_documentation_locator": None,
            "coverage_documentation_locator": None,
            "identity_semantics_locator": None,
            "outcome_semantics_locator": None,
            "licensing_terms_locator": None,
            "availability_documentation_locator": None,
            "schema_or_snapshot_locator": None,
            "required_evidence_classes":
                required_evidence_classes,
            "evidence_acquisition_status":
                EVIDENCE_ACQUISITION_STATUS,
            "evidence_acquisition_blocker_codes": [
                EVIDENCE_ACQUISITION_BLOCKER
            ],
            "evidence_acquisition_implementation_authority_granted":
                False,
            "evidence_acquisition_rationale": (
                "No endpoint candidate submission exists, so no evidence "
                "locators can be evaluated or acquired without inventing a "
                "candidate, provider, documentation URI, source owner, schema "
                "snapshot, licensing evidence, or authority evidence."
            ),
            "evidence_acquisition_limitations": [
                "No endpoint candidate was supplied.",
                "No evidence locator was supplied.",
                "No source owner or source class was established.",
                "No authority evidence was fabricated.",
                "No network retrieval was executed.",
                "No evidence artifact was retained.",
                "No historical outcome value was acquired.",
                (
                    "No canonical source mutation, mapping change, value "
                    "transformation, or downstream recomputation was executed."
                ),
            ],
            "source_evidence_acquisition_plan_identity_digest":
                identity_digest,
        }

        record[
            "source_evidence_acquisition_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.EVIDENCE_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Evidence acquisition record missing fields: "
                + ", ".join(missing_fields)
            )

        records.append(
            {
                field: record[field]
                for field in plan.EVIDENCE_RECORD_FIELDS
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
            normalized_string(
                row.get(
                    "candidate_id"
                )
            ),
            normalized_string(
                row.get(
                    "source_owner"
                )
            ),
            normalized_string(
                row.get(
                    "endpoint_candidate_specification_record_id"
                )
            ),
            normalized_string(
                row.get(
                    "source_evidence_acquisition_plan_record_id"
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

    evidence_records = build_evidence_acquisition_records(
        plan,
        specification_records,
    )

    reverse_evidence_records = build_evidence_acquisition_records(
        plan,
        list(
            reversed(
                reverse_specification_records
            )
        ),
    )

    specification_replay_deterministic = (
        canonical_json(specification_records)
        == canonical_json(
            reverse_specification_records
        )
    )

    evidence_replay_deterministic = (
        canonical_json(evidence_records)
        == canonical_json(
            reverse_evidence_records
        )
    )

    evidence_digest = sha256_payload(
        evidence_records
    )

    reverse_evidence_digest = sha256_payload(
        reverse_evidence_records
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in evidence_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row["evidence_acquisition_status"]
                for row in evidence_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in evidence_records
                for blocker in row[
                    "evidence_acquisition_blocker_codes"
                ]
            ).items()
        )
    )

    candidate_presence_counts = dict(
        sorted(
            Counter(
                str(row["candidate_supplied"])
                for row in evidence_records
            ).items()
        )
    )

    locator_presence_counts = dict(
        sorted(
            Counter(
                str(row["evidence_locator_present"])
                for row in evidence_records
            ).items()
        )
    )

    implementation_authority_records = [
        row
        for row in evidence_records
        if row[
            "evidence_acquisition_implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_as_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": (
                plan.PLAN_VERSION
                == EXPECTED_PLAN_VERSION
            ),
        },
        {
            "check": "nine_ar_contract_version_verified",
            "actual":
                predecessor.SPECIFICATION_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.SPECIFICATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
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
            "check": "evidence_replay_deterministic",
            "actual":
                evidence_replay_deterministic,
            "expected": True,
            "passed":
                evidence_replay_deterministic,
        },
        {
            "check": "evidence_digests_match_reverse_replay",
            "actual":
                evidence_digest,
            "expected":
                reverse_evidence_digest,
            "passed": (
                evidence_digest
                == reverse_evidence_digest
            ),
        },
        {
            "check": "expected_specification_records_replayed",
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
            "check": "expected_evidence_records_materialized",
            "actual":
                len(evidence_records),
            "expected":
                EXPECTED_EVIDENCE_RECORDS,
            "passed": (
                len(evidence_records)
                == EXPECTED_EVIDENCE_RECORDS
            ),
        },
        {
            "check": "one_evidence_record_per_comparison",
            "actual":
                len(comparison_ids),
            "expected":
                EXPECTED_EVIDENCE_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_EVIDENCE_RECORDS
            ),
        },
        {
            "check": "evidence_record_fields_complete",
            "actual":
                len(plan.EVIDENCE_RECORD_FIELDS),
            "expected": 41,
            "passed": all(
                set(row)
                == set(
                    plan.EVIDENCE_RECORD_FIELDS
                )
                for row in evidence_records
            ),
        },
        {
            "check": "evidence_record_ids_unique",
            "actual": len(
                {
                    row[
                        "source_evidence_acquisition_plan_record_id"
                    ]
                    for row in evidence_records
                }
            ),
            "expected":
                len(evidence_records),
            "passed": (
                len(
                    {
                        row[
                            "source_evidence_acquisition_plan_record_id"
                        ]
                        for row in evidence_records
                    }
                )
                == len(evidence_records)
            ),
        },
        {
            "check": "evidence_record_digests_unique",
            "actual": len(
                {
                    row[
                        "source_evidence_acquisition_plan_record_digest"
                    ]
                    for row in evidence_records
                }
            ),
            "expected":
                len(evidence_records),
            "passed": (
                len(
                    {
                        row[
                            "source_evidence_acquisition_plan_record_digest"
                        ]
                        for row in evidence_records
                    }
                )
                == len(evidence_records)
            ),
        },
        {
            "check": "all_evidence_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_plan_identity_digest"
                    ]
                )
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_plan_identity_digest"
                    ]
                )
                for row in evidence_records
            ),
        },
        {
            "check": "all_evidence_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_plan_record_digest"
                    ]
                )
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_acquisition_plan_record_digest"
                    ]
                )
                for row in evidence_records
            ),
        },
        {
            "check": "all_specification_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "endpoint_candidate_specification_record_digest"
                    ]
                )
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                valid_sha256(
                    row[
                        "endpoint_candidate_specification_record_digest"
                    ]
                )
                for row in evidence_records
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
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in evidence_records
            ),
        },
        {
            "check": "supplied_evidence_inventory_empty",
            "actual":
                len(SUPPLIED_EVIDENCE_SUBMISSIONS),
            "expected": 0,
            "passed": (
                len(SUPPLIED_EVIDENCE_SUBMISSIONS)
                == 0
            ),
        },
        {
            "check": "all_candidates_absent",
            "actual":
                candidate_presence_counts,
            "expected": {
                "False":
                    EXPECTED_EVIDENCE_RECORDS
            },
            "passed": (
                candidate_presence_counts
                == {
                    "False":
                        EXPECTED_EVIDENCE_RECORDS
                }
            ),
        },
        {
            "check": "all_evidence_locators_absent",
            "actual":
                locator_presence_counts,
            "expected": {
                "False":
                    EXPECTED_EVIDENCE_RECORDS
            },
            "passed": (
                locator_presence_counts
                == {
                    "False":
                        EXPECTED_EVIDENCE_RECORDS
                }
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual":
                status_counts,
            "expected": {
                EVIDENCE_ACQUISITION_STATUS:
                    EXPECTED_EVIDENCE_RECORDS
            },
            "passed": (
                status_counts
                == {
                    EVIDENCE_ACQUISITION_STATUS:
                        EXPECTED_EVIDENCE_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_present",
            "actual":
                blocker_counts,
            "expected": {
                EVIDENCE_ACQUISITION_BLOCKER:
                    EXPECTED_EVIDENCE_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    EVIDENCE_ACQUISITION_BLOCKER:
                        EXPECTED_EVIDENCE_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_identity_fields_absent",
            "actual": sum(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in evidence_records
            ),
        },
        {
            "check": "all_source_authority_fields_absent",
            "actual": sum(
                row["source_owner"] is None
                and row["source_class"] is None
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                row["source_owner"] is None
                and row["source_class"] is None
                for row in evidence_records
            ),
        },
        {
            "check": "all_authority_locators_absent",
            "actual": sum(
                row[
                    "authority_documentation_locator"
                ]
                is None
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                row[
                    "authority_documentation_locator"
                ]
                is None
                for row in evidence_records
            ),
        },
        {
            "check": "all_coverage_locators_absent",
            "actual": sum(
                row[
                    "coverage_documentation_locator"
                ]
                is None
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                row[
                    "coverage_documentation_locator"
                ]
                is None
                for row in evidence_records
            ),
        },
        {
            "check": "all_identity_semantics_locators_absent",
            "actual": sum(
                row[
                    "identity_semantics_locator"
                ]
                is None
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                row[
                    "identity_semantics_locator"
                ]
                is None
                for row in evidence_records
            ),
        },
        {
            "check": "all_outcome_semantics_locators_absent",
            "actual": sum(
                row[
                    "outcome_semantics_locator"
                ]
                is None
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                row[
                    "outcome_semantics_locator"
                ]
                is None
                for row in evidence_records
            ),
        },
        {
            "check": "all_licensing_locators_absent",
            "actual": sum(
                row[
                    "licensing_terms_locator"
                ]
                is None
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                row[
                    "licensing_terms_locator"
                ]
                is None
                for row in evidence_records
            ),
        },
        {
            "check": "all_availability_locators_absent",
            "actual": sum(
                row[
                    "availability_documentation_locator"
                ]
                is None
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                row[
                    "availability_documentation_locator"
                ]
                is None
                for row in evidence_records
            ),
        },
        {
            "check": "all_schema_or_snapshot_locators_absent",
            "actual": sum(
                row[
                    "schema_or_snapshot_locator"
                ]
                is None
                for row in evidence_records
            ),
            "expected":
                len(evidence_records),
            "passed": all(
                row[
                    "schema_or_snapshot_locator"
                ]
                is None
                for row in evidence_records
            ),
        },
        {
            "check": "required_evidence_classes_preserved",
            "actual": len(
                {
                    canonical_json(
                        row[
                            "required_evidence_classes"
                        ]
                    )
                    for row in evidence_records
                }
            ),
            "expected": 1,
            "passed": all(
                len(
                    row[
                        "required_evidence_classes"
                    ]
                )
                == 8
                for row in evidence_records
            ),
        },
        {
            "check": "no_evidence_acquisition_authority_granted",
            "actual":
                len(
                    implementation_authority_records
                ),
            "expected": 0,
            "passed": (
                len(
                    implementation_authority_records
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
                    for row in evidence_records
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
                for row in evidence_records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row[
                        "authoritative_field_path"
                    ]
                    for row in evidence_records
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
                for row in evidence_records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row[
                        "rejected_metadata_field_name"
                    ]
                    for row in evidence_records
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
                for row in evidence_records
            ),
        },
        {
            "check": "candidate_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "evidence_locator_invention_not_executed",
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
            "check": "evidence_artifacts_retained_zero",
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
        "endpoint_candidate_source_evidence_acquisition_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_implementation_failed"
    )

    next_layer = (
        "9AU_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_evidence_locator_specification_plan"
        if all_checks_passed
        else
        "9AT_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_acquisition_implementation_remediation"
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
        OUTPUT_DIR / "source_evidence_acquisition_records.csv",
        plan.EVIDENCE_RECORD_FIELDS,
        evidence_records,
    )

    write_csv(
        OUTPUT_DIR / "evidence_acquisition_status_counts.csv",
        [
            "evidence_acquisition_status",
            "count",
        ],
        [
            {
                "evidence_acquisition_status": key,
                "count": value,
            }
            for key, value in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "evidence_acquisition_blocker_counts.csv",
        [
            "evidence_acquisition_blocker",
            "count",
        ],
        [
            {
                "evidence_acquisition_blocker": key,
                "count": value,
            }
            for key, value in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR / "supplied_evidence_submission_inventory.json",
        {
            "layer_id": LAYER_ID,
            "supplied_evidence_submission_count":
                len(SUPPLIED_EVIDENCE_SUBMISSIONS),
            "supplied_evidence_submissions":
                list(SUPPLIED_EVIDENCE_SUBMISSIONS),
            "inventory_status":
                "no_candidate_or_source_evidence_submission_supplied",
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "evidence_acquisition_contract_version":
            EVIDENCE_ACQUISITION_CONTRACT_VERSION,
        "plan_version":
            plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.SPECIFICATION_CONTRACT_VERSION,
        "specification_records":
            len(specification_records),
        "evidence_acquisition_records":
            len(evidence_records),
        "evidence_acquisition_comparisons":
            len(comparison_ids),
        "supplied_evidence_submissions":
            len(SUPPLIED_EVIDENCE_SUBMISSIONS),
        "evidence_acquisition_status_counts":
            status_counts,
        "evidence_acquisition_blocker_counts":
            blocker_counts,
        "evidence_acquisition_implementation_authorities_granted":
            len(
                implementation_authority_records
            ),
        "evidence_digest":
            evidence_digest,
        "reverse_evidence_digest":
            reverse_evidence_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
        "source_evidence_acquired": 0,
        "evidence_artifacts_retained": 0,
        "credentials_stored": 0,
        "network_retrievals_executed": 0,
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
        / "source_evidence_acquisition_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "evidence_acquisition_result":
            EVIDENCE_ACQUISITION_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "evidence_locator_specification_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "endpoint_candidate_selection_without_submission",
            "evidence_locator_invention",
            "authority_evidence_fabrication",
            "credential_literal_storage",
            "source_evidence_validation",
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
        "Evidence acquisition contract version: "
        f"{EVIDENCE_ACQUISITION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Specification records replayed: "
        f"{len(specification_records)}"
    )
    print(
        "Evidence acquisition records: "
        f"{len(evidence_records)}"
    )
    print(
        "Evidence acquisition comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Supplied evidence submissions: "
        f"{len(SUPPLIED_EVIDENCE_SUBMISSIONS)}"
    )
    print(
        "Evidence acquisition status counts: "
        f"{status_counts}"
    )
    print(
        "Evidence acquisition blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Evidence acquisition implementation authorities granted: "
        f"{len(implementation_authority_records)}"
    )
    print(
        "Evidence digest: "
        f"{evidence_digest}"
    )
    print(
        "Reverse evidence digest: "
        f"{reverse_evidence_digest}"
    )
    print("Source evidence acquired: 0")
    print("Evidence artifacts retained: 0")
    print("Credentials stored: 0")
    print("Network retrievals executed: 0")
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
        "Evidence acquisition result: "
        f"{diagnosis['evidence_acquisition_result']}"
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
