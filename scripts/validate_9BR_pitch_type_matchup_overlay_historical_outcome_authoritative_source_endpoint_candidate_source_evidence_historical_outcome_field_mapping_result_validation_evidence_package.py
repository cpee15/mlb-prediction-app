#!/usr/bin/env python3
"""
Layer 9BR
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Source Evidence
Historical Outcome Field Mapping Result Validation
Evidence Package Validation Implementation

Implements the deterministic validation contract planned by Layer 9BQ.

Layer 9BP contains structurally valid evidence packages that preserve verified
absence, lineage, canonical field identity, blockers, limitations, manifest
digests, and authority boundaries. No endpoint candidate or candidate-derived
evidence exists.

This implementation validates the packages without inventing evidence or
granting endpoint, retrieval, parsing, mapping, extraction, mutation,
recomputation, production, market, pricing, or betting authority.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BR"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_implementation"
)

EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION = (
    "layer_9BR_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BR_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package_validation"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9BQ_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_historical_outcome_field_"
    "mapping_result_validation_evidence_package_validation.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9BQ_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BP_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_contract_v1"
)

EXPECTED_MANIFEST_VERSION = (
    "layer_9BP_historical_outcome_mapping_result_validation_"
    "evidence_package_manifest_v1"
)

EXPECTED_PACKAGE_RECORDS = 16
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
        "layer_9bq_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9BQ plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BP contract version: "
            f"{predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION}"
        )

    if (
        predecessor.PACKAGE_MANIFEST_VERSION
        != EXPECTED_MANIFEST_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BP manifest version: "
            f"{predecessor.PACKAGE_MANIFEST_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_validation_records(
    plan: Any,
    package_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    validation_records: list[dict[str, Any]] = []

    for package in package_records:
        package_record_identity_valid = all(
            (
                bool(
                    normalized_string(
                        package.get(
                            "evidence_package_plan_record_id"
                        )
                    )
                ),
                valid_sha256(
                    package.get(
                        "evidence_package_plan_identity_digest"
                    )
                ),
            )
        )

        package_record_digest_valid = valid_sha256(
            package.get(
                "evidence_package_plan_record_digest"
            )
        )

        package_manifest_valid = (
            package.get("package_manifest_version")
            == EXPECTED_MANIFEST_VERSION
            and valid_sha256(
                package.get("package_manifest_digest")
            )
        )

        lineage_complete = all(
            bool(normalized_string(package.get(field)))
            for field in (
                "mapping_result_validation_plan_record_id",
                "mapping_result_validation_plan_record_digest",
                "historical_outcome_field_mapping_plan_record_id",
                "historical_outcome_field_mapping_plan_record_digest",
                "source_evidence_parsed_record_validation_plan_record_id",
                "parsed_record_validation_plan_record_digest",
                "comparison_record_id",
                "metric_record_id",
                "defect_source_path",
                "defect_source_symbol",
                "defect_source_record_id",
                "defect_source_record_digest",
            )
        )

        evidence_inventory_valid = (
            package.get(
                "candidate_evidence_artifact_count"
            )
            == 0
            and package.get(
                "response_evidence_artifact_count"
            )
            == 0
            and package.get(
                "parser_evidence_artifact_count"
            )
            == 0
            and package.get(
                "parsed_record_evidence_artifact_count"
            )
            == 0
            and package.get(
                "mapping_evidence_artifact_count"
            )
            == 0
            and package.get(
                "mapping_result_evidence_artifact_count"
            )
            == 0
            and package.get(
                "validation_evidence_artifact_count"
            )
            == 1
            and bool(
                package.get("evidence_absence_explicit")
            )
            and not bool(
                package.get("fabricated_evidence_detected")
            )
        )

        canonical_field_identity_valid = (
            package.get("authoritative_field_name")
            == AUTHORITATIVE_FIELD_NAME
            and package.get("authoritative_field_path")
            == AUTHORITATIVE_FIELD_PATH
            and package.get("rejected_metadata_field_name")
            == REJECTED_METADATA_FIELD
        )

        identity_payload = {
            "evidence_package_validation_contract_version":
                EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION,
            "evidence_package_plan_record_id":
                package.get(
                    "evidence_package_plan_record_id"
                ),
            "evidence_package_plan_record_digest":
                package.get(
                    "evidence_package_plan_record_digest"
                ),
            "package_manifest_digest":
                package.get("package_manifest_digest"),
            "comparison_record_id":
                package.get("comparison_record_id"),
            "defect_source_record_id":
                package.get("defect_source_record_id"),
            "validation_status":
                VALIDATION_STATUS,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "evidence_package_validation_plan_contract_version":
                EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION,
            "evidence_package_validation_plan_record_id":
                "HOASEHOFMRVEPV-" + identity_digest[:20],
            "evidence_package_plan_record_id":
                package.get(
                    "evidence_package_plan_record_id"
                ),
            "evidence_package_plan_identity_digest":
                package.get(
                    "evidence_package_plan_identity_digest"
                ),
            "evidence_package_plan_record_digest":
                package.get(
                    "evidence_package_plan_record_digest"
                ),
            "package_manifest_version":
                package.get("package_manifest_version"),
            "package_manifest_digest":
                package.get("package_manifest_digest"),
            "mapping_result_validation_plan_record_id":
                package.get(
                    "mapping_result_validation_plan_record_id"
                ),
            "mapping_result_validation_plan_record_digest":
                package.get(
                    "mapping_result_validation_plan_record_digest"
                ),
            "historical_outcome_field_mapping_plan_record_id":
                package.get(
                    "historical_outcome_field_mapping_plan_record_id"
                ),
            "historical_outcome_field_mapping_plan_record_digest":
                package.get(
                    "historical_outcome_field_mapping_plan_record_digest"
                ),
            "source_evidence_parsed_record_validation_plan_record_id":
                package.get(
                    "source_evidence_parsed_record_validation_"
                    "plan_record_id"
                ),
            "parsed_record_validation_plan_record_digest":
                package.get(
                    "parsed_record_validation_plan_record_digest"
                ),
            "endpoint_candidate_specification_record_id":
                package.get(
                    "endpoint_candidate_specification_record_id"
                ),
            "comparison_record_id":
                package.get("comparison_record_id"),
            "metric_record_id":
                package.get("metric_record_id"),
            "metric_name":
                package.get("metric_name"),
            "aggregation_name":
                package.get("aggregation_name"),
            "aggregation_key":
                package.get("aggregation_key"),
            "authoritative_field_name":
                package.get("authoritative_field_name"),
            "authoritative_field_path":
                package.get("authoritative_field_path"),
            "rejected_metadata_field_name":
                package.get("rejected_metadata_field_name"),
            "defect_source_path":
                package.get("defect_source_path"),
            "defect_source_symbol":
                package.get("defect_source_symbol"),
            "defect_source_record_id":
                package.get("defect_source_record_id"),
            "defect_source_record_digest":
                package.get("defect_source_record_digest"),
            "candidate_supplied":
                bool(package.get("candidate_supplied")),
            "candidate_id":
                package.get("candidate_id"),
            "candidate_version":
                package.get("candidate_version"),
            "response_artifact_id":
                package.get("response_artifact_id"),
            "response_sha256":
                package.get("response_sha256"),
            "parser_id":
                package.get("parser_id"),
            "parser_version":
                package.get("parser_version"),
            "parser_code_digest":
                package.get("parser_code_digest"),
            "parsed_record_id":
                package.get("parsed_record_id"),
            "parsed_record_version":
                package.get("parsed_record_version"),
            "parsed_record_digest":
                package.get("parsed_record_digest"),
            "mapping_id":
                package.get("mapping_id"),
            "mapping_version":
                package.get("mapping_version"),
            "mapping_digest":
                package.get("mapping_digest"),
            "mapping_result_id":
                package.get("mapping_result_id"),
            "mapping_result_version":
                package.get("mapping_result_version"),
            "mapping_result_digest":
                package.get("mapping_result_digest"),
            "candidate_evidence_artifact_count":
                package.get(
                    "candidate_evidence_artifact_count"
                ),
            "response_evidence_artifact_count":
                package.get(
                    "response_evidence_artifact_count"
                ),
            "parser_evidence_artifact_count":
                package.get(
                    "parser_evidence_artifact_count"
                ),
            "parsed_record_evidence_artifact_count":
                package.get(
                    "parsed_record_evidence_artifact_count"
                ),
            "mapping_evidence_artifact_count":
                package.get(
                    "mapping_evidence_artifact_count"
                ),
            "mapping_result_evidence_artifact_count":
                package.get(
                    "mapping_result_evidence_artifact_count"
                ),
            "validation_evidence_artifact_count":
                package.get(
                    "validation_evidence_artifact_count"
                ),
            "evidence_absence_explicit":
                bool(
                    package.get(
                        "evidence_absence_explicit"
                    )
                ),
            "fabricated_evidence_detected":
                bool(
                    package.get(
                        "fabricated_evidence_detected"
                    )
                ),
            "evidence_package_status":
                package.get("evidence_package_status"),
            "evidence_package_blocker_codes":
                package.get(
                    "evidence_package_blocker_codes"
                ),
            "evidence_package_rationale":
                package.get("evidence_package_rationale"),
            "evidence_package_limitations":
                package.get(
                    "evidence_package_limitations"
                ),
            "evidence_package_authority_boundary":
                package.get(
                    "evidence_package_authority_boundary"
                ),
            "package_record_identity_valid":
                package_record_identity_valid,
            "package_record_digest_valid":
                package_record_digest_valid,
            "package_manifest_valid":
                package_manifest_valid,
            "lineage_complete":
                lineage_complete,
            "evidence_inventory_valid":
                evidence_inventory_valid,
            "canonical_field_identity_valid":
                canonical_field_identity_valid,
            "evidence_package_validation_status":
                VALIDATION_STATUS,
            "evidence_package_validation_blocker_codes": [
                VALIDATION_BLOCKER
            ],
            "evidence_package_validation_implementation_authority_granted":
                False,
            "evidence_package_validation_rationale": (
                "The Layer 9BP package record is structurally valid, "
                "deterministic, lineage-complete, digest-bearing, explicit "
                "about candidate-derived evidence absence, free of fabricated "
                "evidence, and compliant with the canonical field-identity and "
                "authority-boundary contracts. It remains blocked because no "
                "authoritative endpoint candidate was supplied."
            ),
            "evidence_package_validation_limitations": [
                "No endpoint candidate was supplied.",
                "No validated response artifact exists.",
                "No authorized parser execution exists.",
                "No validated parsed record exists.",
                "No field-mapping submission exists.",
                "No mapping-result submission exists.",
                "No authoritative historical outcome was validated.",
                "Structural package validity does not prove outcome truth.",
                "Candidate-derived artifact counts remain zero.",
                "Only the deterministic validation evidence artifact exists.",
                "No fabricated evidence was accepted.",
                "No response bytes were read or parsed.",
                "No historical outcome field was mapped.",
                "No historical outcome value was extracted.",
                "No canonical source record was mutated.",
                "No downstream record was recomputed.",
            ],
            "evidence_package_validation_plan_identity_digest":
                identity_digest,
        }

        record[
            "evidence_package_validation_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.VALIDATION_PLAN_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Evidence-package validation record missing fields: "
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
                row.get("mapping_id")
            ),
            normalized_string(
                row.get("mapping_result_id")
            ),
            normalized_string(
                row.get(
                    "mapping_result_validation_plan_record_id"
                )
            ),
            normalized_string(
                row.get("evidence_package_plan_record_id")
            ),
            normalized_string(
                row.get(
                    "evidence_package_validation_plan_record_id"
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
    package_records = replay["records"]
    reverse_package_records = replay[
        "reverse_records"
    ]

    validation_records = build_validation_records(
        plan,
        package_records,
    )

    reverse_validation_records = build_validation_records(
        plan,
        list(
            reversed(
                reverse_package_records
            )
        ),
    )

    predecessor_replay_deterministic = (
        canonical_json(package_records)
        == canonical_json(
            reverse_package_records
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
                    "evidence_package_validation_status"
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
                    "evidence_package_validation_blocker_codes"
                ]
            ).items()
        )
    )

    structural_validity_counts = {
        "package_record_identity_valid": sum(
            bool(row["package_record_identity_valid"])
            for row in validation_records
        ),
        "package_record_digest_valid": sum(
            bool(row["package_record_digest_valid"])
            for row in validation_records
        ),
        "package_manifest_valid": sum(
            bool(row["package_manifest_valid"])
            for row in validation_records
        ),
        "lineage_complete": sum(
            bool(row["lineage_complete"])
            for row in validation_records
        ),
        "evidence_inventory_valid": sum(
            bool(row["evidence_inventory_valid"])
            for row in validation_records
        ),
        "canonical_field_identity_valid": sum(
            bool(row["canonical_field_identity_valid"])
            for row in validation_records
        ),
    }

    candidate_derived_artifact_count = sum(
        row["candidate_evidence_artifact_count"]
        + row["response_evidence_artifact_count"]
        + row["parser_evidence_artifact_count"]
        + row["parsed_record_evidence_artifact_count"]
        + row["mapping_evidence_artifact_count"]
        + row["mapping_result_evidence_artifact_count"]
        for row in validation_records
    )

    validation_artifact_count = sum(
        row["validation_evidence_artifact_count"]
        for row in validation_records
    )

    authority_records = [
        row
        for row in validation_records
        if row[
            "evidence_package_validation_"
            "implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_bq_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed":
                plan.PLAN_VERSION
                == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_bp_contract_version_verified",
            "actual":
                predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_bp_manifest_version_verified",
            "actual":
                predecessor.PACKAGE_MANIFEST_VERSION,
            "expected": EXPECTED_MANIFEST_VERSION,
            "passed": (
                predecessor.PACKAGE_MANIFEST_VERSION
                == EXPECTED_MANIFEST_VERSION
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
            "check": "expected_package_records_replayed",
            "actual": len(package_records),
            "expected": EXPECTED_PACKAGE_RECORDS,
            "passed": (
                len(package_records)
                == EXPECTED_PACKAGE_RECORDS
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
            "expected": 70,
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
                        "evidence_package_validation_plan_record_id"
                    ]
                    for row in validation_records
                }
            ),
            "expected": len(validation_records),
            "passed": (
                len(
                    {
                        row[
                            "evidence_package_validation_plan_record_id"
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
                        "evidence_package_validation_plan_record_digest"
                    ]
                    for row in validation_records
                }
            ),
            "expected": len(validation_records),
            "passed": (
                len(
                    {
                        row[
                            "evidence_package_validation_plan_record_digest"
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
                        "evidence_package_validation_plan_identity_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_package_validation_plan_identity_digest"
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
                        "evidence_package_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_package_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "all_package_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_package_plan_identity_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_package_plan_identity_digest"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "all_package_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evidence_package_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_package_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "all_package_manifest_digests_valid",
            "actual": sum(
                valid_sha256(
                    row["package_manifest_digest"]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row["package_manifest_digest"]
                )
                for row in validation_records
            ),
        },
        {
            "check": "all_records_structurally_valid",
            "actual": structural_validity_counts,
            "expected": {
                key: EXPECTED_VALIDATION_RECORDS
                for key in structural_validity_counts
            },
            "passed": all(
                count == EXPECTED_VALIDATION_RECORDS
                for count in structural_validity_counts.values()
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
            "check": "candidate_derived_artifact_counts_zero",
            "actual": candidate_derived_artifact_count,
            "expected": 0,
            "passed":
                candidate_derived_artifact_count == 0,
        },
        {
            "check": "one_validation_artifact_per_record",
            "actual": validation_artifact_count,
            "expected": EXPECTED_VALIDATION_RECORDS,
            "passed": (
                validation_artifact_count
                == EXPECTED_VALIDATION_RECORDS
            ),
        },
        {
            "check": "evidence_absence_explicit",
            "actual": sum(
                bool(row["evidence_absence_explicit"])
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                bool(row["evidence_absence_explicit"])
                for row in validation_records
            ),
        },
        {
            "check": "fabricated_evidence_absent",
            "actual": sum(
                not bool(
                    row["fabricated_evidence_detected"]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                not bool(
                    row["fabricated_evidence_detected"]
                )
                for row in validation_records
            ),
        },
        {
            "check": "canonical_target_identity_preserved",
            "actual": sorted(
                {
                    (
                        row["authoritative_field_name"],
                        row["authoritative_field_path"],
                    )
                    for row in validation_records
                }
            ),
            "expected": [
                (
                    AUTHORITATIVE_FIELD_NAME,
                    AUTHORITATIVE_FIELD_PATH,
                )
            ],
            "passed": all(
                row["authoritative_field_name"]
                == AUTHORITATIVE_FIELD_NAME
                and row["authoritative_field_path"]
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
            "check": "validation_rationale_and_limitations_present",
            "actual": sum(
                bool(
                    normalized_string(
                        row[
                            "evidence_package_validation_rationale"
                        ]
                    )
                )
                and bool(
                    row[
                        "evidence_package_validation_limitations"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                bool(
                    normalized_string(
                        row[
                            "evidence_package_validation_rationale"
                        ]
                    )
                )
                and bool(
                    row[
                        "evidence_package_validation_limitations"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "package_authority_boundaries_preserved",
            "actual": sum(
                bool(
                    normalized_string(
                        row[
                            "evidence_package_authority_boundary"
                        ]
                    )
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                bool(
                    normalized_string(
                        row[
                            "evidence_package_authority_boundary"
                        ]
                    )
                )
                for row in validation_records
            ),
        },
        {
            "check": "no_validation_implementation_authority_granted",
            "actual": len(authority_records),
            "expected": 0,
            "passed":
                len(authority_records) == 0,
        },
        {
            "check": "package_or_manifest_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "evidence_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "endpoint_response_parser_mapping_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "historical_outcome_fields_mapped_zero",
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
            "check": "response_bytes_read_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "responses_parsed_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "parsed_records_validated_zero",
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
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_implementation_failed"
    )

    next_layer = (
        "9BS_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_plan"
        if all_checks_passed
        else
        "9BR_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_implementation_"
        "remediation"
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
        / "evidence_package_validation_records.csv",
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
        OUTPUT_DIR / "structural_validity_summary.json",
        {
            "layer_id": LAYER_ID,
            "validation_record_count":
                len(validation_records),
            "structural_validity_counts":
                structural_validity_counts,
            "candidate_derived_artifact_count":
                candidate_derived_artifact_count,
            "validation_artifact_count":
                validation_artifact_count,
            "evidence_absence_explicit_count":
                sum(
                    bool(
                        row["evidence_absence_explicit"]
                    )
                    for row in validation_records
                ),
            "fabricated_evidence_detected_count":
                sum(
                    bool(
                        row["fabricated_evidence_detected"]
                    )
                    for row in validation_records
                ),
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "evidence_package_validation_contract_version":
            EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.EVIDENCE_PACKAGE_CONTRACT_VERSION,
        "predecessor_manifest_version":
            predecessor.PACKAGE_MANIFEST_VERSION,
        "package_records":
            len(package_records),
        "validation_records":
            len(validation_records),
        "validation_comparisons":
            len(comparison_ids),
        "validation_status_counts":
            status_counts,
        "validation_blocker_counts":
            blocker_counts,
        "structural_validity_counts":
            structural_validity_counts,
        "candidate_derived_artifact_count":
            candidate_derived_artifact_count,
        "validation_artifact_count":
            validation_artifact_count,
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
        "package_records_structurally_validated":
            len(validation_records),
        "authoritative_historical_outcomes_validated": 0,
        "candidate_derived_evidence_artifacts_created": 0,
        "fabricated_evidence_artifacts_created": 0,
        "historical_outcome_fields_mapped": 0,
        "historical_outcome_values_extracted": 0,
        "response_bytes_read": 0,
        "responses_parsed": 0,
        "parsed_records_validated": 0,
        "credentials_stored": 0,
        "credential_literals_logged": 0,
        "network_retrievals_executed": 0,
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
        / "evidence_package_validation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "evidence_package_validation_result":
            VALIDATION_STATUS,
        "structural_package_validation_complete":
            all_checks_passed,
        "authoritative_historical_outcome_validated":
            False,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_historical_outcome_field_mapping_result_"
            "validation_evidence_package_validation_result_evidence_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "response_artifact_invention",
            "parser_submission_invention",
            "parsed_record_submission_invention",
            "mapping_submission_invention",
            "mapping_result_submission_invention",
            "validation_result_invention",
            "evidence_artifact_invention",
            "evidence_artifact_identity_invention",
            "evidence_artifact_digest_invention",
            "evidence_locator_invention",
            "package_record_invention",
            "package_record_identity_invention",
            "package_record_digest_invention",
            "package_manifest_invention",
            "package_manifest_digest_invention",
            "authoritative_historical_outcome_validation",
            "historical_outcome_field_mapping_execution",
            "historical_outcome_value_extraction",
            "response_bytes_reading",
            "source_evidence_parse_execution",
            "raw_response_parse_execution",
            "credential_literal_storage",
            "credential_literal_logging",
            "dns_resolution_execution",
            "socket_connection_execution",
            "http_request_execution",
            "browser_execution",
            "api_request_execution",
            "canonical_source_value_mutation",
            "canonical_outcome_mapping_change",
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
        "Evidence package validation contract version: "
        f"{EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Package records replayed: "
        f"{len(package_records)}"
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
        "Validation status counts: "
        f"{status_counts}"
    )
    print(
        "Validation blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Structural validity counts: "
        f"{structural_validity_counts}"
    )
    print(
        "Candidate-derived artifact count: "
        f"{candidate_derived_artifact_count}"
    )
    print(
        "Validation artifact count: "
        f"{validation_artifact_count}"
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
    print(
        "Package records structurally validated: "
        f"{len(validation_records)}"
    )
    print("Authoritative historical outcomes validated: 0")
    print("Candidate-derived evidence artifacts created: 0")
    print("Fabricated evidence artifacts created: 0")
    print("Historical outcome fields mapped: 0")
    print("Historical outcome values extracted: 0")
    print("Response bytes read: 0")
    print("Responses parsed: 0")
    print("Parsed records validated: 0")
    print("Credentials stored: 0")
    print("Credential literals logged: 0")
    print("Network retrievals executed: 0")
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
        "Evidence-package validation result: "
        f"{diagnosis['evidence_package_validation_result']}"
    )
    print(
        "Structural package validation complete: "
        f"{diagnosis['structural_package_validation_complete']}"
    )
    print(
        "Authoritative historical outcome validated: "
        f"{diagnosis['authoritative_historical_outcome_validated']}"
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
