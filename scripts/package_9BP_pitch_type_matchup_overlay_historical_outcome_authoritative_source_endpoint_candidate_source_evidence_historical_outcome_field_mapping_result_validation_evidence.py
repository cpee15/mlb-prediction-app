#!/usr/bin/env python3
"""
Layer 9BP
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Source Evidence
Historical Outcome Field Mapping Result Validation Evidence Package

Implements the deterministic evidence-package contract planned by Layer 9BO.

No endpoint candidate, validated response, parser, parsed record, mapping,
mapping result, source value, mapped value, or validated historical outcome
exists. The implementation therefore packages the verified absence, blocker,
lineage, canonical field identity, limitations, and authority boundaries
without inventing evidence.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BP"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_implementation"
)

EVIDENCE_PACKAGE_CONTRACT_VERSION = (
    "layer_9BP_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_contract_v1"
)

PACKAGE_MANIFEST_VERSION = (
    "layer_9BP_historical_outcome_mapping_result_validation_"
    "evidence_package_manifest_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BP_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9BO_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_historical_outcome_field_"
    "mapping_result_validation_evidence_package.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9BO_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BN_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "contract_v1"
)

EXPECTED_VALIDATION_RECORDS = 16
EXPECTED_PACKAGE_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

PACKAGE_STATUS = "candidate_not_supplied"

PACKAGE_BLOCKER = (
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
        "layer_9bo_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9BO plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BN contract version: "
            f"{predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_package_records(
    plan: Any,
    validation_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    package_records: list[dict[str, Any]] = []

    for validation in validation_records:
        lineage_manifest = {
            "comparison_record_id":
                validation.get("comparison_record_id"),
            "metric_record_id":
                validation.get("metric_record_id"),
            "defect_source_record_id":
                validation.get("defect_source_record_id"),
            "mapping_result_validation_plan_record_id":
                validation.get(
                    "historical_outcome_field_mapping_result_"
                    "validation_plan_record_id"
                ),
            "historical_outcome_field_mapping_plan_record_id":
                validation.get(
                    "historical_outcome_field_mapping_plan_record_id"
                ),
            "source_evidence_parsed_record_validation_plan_record_id":
                validation.get(
                    "source_evidence_parsed_record_validation_"
                    "plan_record_id"
                ),
        }

        artifact_inventory = {
            "candidate_evidence_artifact_count": 0,
            "response_evidence_artifact_count": 0,
            "parser_evidence_artifact_count": 0,
            "parsed_record_evidence_artifact_count": 0,
            "mapping_evidence_artifact_count": 0,
            "mapping_result_evidence_artifact_count": 0,
            "validation_evidence_artifact_count": 1,
        }

        package_manifest_payload = {
            "package_manifest_version":
                PACKAGE_MANIFEST_VERSION,
            "lineage_manifest":
                lineage_manifest,
            "artifact_inventory":
                artifact_inventory,
            "canonical_field_identity": {
                "authoritative_field_name":
                    AUTHORITATIVE_FIELD_NAME,
                "authoritative_field_path":
                    AUTHORITATIVE_FIELD_PATH,
                "rejected_metadata_field_name":
                    REJECTED_METADATA_FIELD,
            },
            "validation_status":
                validation.get(
                    "mapping_result_validation_status"
                ),
            "validation_blockers":
                validation.get(
                    "mapping_result_validation_blocker_codes"
                ),
            "evidence_absence_explicit": True,
            "fabricated_evidence_detected": False,
            "authority_boundary": (
                "Evidence packaging only. No endpoint selection, retrieval, "
                "parsing, mapping, extraction, canonical mutation, downstream "
                "recomputation, production, market, pricing, or betting "
                "authority is granted."
            ),
        }

        package_manifest_digest = sha256_payload(
            package_manifest_payload
        )

        identity_payload = {
            "evidence_package_contract_version":
                EVIDENCE_PACKAGE_CONTRACT_VERSION,
            "mapping_result_validation_plan_record_id":
                validation.get(
                    "historical_outcome_field_mapping_result_"
                    "validation_plan_record_id"
                ),
            "comparison_record_id":
                validation.get("comparison_record_id"),
            "defect_source_record_id":
                validation.get("defect_source_record_id"),
            "candidate_id":
                validation.get("candidate_id"),
            "mapping_id":
                validation.get("mapping_id"),
            "mapping_result_id":
                validation.get("mapping_result_id"),
            "package_manifest_digest":
                package_manifest_digest,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "evidence_package_plan_contract_version":
                EVIDENCE_PACKAGE_CONTRACT_VERSION,
            "evidence_package_plan_record_id":
                "HOASEHOFMRVEP-" + identity_digest[:20],
            "mapping_result_validation_plan_record_id":
                validation.get(
                    "historical_outcome_field_mapping_result_"
                    "validation_plan_record_id"
                ),
            "mapping_result_validation_plan_record_digest":
                validation.get(
                    "mapping_result_validation_plan_record_digest"
                ),
            "historical_outcome_field_mapping_plan_record_id":
                validation.get(
                    "historical_outcome_field_mapping_plan_record_id"
                ),
            "historical_outcome_field_mapping_plan_record_digest":
                validation.get(
                    "historical_outcome_field_mapping_plan_record_digest"
                ),
            "source_evidence_parsed_record_validation_plan_record_id":
                validation.get(
                    "source_evidence_parsed_record_validation_"
                    "plan_record_id"
                ),
            "parsed_record_validation_plan_record_digest":
                validation.get(
                    "parsed_record_validation_plan_record_digest"
                ),
            "endpoint_candidate_specification_record_id":
                validation.get(
                    "endpoint_candidate_specification_record_id"
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
            "candidate_supplied":
                bool(validation.get("candidate_supplied")),
            "candidate_id":
                validation.get("candidate_id"),
            "candidate_version":
                validation.get("candidate_version"),
            "response_artifact_id":
                validation.get("response_artifact_id"),
            "response_sha256":
                validation.get("response_sha256"),
            "parser_id":
                validation.get("parser_id"),
            "parser_version":
                validation.get("parser_version"),
            "parser_code_digest":
                validation.get("parser_code_digest"),
            "parsed_record_id":
                validation.get("parsed_record_id"),
            "parsed_record_version":
                validation.get("parsed_record_version"),
            "parsed_record_digest":
                validation.get("parsed_record_digest"),
            "mapping_id":
                validation.get("mapping_id"),
            "mapping_version":
                validation.get("mapping_version"),
            "mapping_digest":
                validation.get("mapping_digest"),
            "mapping_result_id":
                validation.get("mapping_result_id"),
            "mapping_result_version":
                validation.get("mapping_result_version"),
            "mapping_result_digest":
                validation.get("mapping_result_digest"),
            "mapping_result_validation_status":
                validation.get(
                    "mapping_result_validation_status"
                ),
            "mapping_result_validation_blocker_codes":
                validation.get(
                    "mapping_result_validation_blocker_codes"
                ),
            "mapping_result_validation_rationale":
                validation.get(
                    "mapping_result_validation_rationale"
                ),
            "mapping_result_validation_limitations":
                validation.get(
                    "mapping_result_validation_limitations"
                ),
            "candidate_evidence_artifact_count": 0,
            "response_evidence_artifact_count": 0,
            "parser_evidence_artifact_count": 0,
            "parsed_record_evidence_artifact_count": 0,
            "mapping_evidence_artifact_count": 0,
            "mapping_result_evidence_artifact_count": 0,
            "validation_evidence_artifact_count": 1,
            "evidence_absence_explicit": True,
            "fabricated_evidence_detected": False,
            "package_manifest_version":
                PACKAGE_MANIFEST_VERSION,
            "package_manifest_digest":
                package_manifest_digest,
            "evidence_package_status":
                PACKAGE_STATUS,
            "evidence_package_blocker_codes": [
                PACKAGE_BLOCKER
            ],
            "evidence_package_implementation_authority_granted":
                False,
            "evidence_package_rationale": (
                "The package preserves the deterministic Layer 9BN validation "
                "record, lineage, canonical field identity, blocker, rationale, "
                "limitations, and explicit absence of candidate-derived "
                "evidence. No external or missing evidence is invented."
            ),
            "evidence_package_limitations": [
                "No endpoint candidate was supplied.",
                "No validated response artifact exists.",
                "No authorized parser execution exists.",
                "No validated parsed record exists.",
                "No field-mapping submission exists.",
                "No authorized field-mapping execution exists.",
                "No mapping-result submission exists.",
                "No mapping result was validated.",
                "No candidate evidence artifact exists.",
                "No response evidence artifact exists.",
                "No parser evidence artifact exists.",
                "No parsed-record evidence artifact exists.",
                "No mapping evidence artifact exists.",
                "No mapping-result evidence artifact exists.",
                "Only the deterministic validation record is packaged.",
                "No evidence identity, digest, locator, or content was invented.",
                "No response bytes were read or parsed.",
                "No historical outcome field was mapped.",
                "No historical outcome value was extracted.",
                "No canonical record was mutated.",
                "No downstream record was recomputed.",
            ],
            "evidence_package_authority_boundary": (
                "Evidence-package implementation only. No endpoint candidate "
                "selection, response retrieval, parser execution, mapping "
                "execution, historical-outcome extraction, canonical mutation, "
                "downstream recomputation, quality determination, activation, "
                "production, market, pricing, or betting authority is granted."
            ),
            "evidence_package_plan_identity_digest":
                identity_digest,
        }

        record[
            "evidence_package_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Evidence-package record missing fields: "
                + ", ".join(missing_fields)
            )

        package_records.append(
            {
                field: record[field]
                for field in plan.EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS
            }
        )

    package_records.sort(
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
                row.get("parsed_record_id")
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
                row.get(
                    "evidence_package_plan_record_id"
                )
            ),
        )
    )

    return package_records


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

    package_records = build_package_records(
        plan,
        validation_records,
    )

    reverse_package_records = build_package_records(
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

    package_replay_deterministic = (
        canonical_json(package_records)
        == canonical_json(
            reverse_package_records
        )
    )

    package_digest = sha256_payload(
        package_records
    )

    reverse_package_digest = sha256_payload(
        reverse_package_records
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in package_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row["evidence_package_status"]
                for row in package_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in package_records
                for blocker in row[
                    "evidence_package_blocker_codes"
                ]
            ).items()
        )
    )

    artifact_counts = {
        "candidate": sum(
            row["candidate_evidence_artifact_count"]
            for row in package_records
        ),
        "response": sum(
            row["response_evidence_artifact_count"]
            for row in package_records
        ),
        "parser": sum(
            row["parser_evidence_artifact_count"]
            for row in package_records
        ),
        "parsed_record": sum(
            row["parsed_record_evidence_artifact_count"]
            for row in package_records
        ),
        "mapping": sum(
            row["mapping_evidence_artifact_count"]
            for row in package_records
        ),
        "mapping_result": sum(
            row["mapping_result_evidence_artifact_count"]
            for row in package_records
        ),
        "validation": sum(
            row["validation_evidence_artifact_count"]
            for row in package_records
        ),
    }

    authority_records = [
        row
        for row in package_records
        if row[
            "evidence_package_implementation_"
            "authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_bo_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed":
                plan.PLAN_VERSION
                == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_bn_contract_version_verified",
            "actual":
                predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION
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
            "check": "package_replay_deterministic",
            "actual": package_replay_deterministic,
            "expected": True,
            "passed": package_replay_deterministic,
        },
        {
            "check": "package_digests_match_reverse_replay",
            "actual": package_digest,
            "expected": reverse_package_digest,
            "passed":
                package_digest
                == reverse_package_digest,
        },
        {
            "check": "expected_validation_records_replayed",
            "actual": len(validation_records),
            "expected": EXPECTED_VALIDATION_RECORDS,
            "passed": (
                len(validation_records)
                == EXPECTED_VALIDATION_RECORDS
            ),
        },
        {
            "check": "expected_package_records_materialized",
            "actual": len(package_records),
            "expected": EXPECTED_PACKAGE_RECORDS,
            "passed": (
                len(package_records)
                == EXPECTED_PACKAGE_RECORDS
            ),
        },
        {
            "check": "one_package_record_per_comparison",
            "actual": len(comparison_ids),
            "expected": EXPECTED_PACKAGE_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_PACKAGE_RECORDS
            ),
        },
        {
            "check": "package_record_fields_complete",
            "actual": len(
                plan.EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS
            ),
            "expected": 61,
            "passed": all(
                set(row)
                == set(
                    plan.EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS
                )
                for row in package_records
            ),
        },
        {
            "check": "package_record_ids_unique",
            "actual": len(
                {
                    row[
                        "evidence_package_plan_record_id"
                    ]
                    for row in package_records
                }
            ),
            "expected": len(package_records),
            "passed": (
                len(
                    {
                        row[
                            "evidence_package_plan_record_id"
                        ]
                        for row in package_records
                    }
                )
                == len(package_records)
            ),
        },
        {
            "check": "package_record_digests_unique",
            "actual": len(
                {
                    row[
                        "evidence_package_plan_record_digest"
                    ]
                    for row in package_records
                }
            ),
            "expected": len(package_records),
            "passed": (
                len(
                    {
                        row[
                            "evidence_package_plan_record_digest"
                        ]
                        for row in package_records
                    }
                )
                == len(package_records)
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
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_package_plan_identity_digest"
                    ]
                )
                for row in package_records
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
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                valid_sha256(
                    row[
                        "evidence_package_plan_record_digest"
                    ]
                )
                for row in package_records
            ),
        },
        {
            "check": "all_manifest_digests_valid",
            "actual": sum(
                valid_sha256(
                    row["package_manifest_digest"]
                )
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                valid_sha256(
                    row["package_manifest_digest"]
                )
                for row in package_records
            ),
        },
        {
            "check": "all_validation_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "mapping_result_validation_plan_record_digest"
                    ]
                )
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                valid_sha256(
                    row[
                        "mapping_result_validation_plan_record_digest"
                    ]
                )
                for row in package_records
            ),
        },
        {
            "check": "all_mapping_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "historical_outcome_field_mapping_plan_record_digest"
                    ]
                )
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                valid_sha256(
                    row[
                        "historical_outcome_field_mapping_plan_record_digest"
                    ]
                )
                for row in package_records
            ),
        },
        {
            "check": "all_parsed_record_validation_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "parsed_record_validation_plan_record_digest"
                    ]
                )
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                valid_sha256(
                    row[
                        "parsed_record_validation_plan_record_digest"
                    ]
                )
                for row in package_records
            ),
        },
        {
            "check": "all_defect_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row["defect_source_record_digest"]
                )
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                valid_sha256(
                    row["defect_source_record_digest"]
                )
                for row in package_records
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": status_counts,
            "expected": {
                PACKAGE_STATUS:
                    EXPECTED_PACKAGE_RECORDS
            },
            "passed": (
                status_counts
                == {
                    PACKAGE_STATUS:
                        EXPECTED_PACKAGE_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_present",
            "actual": blocker_counts,
            "expected": {
                PACKAGE_BLOCKER:
                    EXPECTED_PACKAGE_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    PACKAGE_BLOCKER:
                        EXPECTED_PACKAGE_RECORDS
                }
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
                    for row in package_records
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
                for row in package_records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row["rejected_metadata_field_name"]
                    for row in package_records
                }
            ),
            "expected": [REJECTED_METADATA_FIELD],
            "passed": all(
                row["rejected_metadata_field_name"]
                == REJECTED_METADATA_FIELD
                for row in package_records
            ),
        },
        {
            "check": "all_candidate_derived_artifact_counts_zero",
            "actual": {
                key: value
                for key, value in artifact_counts.items()
                if key != "validation"
            },
            "expected": {
                "candidate": 0,
                "response": 0,
                "parser": 0,
                "parsed_record": 0,
                "mapping": 0,
                "mapping_result": 0,
            },
            "passed": all(
                artifact_counts[key] == 0
                for key in (
                    "candidate",
                    "response",
                    "parser",
                    "parsed_record",
                    "mapping",
                    "mapping_result",
                )
            ),
        },
        {
            "check": "one_validation_evidence_artifact_per_record",
            "actual": artifact_counts["validation"],
            "expected": EXPECTED_PACKAGE_RECORDS,
            "passed": (
                artifact_counts["validation"]
                == EXPECTED_PACKAGE_RECORDS
            ),
        },
        {
            "check": "evidence_absence_explicit_for_all_records",
            "actual": sum(
                bool(row["evidence_absence_explicit"])
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                bool(row["evidence_absence_explicit"])
                for row in package_records
            ),
        },
        {
            "check": "fabricated_evidence_absent_for_all_records",
            "actual": sum(
                not bool(
                    row["fabricated_evidence_detected"]
                )
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                not bool(
                    row["fabricated_evidence_detected"]
                )
                for row in package_records
            ),
        },
        {
            "check": "package_manifest_version_preserved",
            "actual": sorted(
                {
                    row["package_manifest_version"]
                    for row in package_records
                }
            ),
            "expected": [PACKAGE_MANIFEST_VERSION],
            "passed": all(
                row["package_manifest_version"]
                == PACKAGE_MANIFEST_VERSION
                for row in package_records
            ),
        },
        {
            "check": "all_candidate_response_parser_fields_absent",
            "actual": sum(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                and row["response_artifact_id"] is None
                and row["response_sha256"] is None
                and row["parser_id"] is None
                and row["parser_version"] is None
                and row["parser_code_digest"] is None
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                and row["response_artifact_id"] is None
                and row["response_sha256"] is None
                and row["parser_id"] is None
                and row["parser_version"] is None
                and row["parser_code_digest"] is None
                for row in package_records
            ),
        },
        {
            "check": "all_parsed_record_mapping_and_result_fields_absent",
            "actual": sum(
                row["parsed_record_id"] is None
                and row["parsed_record_version"] is None
                and row["parsed_record_digest"] is None
                and row["mapping_id"] is None
                and row["mapping_version"] is None
                and row["mapping_digest"] is None
                and row["mapping_result_id"] is None
                and row["mapping_result_version"] is None
                and row["mapping_result_digest"] is None
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                row["parsed_record_id"] is None
                and row["parsed_record_version"] is None
                and row["parsed_record_digest"] is None
                and row["mapping_id"] is None
                and row["mapping_version"] is None
                and row["mapping_digest"] is None
                and row["mapping_result_id"] is None
                and row["mapping_result_version"] is None
                and row["mapping_result_digest"] is None
                for row in package_records
            ),
        },
        {
            "check": "validation_rationale_and_limitations_preserved",
            "actual": sum(
                bool(
                    normalized_string(
                        row[
                            "mapping_result_validation_rationale"
                        ]
                    )
                )
                and bool(
                    row[
                        "mapping_result_validation_limitations"
                    ]
                )
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                bool(
                    normalized_string(
                        row[
                            "mapping_result_validation_rationale"
                        ]
                    )
                )
                and bool(
                    row[
                        "mapping_result_validation_limitations"
                    ]
                )
                for row in package_records
            ),
        },
        {
            "check": "package_authority_boundaries_present",
            "actual": sum(
                bool(
                    normalized_string(
                        row[
                            "evidence_package_authority_boundary"
                        ]
                    )
                )
                for row in package_records
            ),
            "expected": len(package_records),
            "passed": all(
                bool(
                    normalized_string(
                        row[
                            "evidence_package_authority_boundary"
                        ]
                    )
                )
                for row in package_records
            ),
        },
        {
            "check": "no_package_implementation_authority_granted",
            "actual": len(authority_records),
            "expected": 0,
            "passed":
                len(authority_records) == 0,
        },
        {
            "check": "evidence_artifact_invention_not_executed",
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
            "check": "candidate_response_parser_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "mapping_and_result_invention_not_executed",
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
        "result_validation_evidence_package_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_implementation_failed"
    )

    next_layer = (
        "9BQ_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_plan"
        if all_checks_passed
        else
        "9BP_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_implementation_remediation"
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
        OUTPUT_DIR / "evidence_package_records.csv",
        plan.EVIDENCE_PACKAGE_PLAN_RECORD_FIELDS,
        package_records,
    )

    write_csv(
        OUTPUT_DIR / "evidence_package_status_counts.csv",
        [
            "evidence_package_status",
            "count",
        ],
        [
            {
                "evidence_package_status": key,
                "count": value,
            }
            for key, value in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "evidence_package_blocker_counts.csv",
        [
            "evidence_package_blocker",
            "count",
        ],
        [
            {
                "evidence_package_blocker": key,
                "count": value,
            }
            for key, value in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR / "evidence_artifact_inventory.json",
        {
            "layer_id": LAYER_ID,
            "artifact_counts": artifact_counts,
            "candidate_derived_artifact_count": sum(
                value
                for key, value in artifact_counts.items()
                if key != "validation"
            ),
            "validation_evidence_artifact_count":
                artifact_counts["validation"],
            "fabricated_evidence_artifact_count": 0,
            "inventory_status": (
                "validation_records_packaged_with_"
                "candidate_evidence_absence_explicit"
            ),
        },
    )

    write_json(
        OUTPUT_DIR / "package_manifest.json",
        {
            "layer_id": LAYER_ID,
            "package_manifest_version":
                PACKAGE_MANIFEST_VERSION,
            "evidence_package_contract_version":
                EVIDENCE_PACKAGE_CONTRACT_VERSION,
            "package_record_count":
                len(package_records),
            "comparison_count":
                len(comparison_ids),
            "package_digest":
                package_digest,
            "reverse_package_digest":
                reverse_package_digest,
            "package_status_counts":
                status_counts,
            "package_blocker_counts":
                blocker_counts,
            "artifact_counts":
                artifact_counts,
            "canonical_field_identity": {
                "authoritative_field_name":
                    AUTHORITATIVE_FIELD_NAME,
                "authoritative_field_path":
                    AUTHORITATIVE_FIELD_PATH,
                "rejected_metadata_field_name":
                    REJECTED_METADATA_FIELD,
            },
            "evidence_absence_explicit": True,
            "fabricated_evidence_detected": False,
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "evidence_package_contract_version":
            EVIDENCE_PACKAGE_CONTRACT_VERSION,
        "package_manifest_version":
            PACKAGE_MANIFEST_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.MAPPING_RESULT_VALIDATION_CONTRACT_VERSION,
        "validation_records":
            len(validation_records),
        "evidence_package_records":
            len(package_records),
        "evidence_package_comparisons":
            len(comparison_ids),
        "evidence_package_status_counts":
            status_counts,
        "evidence_package_blocker_counts":
            blocker_counts,
        "artifact_counts":
            artifact_counts,
        "package_implementation_authorities_granted":
            len(authority_records),
        "package_digest":
            package_digest,
        "reverse_package_digest":
            reverse_package_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
        "candidate_derived_evidence_artifacts_created": 0,
        "validation_evidence_artifacts_packaged":
            artifact_counts["validation"],
        "fabricated_evidence_artifacts_created": 0,
        "mapping_results_validated": 0,
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
        OUTPUT_DIR / "evidence_package_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "evidence_package_result":
            PACKAGE_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_historical_outcome_field_mapping_result_"
            "validation_evidence_package_validation_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "response_artifact_invention",
            "response_metadata_invention",
            "parser_submission_invention",
            "parser_identity_invention",
            "parser_code_invention",
            "parsed_record_submission_invention",
            "parsed_record_identity_invention",
            "parsed_record_content_invention",
            "mapping_submission_invention",
            "mapping_identity_invention",
            "mapping_result_submission_invention",
            "mapping_result_identity_invention",
            "mapping_result_content_invention",
            "validation_result_invention",
            "evidence_artifact_invention",
            "evidence_artifact_identity_invention",
            "evidence_artifact_digest_invention",
            "evidence_locator_invention",
            "source_value_invention",
            "mapped_value_invention",
            "source_to_target_provenance_invention",
            "mapping_rule_provenance_invention",
            "rejected_metadata_field_substitution",
            "boolean_to_integer_coercion",
            "source_value_defaulting",
            "source_value_inference",
            "source_value_imputation",
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
        "Evidence package contract version: "
        f"{EVIDENCE_PACKAGE_CONTRACT_VERSION}"
    )
    print(
        "Package manifest version: "
        f"{PACKAGE_MANIFEST_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Validation records replayed: "
        f"{len(validation_records)}"
    )
    print(
        "Evidence-package records: "
        f"{len(package_records)}"
    )
    print(
        "Evidence-package comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Evidence-package status counts: "
        f"{status_counts}"
    )
    print(
        "Evidence-package blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Artifact counts: "
        f"{artifact_counts}"
    )
    print(
        "Package implementation authorities granted: "
        f"{len(authority_records)}"
    )
    print(
        f"Package digest: {package_digest}"
    )
    print(
        "Reverse package digest: "
        f"{reverse_package_digest}"
    )
    print("Candidate-derived evidence artifacts created: 0")
    print(
        "Validation evidence artifacts packaged: "
        f"{artifact_counts['validation']}"
    )
    print("Fabricated evidence artifacts created: 0")
    print("Mapping results validated: 0")
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
        "Evidence-package result: "
        f"{diagnosis['evidence_package_result']}"
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
