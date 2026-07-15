#!/usr/bin/env python3
"""
Layer 9BJ
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Parsed Record Validation Implementation

Implements the deterministic parsed-record validation contract planned by
Layer 9BI.

Layer 9BH established that no endpoint candidate, validated response, parser
submission, parsing execution, or parsed source-evidence record exists.
Layer 9BI therefore authorized validation implementation only.

This implementation:
- verifies the Layer 9BI plan;
- replays Layer 9BH response-parsing records deterministically;
- inventories explicitly supplied parsed-record submissions;
- emits one deterministic validation record per comparison;
- classifies all records as candidate_not_supplied;
- validates no parsed source-evidence record;
- performs no historical-outcome field mapping or value extraction.

No candidate, response, parser, parsed record, schema, provenance, value, or
authority evidence is invented, inferred, defaulted, imputed, or fabricated.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BJ"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_parsed_record_validation_implementation"
)

PARSED_RECORD_VALIDATION_CONTRACT_VERSION = (
    "layer_9BJ_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_parsed_record_validation_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BJ_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_parsed_record_"
    "validation"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9BI_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_parsed_record_validation.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9BI_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_parsed_record_validation_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BH_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_response_parsing_contract_v1"
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

SUPPLIED_PARSED_RECORD_SUBMISSIONS: tuple[
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
        "layer_9bi_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9BI plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BH contract version: "
            f"{predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_validation_records(
    plan: Any,
    parsing_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    validation_records: list[dict[str, Any]] = []

    for parsing in parsing_records:
        identity_payload = {
            "validation_contract_version":
                PARSED_RECORD_VALIDATION_CONTRACT_VERSION,
            "response_parsing_record_id":
                parsing.get(
                    "source_evidence_response_parsing_plan_record_id"
                ),
            "comparison_record_id":
                parsing.get("comparison_record_id"),
            "defect_source_record_id":
                parsing.get("defect_source_record_id"),
            "candidate_id":
                parsing.get("candidate_id"),
            "response_artifact_id":
                parsing.get("response_artifact_id"),
            "parser_id":
                parsing.get("parser_id"),
            "parsed_record_id": None,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "source_evidence_parsed_record_validation_plan_contract_version":
                PARSED_RECORD_VALIDATION_CONTRACT_VERSION,
            "source_evidence_parsed_record_validation_plan_record_id":
                "HOASEPRV-" + identity_digest[:20],
            "source_evidence_response_parsing_plan_record_id":
                parsing.get(
                    "source_evidence_response_parsing_plan_record_id"
                ),
            "source_evidence_response_parsing_plan_record_digest":
                parsing.get(
                    "source_evidence_response_parsing_plan_record_digest"
                ),
            "source_evidence_acquisition_result_validation_plan_record_id":
                parsing.get(
                    "source_evidence_acquisition_result_validation_"
                    "plan_record_id"
                ),
            "endpoint_candidate_specification_record_id":
                parsing.get(
                    "endpoint_candidate_specification_record_id"
                ),
            "comparison_record_id":
                parsing.get("comparison_record_id"),
            "metric_record_id":
                parsing.get("metric_record_id"),
            "metric_name":
                parsing.get("metric_name"),
            "aggregation_name":
                parsing.get("aggregation_name"),
            "aggregation_key":
                parsing.get("aggregation_key"),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                parsing.get("defect_source_path"),
            "defect_source_symbol":
                parsing.get("defect_source_symbol"),
            "defect_source_record_id":
                parsing.get("defect_source_record_id"),
            "defect_source_record_digest":
                parsing.get("defect_source_record_digest"),
            "response_parsing_status":
                parsing.get(
                    "source_evidence_response_parsing_status"
                ),
            "response_parsing_blocker_codes":
                parsing.get(
                    "source_evidence_response_parsing_blocker_codes"
                ),
            "candidate_supplied":
                bool(parsing.get("candidate_supplied")),
            "candidate_id":
                parsing.get("candidate_id"),
            "candidate_version":
                parsing.get("candidate_version"),
            "response_artifact_id":
                parsing.get("response_artifact_id"),
            "response_sha256":
                parsing.get("response_sha256"),
            "parser_submission_supplied":
                bool(
                    parsing.get(
                        "parser_submission_supplied"
                    )
                ),
            "parser_id":
                parsing.get("parser_id"),
            "parser_version":
                parsing.get("parser_version"),
            "parser_code_digest":
                parsing.get("parser_code_digest"),
            "parsed_record_submission_supplied":
                False,
            "parsed_record_id":
                None,
            "parsed_record_version":
                None,
            "parsed_record_digest":
                None,
            "schema_version":
                None,
            "record_selector":
                None,
            "raw_record_payload":
                None,
            "raw_field_provenance":
                None,
            "source_location_provenance":
                None,
            "required_fields_complete":
                None,
            "field_types_valid":
                None,
            "record_ordering_valid":
                None,
            "duplicate_policy_satisfied":
                None,
            "record_cardinality_valid":
                None,
            "ambiguity_detected":
                None,
            "malformed_record_detected":
                None,
            "parsed_record_validation_status":
                VALIDATION_STATUS,
            "parsed_record_validation_blocker_codes": [
                VALIDATION_BLOCKER
            ],
            "parsed_record_validation_implementation_authority_granted":
                False,
            "parsed_record_validation_rationale": (
                "No endpoint candidate exists, so no validated response, "
                "authorized parser execution, or parsed source-evidence record "
                "exists. Parsed-record validation cannot proceed without "
                "inventing parsed-record identity, content, digest, schema, "
                "raw payload, provenance, or validation evidence."
            ),
            "parsed_record_validation_limitations": [
                "No endpoint candidate was supplied.",
                "No validated response artifact exists.",
                "No authorized parser execution exists.",
                "No parsed-record submission was supplied.",
                "No parsed-record identifier was invented.",
                "No parsed-record version was invented.",
                "No parsed-record digest was invented.",
                "No schema version was invented.",
                "No record selector was invented.",
                "No raw record payload was invented.",
                "No raw-field provenance was invented.",
                "No source-location provenance was invented.",
                "No required-field completeness determination was inferred.",
                "No field-type validity determination was inferred.",
                "No ordering determination was inferred.",
                "No duplicate-policy determination was inferred.",
                "No cardinality determination was inferred.",
                "No ambiguity determination was inferred.",
                "No malformed-record determination was inferred.",
                "No parsed record was validated.",
                "No historical outcome field was mapped.",
                "No historical outcome value was extracted.",
                "No canonical record or mapping was mutated.",
                "No downstream record was recomputed.",
            ],
            "parsed_record_validation_plan_identity_digest":
                identity_digest,
        }

        record[
            "parsed_record_validation_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.VALIDATION_PLAN_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Parsed-record validation record missing fields: "
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
                row.get("response_artifact_id")
            ),
            normalized_string(
                row.get("parser_id")
            ),
            normalized_string(
                row.get("parsed_record_id")
            ),
            normalized_string(
                row.get(
                    "source_evidence_parsed_record_validation_"
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
    parsing_records = replay["records"]
    reverse_parsing_records = replay[
        "reverse_records"
    ]

    validation_records = build_validation_records(
        plan,
        parsing_records,
    )

    reverse_validation_records = build_validation_records(
        plan,
        list(
            reversed(
                reverse_parsing_records
            )
        ),
    )

    predecessor_replay_deterministic = (
        canonical_json(parsing_records)
        == canonical_json(
            reverse_parsing_records
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
                    "parsed_record_validation_status"
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
                    "parsed_record_validation_blocker_codes"
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

    parser_presence_counts = dict(
        sorted(
            Counter(
                str(
                    row[
                        "parser_submission_supplied"
                    ]
                )
                for row in validation_records
            ).items()
        )
    )

    parsed_record_presence_counts = dict(
        sorted(
            Counter(
                str(
                    row[
                        "parsed_record_submission_supplied"
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
            "parsed_record_validation_"
            "implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_bi_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed":
                plan.PLAN_VERSION
                == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_bh_contract_version_verified",
            "actual":
                predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION
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
            "actual": len(parsing_records),
            "expected": EXPECTED_PREDECESSOR_RECORDS,
            "passed": (
                len(parsing_records)
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
            "expected": 52,
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
                        "source_evidence_parsed_record_validation_"
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
                            "source_evidence_parsed_record_validation_"
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
                        "parsed_record_validation_plan_record_digest"
                    ]
                    for row in validation_records
                }
            ),
            "expected": len(validation_records),
            "passed": (
                len(
                    {
                        row[
                            "parsed_record_validation_plan_record_digest"
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
                        "parsed_record_validation_plan_identity_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "parsed_record_validation_plan_identity_digest"
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
                        "parsed_record_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "parsed_record_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "all_response_parsing_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_response_parsing_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_response_parsing_plan_record_digest"
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
            "check": "supplied_parsed_record_inventory_empty",
            "actual": len(
                SUPPLIED_PARSED_RECORD_SUBMISSIONS
            ),
            "expected": 0,
            "passed": (
                len(
                    SUPPLIED_PARSED_RECORD_SUBMISSIONS
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
            "check": "all_parser_submissions_absent",
            "actual": parser_presence_counts,
            "expected": {
                "False": EXPECTED_VALIDATION_RECORDS
            },
            "passed": (
                parser_presence_counts
                == {
                    "False":
                        EXPECTED_VALIDATION_RECORDS
                }
            ),
        },
        {
            "check": "all_parsed_record_submissions_absent",
            "actual": parsed_record_presence_counts,
            "expected": {
                "False": EXPECTED_VALIDATION_RECORDS
            },
            "passed": (
                parsed_record_presence_counts
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
            "check": "all_response_identity_fields_absent",
            "actual": sum(
                row["response_artifact_id"] is None
                and row["response_sha256"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["response_artifact_id"] is None
                and row["response_sha256"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_parser_identity_fields_absent",
            "actual": sum(
                row["parser_id"] is None
                and row["parser_version"] is None
                and row["parser_code_digest"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["parser_id"] is None
                and row["parser_version"] is None
                and row["parser_code_digest"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_parsed_record_identity_fields_absent",
            "actual": sum(
                row["parsed_record_id"] is None
                and row["parsed_record_version"] is None
                and row["parsed_record_digest"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["parsed_record_id"] is None
                and row["parsed_record_version"] is None
                and row["parsed_record_digest"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_parsed_record_schema_fields_absent",
            "actual": sum(
                row["schema_version"] is None
                and row["record_selector"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["schema_version"] is None
                and row["record_selector"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_parsed_record_payload_and_provenance_absent",
            "actual": sum(
                row["raw_record_payload"] is None
                and row["raw_field_provenance"] is None
                and row["source_location_provenance"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["raw_record_payload"] is None
                and row["raw_field_provenance"] is None
                and row["source_location_provenance"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_structural_validation_results_absent",
            "actual": sum(
                row["required_fields_complete"] is None
                and row["field_types_valid"] is None
                and row["record_ordering_valid"] is None
                and row["duplicate_policy_satisfied"] is None
                and row["record_cardinality_valid"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["required_fields_complete"] is None
                and row["field_types_valid"] is None
                and row["record_ordering_valid"] is None
                and row["duplicate_policy_satisfied"] is None
                and row["record_cardinality_valid"] is None
                for row in validation_records
            ),
        },
        {
            "check": "all_ambiguity_and_malformed_results_absent",
            "actual": sum(
                row["ambiguity_detected"] is None
                and row["malformed_record_detected"] is None
                for row in validation_records
            ),
            "expected": len(validation_records),
            "passed": all(
                row["ambiguity_detected"] is None
                and row["malformed_record_detected"] is None
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
            "check": "response_or_parser_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "parsed_record_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "parsed_record_provenance_invention_not_executed",
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
        "endpoint_candidate_source_evidence_parsed_record_validation_"
        "implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_parsed_record_validation_"
        "implementation_failed"
    )

    next_layer = (
        "9BK_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_plan"
        if all_checks_passed
        else
        "9BJ_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_parsed_record_validation_"
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
        / "source_evidence_parsed_record_validation_records.csv",
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
        / "supplied_parsed_record_submission_inventory.json",
        {
            "layer_id": LAYER_ID,
            "supplied_parsed_record_submission_count":
                len(
                    SUPPLIED_PARSED_RECORD_SUBMISSIONS
                ),
            "supplied_parsed_record_submissions":
                list(
                    SUPPLIED_PARSED_RECORD_SUBMISSIONS
                ),
            "inventory_status": (
                "no_candidate_response_parser_or_parsed_"
                "record_submission_supplied"
            ),
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "parsed_record_validation_contract_version":
            PARSED_RECORD_VALIDATION_CONTRACT_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION,
        "predecessor_records":
            len(parsing_records),
        "validation_records":
            len(validation_records),
        "validation_comparisons":
            len(comparison_ids),
        "supplied_parsed_record_submissions":
            len(
                SUPPLIED_PARSED_RECORD_SUBMISSIONS
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
        "response_bytes_read": 0,
        "responses_parsed": 0,
        "parsed_records_validated": 0,
        "historical_outcome_fields_mapped": 0,
        "historical_outcome_values_extracted": 0,
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
        / "source_evidence_parsed_record_validation_summary.json",
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
            "source_evidence_historical_outcome_field_mapping_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "endpoint_candidate_selection_without_submission",
            "response_artifact_invention",
            "response_metadata_invention",
            "parser_submission_invention",
            "parser_identity_invention",
            "parser_code_invention",
            "parsed_record_submission_invention",
            "parsed_record_identity_invention",
            "parsed_record_content_invention",
            "parsed_record_digest_invention",
            "schema_invention",
            "record_selector_invention",
            "raw_record_payload_invention",
            "raw_field_provenance_invention",
            "source_location_provenance_invention",
            "response_bytes_reading",
            "source_evidence_parse_execution",
            "raw_response_parse_execution",
            "historical_outcome_field_mapping_execution",
            "historical_outcome_value_extraction",
            "credential_literal_storage",
            "credential_literal_logging",
            "dns_resolution_execution",
            "socket_connection_execution",
            "http_request_execution",
            "browser_execution",
            "api_request_execution",
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
        "Parsed record validation contract version: "
        f"{PARSED_RECORD_VALIDATION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Predecessor records replayed: "
        f"{len(parsing_records)}"
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
        "Supplied parsed record submissions: "
        f"{len(SUPPLIED_PARSED_RECORD_SUBMISSIONS)}"
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
    print("Response bytes read: 0")
    print("Responses parsed: 0")
    print("Parsed records validated: 0")
    print("Historical outcome fields mapped: 0")
    print("Historical outcome values extracted: 0")
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
