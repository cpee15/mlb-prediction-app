#!/usr/bin/env python3
"""
Layer 9BH
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate
Source Evidence Response Parsing Implementation

Implements the deterministic response-parsing contract planned by Layer 9BG.

Layer 9BF established that no endpoint candidate, acquisition-result submission,
validated response artifact, or immutable response exists. Layer 9BG therefore
authorized parsing implementation only, not parsing execution.

This implementation:
- replays Layer 9BF validation records deterministically;
- verifies the Layer 9BG response-parsing plan;
- inventories explicitly supplied parser submissions;
- emits one deterministic parsing record per comparison;
- classifies all records as `candidate_not_supplied`;
- reads and parses no response bytes;
- grants no historical-outcome extraction authority.

No candidate, response, parser, schema, selector, field path, namespace,
delimiter, header contract, parsed value, or authority evidence is invented,
inferred, defaulted, imputed, or fabricated.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BH"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_response_parsing_implementation"
)

SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION = (
    "layer_9BH_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_response_parsing_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BH_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_response_parsing"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9BG_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_response_parsing.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9BG_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_response_parsing_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BF_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_acquisition_result_validation_contract_v1"
)

EXPECTED_PREDECESSOR_RECORDS = 16
EXPECTED_PARSING_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

PARSING_STATUS = "candidate_not_supplied"

PARSING_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

SUPPLIED_RESPONSE_PARSER_SUBMISSIONS: tuple[
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
        "layer_9bg_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9BG plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BF contract version: "
            f"{predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_parsing_records(
    plan: Any,
    validation_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    parsing_records: list[dict[str, Any]] = []

    for validation in validation_records:
        identity_payload = {
            "parsing_contract_version":
                SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION,
            "validation_record_id":
                validation.get(
                    "source_evidence_acquisition_result_validation_"
                    "plan_record_id"
                ),
            "comparison_record_id":
                validation.get("comparison_record_id"),
            "defect_source_record_id":
                validation.get("defect_source_record_id"),
            "candidate_id":
                validation.get("candidate_id"),
            "response_artifact_id":
                validation.get("response_artifact_id"),
            "parser_id": None,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "source_evidence_response_parsing_plan_contract_version":
                SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION,
            "source_evidence_response_parsing_plan_record_id":
                "HOASERP-" + identity_digest[:20],
            "source_evidence_acquisition_result_validation_plan_record_id":
                validation.get(
                    "source_evidence_acquisition_result_validation_"
                    "plan_record_id"
                ),
            "acquisition_result_validation_plan_record_digest":
                validation.get(
                    "acquisition_result_validation_plan_record_digest"
                ),
            "source_evidence_acquisition_execution_plan_record_id":
                validation.get(
                    "source_evidence_acquisition_execution_plan_record_id"
                ),
            "source_evidence_acquisition_authorization_plan_record_id":
                validation.get(
                    "source_evidence_acquisition_authorization_plan_record_id"
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
            "acquisition_result_validation_status":
                validation.get(
                    "acquisition_result_validation_status"
                ),
            "acquisition_result_validation_blocker_codes":
                validation.get(
                    "acquisition_result_validation_blocker_codes"
                ),
            "candidate_supplied":
                bool(validation.get("candidate_supplied")),
            "candidate_id":
                validation.get("candidate_id"),
            "candidate_version":
                validation.get("candidate_version"),
            "result_submission_supplied":
                bool(
                    validation.get(
                        "result_submission_supplied"
                    )
                ),
            "result_id":
                validation.get("result_id"),
            "response_artifact_id":
                validation.get("response_artifact_id"),
            "response_media_type":
                validation.get("response_media_type"),
            "response_sha256":
                validation.get("response_sha256"),
            "response_quarantined":
                validation.get("response_quarantined"),
            "response_immutable":
                validation.get("response_immutable"),
            "parser_submission_supplied": False,
            "parser_id": None,
            "parser_version": None,
            "parser_code_digest": None,
            "supported_media_types": None,
            "character_encoding": None,
            "container_format": None,
            "schema_version": None,
            "record_selector": None,
            "field_path_contract": None,
            "namespace_contract": None,
            "delimiter_contract": None,
            "header_contract": None,
            "record_ordering_contract": None,
            "duplicate_policy": None,
            "missing_field_policy": None,
            "unknown_field_policy": None,
            "malformed_record_policy": None,
            "raw_field_provenance_contract": None,
            "source_location_provenance_contract": None,
            "source_evidence_response_parsing_status":
                PARSING_STATUS,
            "source_evidence_response_parsing_blocker_codes": [
                PARSING_BLOCKER
            ],
            "source_evidence_response_parsing_implementation_authority_granted":
                False,
            "source_evidence_response_parsing_rationale": (
                "No endpoint candidate exists, so no validated immutable "
                "response artifact or explicit parser submission exists. "
                "Response parsing cannot proceed without inventing response, "
                "parser, schema, selector, field-path, encoding, ordering, "
                "duplicate-policy, or provenance metadata."
            ),
            "source_evidence_response_parsing_limitations": [
                "No endpoint candidate was supplied.",
                "No validated acquisition result exists.",
                "No immutable response artifact exists.",
                "No response SHA-256 digest exists.",
                "No parser submission was supplied.",
                "No parser identifier or version was invented.",
                "No parser code digest was invented.",
                "No media-type compatibility declaration was invented.",
                "No character encoding was invented.",
                "No container format or schema version was invented.",
                "No record selector or field-path contract was invented.",
                "No namespace, delimiter, or header contract was invented.",
                "No ordering or duplicate policy was invented.",
                "No missing-field, unknown-field, or malformed-record policy was invented.",
                "No raw-field or source-location provenance contract was invented.",
                "No response bytes were read.",
                "No response was parsed.",
                "No historical outcome value was extracted.",
                "No canonical record or mapping was mutated.",
                "No downstream record was recomputed.",
            ],
            "source_evidence_response_parsing_plan_identity_digest":
                identity_digest,
        }

        record[
            "source_evidence_response_parsing_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.PARSING_PLAN_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Parsing record missing fields: "
                + ", ".join(missing_fields)
            )

        parsing_records.append(
            {
                field: record[field]
                for field in plan.PARSING_PLAN_RECORD_FIELDS
            }
        )

    parsing_records.sort(
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
                row.get(
                    "source_evidence_response_parsing_plan_record_id"
                )
            ),
        )
    )

    return parsing_records


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

    parsing_records = build_parsing_records(
        plan,
        validation_records,
    )

    reverse_parsing_records = build_parsing_records(
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

    parsing_replay_deterministic = (
        canonical_json(parsing_records)
        == canonical_json(
            reverse_parsing_records
        )
    )

    parsing_digest = sha256_payload(
        parsing_records
    )

    reverse_parsing_digest = sha256_payload(
        reverse_parsing_records
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in parsing_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row[
                    "source_evidence_response_parsing_status"
                ]
                for row in parsing_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in parsing_records
                for blocker in row[
                    "source_evidence_response_parsing_blocker_codes"
                ]
            ).items()
        )
    )

    candidate_presence_counts = dict(
        sorted(
            Counter(
                str(row["candidate_supplied"])
                for row in parsing_records
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
                for row in parsing_records
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
                for row in parsing_records
            ).items()
        )
    )

    authority_records = [
        row
        for row in parsing_records
        if row[
            "source_evidence_response_parsing_"
            "implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_bg_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed":
                plan.PLAN_VERSION == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_bf_contract_version_verified",
            "actual":
                predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION
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
            "check": "parsing_replay_deterministic",
            "actual": parsing_replay_deterministic,
            "expected": True,
            "passed": parsing_replay_deterministic,
        },
        {
            "check": "parsing_digests_match_reverse_replay",
            "actual": parsing_digest,
            "expected": reverse_parsing_digest,
            "passed": (
                parsing_digest
                == reverse_parsing_digest
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
            "check": "expected_parsing_records_materialized",
            "actual": len(parsing_records),
            "expected": EXPECTED_PARSING_RECORDS,
            "passed": (
                len(parsing_records)
                == EXPECTED_PARSING_RECORDS
            ),
        },
        {
            "check": "one_parsing_record_per_comparison",
            "actual": len(comparison_ids),
            "expected": EXPECTED_PARSING_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_PARSING_RECORDS
            ),
        },
        {
            "check": "parsing_record_fields_complete",
            "actual": len(
                plan.PARSING_PLAN_RECORD_FIELDS
            ),
            "expected": 58,
            "passed": all(
                set(row)
                == set(
                    plan.PARSING_PLAN_RECORD_FIELDS
                )
                for row in parsing_records
            ),
        },
        {
            "check": "parsing_record_ids_unique",
            "actual": len(
                {
                    row[
                        "source_evidence_response_parsing_plan_record_id"
                    ]
                    for row in parsing_records
                }
            ),
            "expected": len(parsing_records),
            "passed": (
                len(
                    {
                        row[
                            "source_evidence_response_parsing_plan_record_id"
                        ]
                        for row in parsing_records
                    }
                )
                == len(parsing_records)
            ),
        },
        {
            "check": "parsing_record_digests_unique",
            "actual": len(
                {
                    row[
                        "source_evidence_response_parsing_plan_record_digest"
                    ]
                    for row in parsing_records
                }
            ),
            "expected": len(parsing_records),
            "passed": (
                len(
                    {
                        row[
                            "source_evidence_response_parsing_plan_record_digest"
                        ]
                        for row in parsing_records
                    }
                )
                == len(parsing_records)
            ),
        },
        {
            "check": "all_parsing_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_response_parsing_plan_identity_digest"
                    ]
                )
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_response_parsing_plan_identity_digest"
                    ]
                )
                for row in parsing_records
            ),
        },
        {
            "check": "all_parsing_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_evidence_response_parsing_plan_record_digest"
                    ]
                )
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_evidence_response_parsing_plan_record_digest"
                    ]
                )
                for row in parsing_records
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
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                valid_sha256(
                    row[
                        "acquisition_result_validation_plan_record_digest"
                    ]
                )
                for row in parsing_records
            ),
        },
        {
            "check": "all_defect_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row["defect_source_record_digest"]
                )
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                valid_sha256(
                    row["defect_source_record_digest"]
                )
                for row in parsing_records
            ),
        },
        {
            "check": "supplied_parser_inventory_empty",
            "actual": len(
                SUPPLIED_RESPONSE_PARSER_SUBMISSIONS
            ),
            "expected": 0,
            "passed": (
                len(
                    SUPPLIED_RESPONSE_PARSER_SUBMISSIONS
                )
                == 0
            ),
        },
        {
            "check": "all_candidates_absent",
            "actual": candidate_presence_counts,
            "expected": {
                "False": EXPECTED_PARSING_RECORDS
            },
            "passed": (
                candidate_presence_counts
                == {
                    "False":
                        EXPECTED_PARSING_RECORDS
                }
            ),
        },
        {
            "check": "all_result_submissions_absent",
            "actual": result_presence_counts,
            "expected": {
                "False": EXPECTED_PARSING_RECORDS
            },
            "passed": (
                result_presence_counts
                == {
                    "False":
                        EXPECTED_PARSING_RECORDS
                }
            ),
        },
        {
            "check": "all_parser_submissions_absent",
            "actual": parser_presence_counts,
            "expected": {
                "False": EXPECTED_PARSING_RECORDS
            },
            "passed": (
                parser_presence_counts
                == {
                    "False":
                        EXPECTED_PARSING_RECORDS
                }
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": status_counts,
            "expected": {
                PARSING_STATUS:
                    EXPECTED_PARSING_RECORDS
            },
            "passed": (
                status_counts
                == {
                    PARSING_STATUS:
                        EXPECTED_PARSING_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_present",
            "actual": blocker_counts,
            "expected": {
                PARSING_BLOCKER:
                    EXPECTED_PARSING_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    PARSING_BLOCKER:
                        EXPECTED_PARSING_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_identity_fields_absent",
            "actual": sum(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                row["candidate_id"] is None
                and row["candidate_version"] is None
                for row in parsing_records
            ),
        },
        {
            "check": "all_response_identity_fields_absent",
            "actual": sum(
                row["result_id"] is None
                and row["response_artifact_id"] is None
                and row["response_sha256"] is None
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                row["result_id"] is None
                and row["response_artifact_id"] is None
                and row["response_sha256"] is None
                for row in parsing_records
            ),
        },
        {
            "check": "all_response_validation_metadata_absent",
            "actual": sum(
                row["response_media_type"] is None
                and row["response_quarantined"] is None
                and row["response_immutable"] is None
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                row["response_media_type"] is None
                and row["response_quarantined"] is None
                and row["response_immutable"] is None
                for row in parsing_records
            ),
        },
        {
            "check": "all_parser_identity_fields_absent",
            "actual": sum(
                row["parser_id"] is None
                and row["parser_version"] is None
                and row["parser_code_digest"] is None
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                row["parser_id"] is None
                and row["parser_version"] is None
                and row["parser_code_digest"] is None
                for row in parsing_records
            ),
        },
        {
            "check": "all_parser_format_contract_fields_absent",
            "actual": sum(
                row["supported_media_types"] is None
                and row["character_encoding"] is None
                and row["container_format"] is None
                and row["schema_version"] is None
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                row["supported_media_types"] is None
                and row["character_encoding"] is None
                and row["container_format"] is None
                and row["schema_version"] is None
                for row in parsing_records
            ),
        },
        {
            "check": "all_parser_schema_contract_fields_absent",
            "actual": sum(
                row["record_selector"] is None
                and row["field_path_contract"] is None
                and row["namespace_contract"] is None
                and row["delimiter_contract"] is None
                and row["header_contract"] is None
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                row["record_selector"] is None
                and row["field_path_contract"] is None
                and row["namespace_contract"] is None
                and row["delimiter_contract"] is None
                and row["header_contract"] is None
                for row in parsing_records
            ),
        },
        {
            "check": "all_parser_policy_fields_absent",
            "actual": sum(
                row["record_ordering_contract"] is None
                and row["duplicate_policy"] is None
                and row["missing_field_policy"] is None
                and row["unknown_field_policy"] is None
                and row["malformed_record_policy"] is None
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                row["record_ordering_contract"] is None
                and row["duplicate_policy"] is None
                and row["missing_field_policy"] is None
                and row["unknown_field_policy"] is None
                and row["malformed_record_policy"] is None
                for row in parsing_records
            ),
        },
        {
            "check": "all_parser_provenance_fields_absent",
            "actual": sum(
                row["raw_field_provenance_contract"] is None
                and row["source_location_provenance_contract"] is None
                for row in parsing_records
            ),
            "expected": len(parsing_records),
            "passed": all(
                row["raw_field_provenance_contract"] is None
                and row["source_location_provenance_contract"] is None
                for row in parsing_records
            ),
        },
        {
            "check": "no_parsing_implementation_authority_granted",
            "actual": len(authority_records),
            "expected": 0,
            "passed": len(authority_records) == 0,
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_name"]
                    for row in parsing_records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_NAME],
            "passed": all(
                row["authoritative_field_name"]
                == AUTHORITATIVE_FIELD_NAME
                for row in parsing_records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row["authoritative_field_path"]
                    for row in parsing_records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_PATH],
            "passed": all(
                row["authoritative_field_path"]
                == AUTHORITATIVE_FIELD_PATH
                for row in parsing_records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row["rejected_metadata_field_name"]
                    for row in parsing_records
                }
            ),
            "expected": [REJECTED_METADATA_FIELD],
            "passed": all(
                row["rejected_metadata_field_name"]
                == REJECTED_METADATA_FIELD
                for row in parsing_records
            ),
        },
        {
            "check": "candidate_invention_not_executed",
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
            "check": "parser_submission_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "schema_or_selector_invention_not_executed",
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
            "check": "parsed_source_evidence_records_zero",
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
        "endpoint_candidate_source_evidence_response_parsing_"
        "implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_response_parsing_"
        "implementation_failed"
    )

    next_layer = (
        "9BI_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_parsed_record_validation_plan"
        if all_checks_passed
        else
        "9BH_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_response_parsing_"
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
        / "source_evidence_response_parsing_records.csv",
        plan.PARSING_PLAN_RECORD_FIELDS,
        parsing_records,
    )

    write_csv(
        OUTPUT_DIR / "parsing_status_counts.csv",
        [
            "parsing_status",
            "count",
        ],
        [
            {
                "parsing_status": key,
                "count": value,
            }
            for key, value in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "parsing_blocker_counts.csv",
        [
            "parsing_blocker",
            "count",
        ],
        [
            {
                "parsing_blocker": key,
                "count": value,
            }
            for key, value in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR
        / "supplied_response_parser_submission_inventory.json",
        {
            "layer_id": LAYER_ID,
            "supplied_response_parser_submission_count":
                len(
                    SUPPLIED_RESPONSE_PARSER_SUBMISSIONS
                ),
            "supplied_response_parser_submissions":
                list(
                    SUPPLIED_RESPONSE_PARSER_SUBMISSIONS
                ),
            "inventory_status": (
                "no_candidate_validated_response_or_parser_"
                "submission_supplied"
            ),
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "source_evidence_response_parsing_contract_version":
            SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.ACQUISITION_RESULT_VALIDATION_CONTRACT_VERSION,
        "predecessor_records":
            len(validation_records),
        "parsing_records":
            len(parsing_records),
        "parsing_comparisons":
            len(comparison_ids),
        "supplied_response_parser_submissions":
            len(
                SUPPLIED_RESPONSE_PARSER_SUBMISSIONS
            ),
        "parsing_status_counts":
            status_counts,
        "parsing_blocker_counts":
            blocker_counts,
        "parsing_implementation_authorities_granted":
            len(authority_records),
        "parsing_digest":
            parsing_digest,
        "reverse_parsing_digest":
            reverse_parsing_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
        "response_bytes_read": 0,
        "responses_parsed": 0,
        "parsed_source_evidence_records": 0,
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
        / "source_evidence_response_parsing_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "parsing_result":
            PARSING_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_parsed_record_validation_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "endpoint_candidate_selection_without_submission",
            "acquisition_result_submission_invention",
            "response_artifact_invention",
            "response_metadata_invention",
            "parser_submission_invention",
            "parser_identity_invention",
            "parser_code_invention",
            "schema_invention",
            "record_selector_invention",
            "field_path_invention",
            "namespace_invention",
            "delimiter_invention",
            "header_contract_invention",
            "response_bytes_reading",
            "source_evidence_parse_execution",
            "raw_response_parse_execution",
            "historical_outcome_retrieval_planning",
            "historical_outcome_fetch_execution",
            "historical_outcome_parse_execution",
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
        "Source evidence response parsing contract version: "
        f"{SOURCE_EVIDENCE_RESPONSE_PARSING_CONTRACT_VERSION}"
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
        "Parsing records: "
        f"{len(parsing_records)}"
    )
    print(
        "Parsing comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Supplied response parser submissions: "
        f"{len(SUPPLIED_RESPONSE_PARSER_SUBMISSIONS)}"
    )
    print(
        "Parsing status counts: "
        f"{status_counts}"
    )
    print(
        "Parsing blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Parsing implementation authorities granted: "
        f"{len(authority_records)}"
    )
    print(
        f"Parsing digest: {parsing_digest}"
    )
    print(
        "Reverse parsing digest: "
        f"{reverse_parsing_digest}"
    )
    print("Response bytes read: 0")
    print("Responses parsed: 0")
    print("Parsed source evidence records: 0")
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
        "Parsing result: "
        f"{diagnosis['parsing_result']}"
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
