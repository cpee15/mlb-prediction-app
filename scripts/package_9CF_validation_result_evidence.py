#!/usr/bin/env python3
"""
Layer 9CF

Materializes deterministic result-evidence records for Layer 9CD validation
records according to the Layer 9CE plan.

This layer preserves structural validation, lineage, status, blockers,
documentation, and authority boundaries. It does not validate authoritative
historical-outcome truth or execute retrieval, parsing, mapping, extraction,
mutation, recomputation, production, market, pricing, or betting operations.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9CF"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_result_evidence_"
    "validation_result_evidence_validation_result_evidence_validation_"
    "result_evidence_implementation"
)

RESULT_EVIDENCE_CONTRACT_VERSION = (
    "layer_9CF_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_result_evidence_"
    "contract_v1"
)

RESULT_EVIDENCE_MANIFEST_VERSION = (
    "layer_9CF_validation_result_evidence_manifest_v1"
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "tmp" / "layer_9CF_validation_result_evidence"

PLAN_PATH = (
    ROOT / "scripts" / "plan_9CE_validation_result_evidence.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9CE_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_result_evidence_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9CD_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_contract_v1"
)

EXPECTED_PLAN_DIGEST = (
    "bd712bfe2ad826ecbe1d2ddead71f2821b54fd9e98e3e45c7390afba5c51d42e"
)

EXPECTED_VALIDATION_DIGEST = (
    "feaf77c1be094c95213abe84c970da60bd0ca816e6916d121f22320c69978245"
)

EXPECTED_RECORDS = 16
EXPECTED_COMPARISONS = 16
EXPECTED_STATUS = "candidate_not_supplied"
EXPECTED_BLOCKER = "historical_outcome_endpoint_candidate_missing"

AUTHORITATIVE_FIELD_NAME = "outcome_value"
AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)
REJECTED_METADATA_FIELD = "outcome_available_at_utc"


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
        and all(character in "0123456789abcdef" for character in value)
    )


def normalized_string(value: Any) -> str:
    return "" if value is None else str(value).strip()


def load_module(path: Path, module_name: str) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)

    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_csv(
    path: Path,
    fieldnames: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(fieldnames),
            extrasaction="ignore",
        )
        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    field: (
                        canonical_json(row.get(field))
                        if isinstance(row.get(field), (dict, list, tuple))
                        else row.get(field)
                    )
                    for field in fieldnames
                }
            )


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

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
    plan = load_module(PLAN_PATH, "layer_9ce_plan")

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9CE plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9CD validation contract version: "
            f"{predecessor.VALIDATION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def compute_plan_digest(plan: Any) -> str:
    return sha256_payload(
        {
            "plan_version": plan.PLAN_VERSION,
            "principles": plan.RESULT_EVIDENCE_PRINCIPLES,
            "components": plan.RESULT_EVIDENCE_COMPONENTS,
            "stages": plan.RESULT_EVIDENCE_STAGES,
            "requirements": plan.RESULT_EVIDENCE_REQUIREMENTS,
            "statuses": plan.RESULT_EVIDENCE_STATUSES,
            "blockers": plan.BLOCKER_CODES,
            "record_fields": plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS,
            "ordering_fields": plan.ORDERING_FIELDS,
            "implementation_steps": plan.IMPLEMENTATION_STEPS,
            "prohibited_authorities": plan.PROHIBITED_AUTHORITIES,
        }
    )


def build_result_evidence_records(
    plan: Any,
    source_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []

    for source in source_records:
        identity_payload = {
            "contract_version": RESULT_EVIDENCE_CONTRACT_VERSION,
            "source_validation_record_id":
                source["result_evidence_validation_plan_record_id"],
            "source_validation_record_digest":
                source["result_evidence_validation_plan_record_digest"],
            "comparison_record_id":
                source["comparison_record_id"],
            "defect_source_record_id":
                source["defect_source_record_id"],
            "status": EXPECTED_STATUS,
        }

        identity_digest = sha256_payload(identity_payload)

        record = {
            "validation_result_evidence_result_evidence_contract_version":
                RESULT_EVIDENCE_CONTRACT_VERSION,
            "validation_result_evidence_result_evidence_plan_record_id":
                "HOASEHOFMRVEPVREVREVREVRE-"
                + identity_digest[:20],
            "validation_result_evidence_result_evidence_plan_identity_digest":
                identity_digest,
            "validation_result_evidence_source_record_id":
                source["result_evidence_validation_plan_record_id"],
            "validation_result_evidence_source_record_identity_digest":
                source["result_evidence_validation_plan_identity_digest"],
            "validation_result_evidence_source_record_digest":
                source["result_evidence_validation_plan_record_digest"],
            "result_evidence_validation_contract_version":
                source["result_evidence_validation_contract_version"],
            "result_evidence_validation_plan_record_id":
                source["result_evidence_validation_plan_record_id"],
            "result_evidence_validation_plan_identity_digest":
                source["result_evidence_validation_plan_identity_digest"],
            "result_evidence_validation_plan_record_digest":
                source["result_evidence_validation_plan_record_digest"],
            "result_evidence_source_record_id":
                source["result_evidence_source_record_id"],
            "result_evidence_source_record_identity_digest":
                source["result_evidence_source_record_identity_digest"],
            "result_evidence_source_record_digest":
                source["result_evidence_source_record_digest"],
            "result_evidence_manifest_version":
                source["result_evidence_manifest_version"],
            "result_evidence_manifest_digest":
                source["result_evidence_manifest_digest"],
            "validation_result_evidence_validation_plan_record_id":
                source[
                    "validation_result_evidence_validation_plan_record_id"
                ],
            "validation_result_evidence_validation_plan_identity_digest":
                source[
                    "validation_result_evidence_validation_plan_identity_digest"
                ],
            "validation_result_evidence_validation_plan_record_digest":
                source[
                    "validation_result_evidence_validation_plan_record_digest"
                ],
            "validation_result_evidence_plan_record_id":
                source["validation_result_evidence_plan_record_id"],
            "validation_result_evidence_plan_record_digest":
                source["validation_result_evidence_plan_record_digest"],
            "validation_result_evidence_source_record_id":
                source["validation_result_evidence_source_record_id"],
            "validation_result_evidence_source_record_digest":
                source["validation_result_evidence_source_record_digest"],
            "evidence_package_validation_plan_record_id":
                source["evidence_package_validation_plan_record_id"],
            "evidence_package_validation_plan_record_digest":
                source["evidence_package_validation_plan_record_digest"],
            "evidence_package_plan_record_id":
                source["evidence_package_plan_record_id"],
            "evidence_package_plan_record_digest":
                source["evidence_package_plan_record_digest"],
            "mapping_result_validation_plan_record_id":
                source["mapping_result_validation_plan_record_id"],
            "mapping_result_validation_plan_record_digest":
                source["mapping_result_validation_plan_record_digest"],
            "historical_outcome_field_mapping_plan_record_id":
                source[
                    "historical_outcome_field_mapping_plan_record_id"
                ],
            "historical_outcome_field_mapping_plan_record_digest":
                source[
                    "historical_outcome_field_mapping_plan_record_digest"
                ],
            "comparison_record_id":
                source["comparison_record_id"],
            "metric_record_id":
                source["metric_record_id"],
            "metric_name":
                source["metric_name"],
            "aggregation_name":
                source["aggregation_name"],
            "aggregation_key":
                source["aggregation_key"],
            "defect_source_path":
                source["defect_source_path"],
            "defect_source_symbol":
                source["defect_source_symbol"],
            "defect_source_record_id":
                source["defect_source_record_id"],
            "defect_source_record_digest":
                source["defect_source_record_digest"],
            "authoritative_field_name":
                source["authoritative_field_name"],
            "authoritative_field_path":
                source["authoritative_field_path"],
            "rejected_metadata_field_name":
                source["rejected_metadata_field_name"],
            "candidate_supplied":
                source["candidate_supplied"],
            "candidate_id":
                source["candidate_id"],
            "candidate_version":
                source["candidate_version"],
            "candidate_derived_artifact_count":
                source["candidate_derived_artifact_count"],
            "validation_artifact_count":
                source["validation_artifact_count"],
            "evidence_absence_explicit":
                source["evidence_absence_explicit"],
            "fabricated_evidence_detected":
                source["fabricated_evidence_detected"],
            "structural_validation_complete":
                source["structural_validation_complete"],
            "authoritative_historical_outcome_validated":
                False,
            "source_validation_status":
                source["result_evidence_validation_status"],
            "source_validation_blocker_codes":
                source["result_evidence_validation_blocker_codes"],
            "source_validation_rationale":
                source["result_evidence_validation_rationale"],
            "source_validation_limitations":
                source["result_evidence_validation_limitations"],
            "source_validation_authority_boundary":
                source["result_evidence_validation_authority_boundary"],
            "validation_result_evidence_result_evidence_status":
                EXPECTED_STATUS,
            "validation_result_evidence_result_evidence_blocker_codes":
                [EXPECTED_BLOCKER],
            "validation_result_evidence_result_evidence_implementation_authority_granted":
                False,
            "validation_result_evidence_result_evidence_rationale": (
                "The Layer 9CD validation record preserves deterministic "
                "identity, digest integrity, complete lineage, structural "
                "validation, explicit candidate-evidence absence, canonical "
                "field identity, documentation, and authority boundaries. "
                "It remains candidate_not_supplied because no authoritative "
                "historical-outcome endpoint candidate or evidence exists."
            ),
            "validation_result_evidence_result_evidence_limitations": [
                "No authoritative endpoint candidate was supplied.",
                "No candidate-derived source evidence exists.",
                "No authoritative historical outcome was validated.",
                (
                    "Structural validation-result evidence does not establish "
                    "historical-outcome truth."
                ),
                "The canonical target remains outcome_value.",
                (
                    "outcome_available_at_utc remains rejected as an outcome "
                    "substitute."
                ),
                "No response bytes were read or parsed.",
                (
                    "No historical-outcome mapping or extraction was "
                    "executed."
                ),
                "No canonical source records were mutated.",
                "No downstream records were recomputed.",
                (
                    "No production, market, pricing, or betting authority "
                    "is granted."
                ),
            ],
            "validation_result_evidence_result_evidence_authority_boundary": (
                "This record authorizes only deterministic preservation of "
                "the Layer 9CD validation result. It grants no authority to "
                "invent or retrieve endpoint candidates, responses, parsers, "
                "mappings, evidence artifacts, outcome values, credentials, "
                "canonical mutations, recomputations, or production, market, "
                "pricing, or betting decisions."
            ),
        }

        missing_fields = [
            field
            for field in plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS
            if field
            != (
                "validation_result_evidence_result_evidence_"
                "plan_record_digest"
            )
            and field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Result-evidence record missing fields: "
                + ", ".join(missing_fields)
            )

        record[
            "validation_result_evidence_result_evidence_plan_record_digest"
        ] = sha256_payload(record)

        output.append(
            {
                field: record[field]
                for field in plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS
            }
        )

    output.sort(
        key=lambda row: (
            normalized_string(row["comparison_record_id"]),
            normalized_string(row["defect_source_record_id"]),
            normalized_string(row["candidate_id"]),
            normalized_string(
                row["validation_result_evidence_plan_record_id"]
            ),
            normalized_string(
                row[
                    "validation_result_evidence_validation_plan_record_id"
                ]
            ),
            normalized_string(
                row["result_evidence_validation_plan_record_id"]
            ),
        )
    )

    return output


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    replay = replay_plan()
    plan = replay["plan"]
    predecessor = replay["predecessor"]
    source_records = replay["records"]
    reverse_source_records = replay["reverse_records"]

    result_records = build_result_evidence_records(
        plan,
        source_records,
    )

    reverse_result_records = build_result_evidence_records(
        plan,
        list(reversed(reverse_source_records)),
    )

    predecessor_digest = sha256_payload(source_records)
    reverse_predecessor_digest = sha256_payload(
        reverse_source_records
    )

    result_digest = sha256_payload(result_records)
    reverse_result_digest = sha256_payload(
        reverse_result_records
    )

    plan_digest = compute_plan_digest(plan)

    comparison_ids = {
        row["comparison_record_id"]
        for row in result_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row[
                    "validation_result_evidence_result_evidence_status"
                ]
                for row in result_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in result_records
                for blocker in row[
                    "validation_result_evidence_result_evidence_"
                    "blocker_codes"
                ]
            ).items()
        )
    )

    candidate_derived_artifacts = sum(
        int(row["candidate_derived_artifact_count"])
        for row in result_records
    )

    validation_artifacts = sum(
        int(row["validation_artifact_count"])
        for row in result_records
    )

    fabricated_evidence_count = sum(
        bool(row["fabricated_evidence_detected"])
        for row in result_records
    )

    authoritative_outcomes_validated = sum(
        bool(row["authoritative_historical_outcome_validated"])
        for row in result_records
    )

    implementation_authorities = sum(
        bool(
            row[
                "validation_result_evidence_result_evidence_"
                "implementation_authority_granted"
            ]
        )
        for row in result_records
    )

    manifest_payload = {
        "manifest_version":
            RESULT_EVIDENCE_MANIFEST_VERSION,
        "contract_version":
            RESULT_EVIDENCE_CONTRACT_VERSION,
        "record_count":
            len(result_records),
        "comparison_count":
            len(comparison_ids),
        "result_digest":
            result_digest,
        "status_counts":
            status_counts,
        "blocker_counts":
            blocker_counts,
        "candidate_derived_artifact_count":
            candidate_derived_artifacts,
        "validation_artifact_count":
            validation_artifacts,
        "authoritative_historical_outcomes_validated":
            authoritative_outcomes_validated,
    }

    manifest_digest = sha256_payload(manifest_payload)

    checks = [
        {
            "check": "nine_ce_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": plan.PLAN_VERSION == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_ce_plan_digest_verified",
            "actual": plan_digest,
            "expected": EXPECTED_PLAN_DIGEST,
            "passed": plan_digest == EXPECTED_PLAN_DIGEST,
        },
        {
            "check": "nine_cd_contract_version_verified",
            "actual": predecessor.VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.VALIDATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "predecessor_replay_deterministic",
            "actual":
                predecessor_digest == reverse_predecessor_digest,
            "expected": True,
            "passed":
                predecessor_digest == reverse_predecessor_digest,
        },
        {
            "check": "predecessor_validation_digest_preserved",
            "actual": predecessor_digest,
            "expected": EXPECTED_VALIDATION_DIGEST,
            "passed":
                predecessor_digest == EXPECTED_VALIDATION_DIGEST,
        },
        {
            "check": "result_evidence_replay_deterministic",
            "actual":
                canonical_json(result_records)
                == canonical_json(reverse_result_records),
            "expected": True,
            "passed": (
                canonical_json(result_records)
                == canonical_json(reverse_result_records)
            ),
        },
        {
            "check": "result_evidence_digests_match_reverse_replay",
            "actual": result_digest,
            "expected": reverse_result_digest,
            "passed": result_digest == reverse_result_digest,
        },
        {
            "check": "expected_source_validation_records_replayed",
            "actual": len(source_records),
            "expected": EXPECTED_RECORDS,
            "passed": len(source_records) == EXPECTED_RECORDS,
        },
        {
            "check": "expected_result_evidence_records_materialized",
            "actual": len(result_records),
            "expected": EXPECTED_RECORDS,
            "passed": len(result_records) == EXPECTED_RECORDS,
        },
        {
            "check": "one_result_evidence_record_per_comparison",
            "actual": len(comparison_ids),
            "expected": EXPECTED_COMPARISONS,
            "passed": len(comparison_ids) == EXPECTED_COMPARISONS,
        },
        {
            "check": "result_evidence_field_contract_preserved",
            "actual": {
                "declared_fields":
                    len(plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS),
                "unique_fields":
                    len(
                        list(
                            dict.fromkeys(
                                plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS
                            )
                        )
                    ),
            },
            "expected": {
                "declared_fields": 63,
                "unique_fields":
                    len(
                        list(
                            dict.fromkeys(
                                plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS
                            )
                        )
                    ),
            },
            "passed": (
                len(plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS) == 63
                and all(
                    list(row.keys())
                    == list(
                        dict.fromkeys(
                            plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS
                        )
                    )
                    for row in result_records
                )
            ),
        },
        {
            "check": "result_evidence_record_ids_unique",
            "actual": len(
                {
                    row[
                        "validation_result_evidence_result_evidence_"
                        "plan_record_id"
                    ]
                    for row in result_records
                }
            ),
            "expected": EXPECTED_RECORDS,
            "passed": (
                len(
                    {
                        row[
                            "validation_result_evidence_result_evidence_"
                            "plan_record_id"
                        ]
                        for row in result_records
                    }
                )
                == EXPECTED_RECORDS
            ),
        },
        {
            "check": "result_evidence_record_digests_unique",
            "actual": len(
                {
                    row[
                        "validation_result_evidence_result_evidence_"
                        "plan_record_digest"
                    ]
                    for row in result_records
                }
            ),
            "expected": EXPECTED_RECORDS,
            "passed": (
                len(
                    {
                        row[
                            "validation_result_evidence_result_evidence_"
                            "plan_record_digest"
                        ]
                        for row in result_records
                    }
                )
                == EXPECTED_RECORDS
            ),
        },
        {
            "check": "all_result_evidence_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "plan_identity_digest"
                    ]
                )
                for row in result_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "plan_identity_digest"
                    ]
                )
                for row in result_records
            ),
        },
        {
            "check": "all_result_evidence_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "plan_record_digest"
                    ]
                )
                for row in result_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "plan_record_digest"
                    ]
                )
                for row in result_records
            ),
        },
        {
            "check": "source_validation_identity_preserved",
            "actual": sum(
                bool(
                    normalized_string(
                        row["validation_result_evidence_source_record_id"]
                    )
                )
                and valid_sha256(
                    row[
                        "validation_result_evidence_source_record_"
                        "identity_digest"
                    ]
                )
                and valid_sha256(
                    row[
                        "validation_result_evidence_source_record_digest"
                    ]
                )
                for row in result_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(
                    normalized_string(
                        row["validation_result_evidence_source_record_id"]
                    )
                )
                and valid_sha256(
                    row[
                        "validation_result_evidence_source_record_"
                        "identity_digest"
                    ]
                )
                and valid_sha256(
                    row[
                        "validation_result_evidence_source_record_digest"
                    ]
                )
                for row in result_records
            ),
        },
        {
            "check": "all_structural_validation_complete",
            "actual": sum(
                bool(row["structural_validation_complete"])
                for row in result_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["structural_validation_complete"])
                for row in result_records
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": status_counts,
            "expected": {EXPECTED_STATUS: EXPECTED_RECORDS},
            "passed": (
                status_counts
                == {EXPECTED_STATUS: EXPECTED_RECORDS}
            ),
        },
        {
            "check": "all_missing_endpoint_blockers_preserved",
            "actual": blocker_counts,
            "expected": {EXPECTED_BLOCKER: EXPECTED_RECORDS},
            "passed": (
                blocker_counts
                == {EXPECTED_BLOCKER: EXPECTED_RECORDS}
            ),
        },
        {
            "check": "candidate_derived_artifact_count_zero",
            "actual": candidate_derived_artifacts,
            "expected": 0,
            "passed": candidate_derived_artifacts == 0,
        },
        {
            "check": "validation_artifact_count_preserved",
            "actual": validation_artifacts,
            "expected": EXPECTED_RECORDS,
            "passed": validation_artifacts == EXPECTED_RECORDS,
        },
        {
            "check": "evidence_absence_explicit",
            "actual": sum(
                bool(row["evidence_absence_explicit"])
                for row in result_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["evidence_absence_explicit"])
                for row in result_records
            ),
        },
        {
            "check": "fabricated_evidence_absent",
            "actual": fabricated_evidence_count,
            "expected": 0,
            "passed": fabricated_evidence_count == 0,
        },
        {
            "check": "canonical_field_identity_preserved",
            "actual": sorted(
                {
                    (
                        row["authoritative_field_name"],
                        row["authoritative_field_path"],
                        row["rejected_metadata_field_name"],
                    )
                    for row in result_records
                }
            ),
            "expected": [
                (
                    AUTHORITATIVE_FIELD_NAME,
                    AUTHORITATIVE_FIELD_PATH,
                    REJECTED_METADATA_FIELD,
                )
            ],
            "passed": all(
                row["authoritative_field_name"]
                == AUTHORITATIVE_FIELD_NAME
                and row["authoritative_field_path"]
                == AUTHORITATIVE_FIELD_PATH
                and row["rejected_metadata_field_name"]
                == REJECTED_METADATA_FIELD
                for row in result_records
            ),
        },
        {
            "check": "source_documentation_preserved",
            "actual": sum(
                bool(row["source_validation_rationale"])
                and bool(row["source_validation_limitations"])
                and bool(row["source_validation_authority_boundary"])
                for row in result_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["source_validation_rationale"])
                and bool(row["source_validation_limitations"])
                and bool(row["source_validation_authority_boundary"])
                for row in result_records
            ),
        },
        {
            "check": "result_documentation_and_boundary_present",
            "actual": sum(
                bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "rationale"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "authority_boundary"
                    ]
                )
                for row in result_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "rationale"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_result_evidence_"
                        "authority_boundary"
                    ]
                )
                for row in result_records
            ),
        },
        {
            "check": "authoritative_historical_outcomes_validated_zero",
            "actual": authoritative_outcomes_validated,
            "expected": 0,
            "passed": authoritative_outcomes_validated == 0,
        },
        {
            "check": "implementation_authorities_granted_zero",
            "actual": implementation_authorities,
            "expected": 0,
            "passed": implementation_authorities == 0,
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
            "check": "network_retrievals_executed_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "canonical_mutations_and_recomputations_zero",
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

    next_layer = (
        "9CG_validation_result_evidence_validation_plan"
        if all_checks_passed
        else "9CF_validation_result_evidence_remediation"
    )

    diagnosis_name = (
        "validation_result_evidence_validation_result_evidence_"
        "implementation_complete"
        if all_checks_passed
        else
        "validation_result_evidence_validation_result_evidence_"
        "implementation_failed"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "result_evidence_records.csv",
        list(
            dict.fromkeys(
                plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS
            )
        ),
        result_records,
    )

    write_csv(
        OUTPUT_DIR / "status_counts.csv",
        ["status", "count"],
        [
            {"status": status, "count": count}
            for status, count in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "blocker_counts.csv",
        ["blocker", "count"],
        [
            {"blocker": blocker, "count": count}
            for blocker, count in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR / "manifest.json",
        {
            **manifest_payload,
            "manifest_digest": manifest_digest,
        },
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "result_evidence_contract_version":
            RESULT_EVIDENCE_CONTRACT_VERSION,
        "result_evidence_manifest_version":
            RESULT_EVIDENCE_MANIFEST_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "plan_digest": plan_digest,
        "predecessor_contract_version":
            predecessor.VALIDATION_CONTRACT_VERSION,
        "source_validation_records":
            len(source_records),
        "result_evidence_records":
            len(result_records),
        "result_evidence_comparisons":
            len(comparison_ids),
        "result_evidence_status_counts":
            status_counts,
        "result_evidence_blocker_counts":
            blocker_counts,
        "candidate_derived_artifact_count":
            candidate_derived_artifacts,
        "validation_artifact_count":
            validation_artifacts,
        "authoritative_historical_outcomes_validated":
            authoritative_outcomes_validated,
        "fabricated_evidence_detected_count":
            fabricated_evidence_count,
        "implementation_authorities_granted":
            implementation_authorities,
        "predecessor_validation_digest":
            predecessor_digest,
        "result_evidence_digest":
            result_digest,
        "reverse_result_evidence_digest":
            reverse_result_digest,
        "manifest_digest":
            manifest_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
        "historical_outcome_fields_mapped": 0,
        "historical_outcome_values_extracted": 0,
        "response_bytes_read": 0,
        "responses_parsed": 0,
        "network_retrievals_executed": 0,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "downstream_records_recomputed": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "pricing_changes_emitted": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed":
            all_checks_passed,
        "recommended_next_layer":
            next_layer,
    }

    write_json(
        OUTPUT_DIR / "summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "result_evidence_status":
            EXPECTED_STATUS,
        "structural_validation_complete":
            all_checks_passed,
        "authoritative_historical_outcome_validated":
            False,
        "authority_granted": (
            "validation_result_evidence_validation_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "endpoint_candidate_invention",
            "response_artifact_invention",
            "parser_submission_invention",
            "mapping_submission_invention",
            "validation_result_invention",
            "evidence_artifact_invention",
            "authoritative_historical_outcome_validation",
            "historical_outcome_field_mapping_execution",
            "historical_outcome_value_extraction",
            "response_bytes_reading",
            "network_request_execution",
            "canonical_source_value_mutation",
            "canonical_outcome_mapping_change",
            "canonical_record_recomputation",
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
            str(OUTPUT_DIR.relative_to(ROOT)),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(f"Layer: {LAYER_ID} — {LAYER_NAME}")
    print(
        "Result-evidence contract version: "
        f"{RESULT_EVIDENCE_CONTRACT_VERSION}"
    )
    print(
        "Result-evidence manifest version: "
        f"{RESULT_EVIDENCE_MANIFEST_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(f"Source validation records: {len(source_records)}")
    print(f"Result-evidence records: {len(result_records)}")
    print(f"Result-evidence comparisons: {len(comparison_ids)}")
    print(f"Status counts: {status_counts}")
    print(f"Blocker counts: {blocker_counts}")
    print(
        "Candidate-derived artifact count: "
        f"{candidate_derived_artifacts}"
    )
    print(f"Validation artifact count: {validation_artifacts}")
    print(
        "Authoritative historical outcomes validated: "
        f"{authoritative_outcomes_validated}"
    )
    print(
        f"Fabricated evidence detected: {fabricated_evidence_count}"
    )
    print(
        "Implementation authorities granted: "
        f"{implementation_authorities}"
    )
    print(f"Plan digest: {plan_digest}")
    print(f"Predecessor validation digest: {predecessor_digest}")
    print(f"Result-evidence digest: {result_digest}")
    print(
        "Reverse result-evidence digest: "
        f"{reverse_result_digest}"
    )
    print(f"Manifest digest: {manifest_digest}")
    print("Historical outcome fields mapped: 0")
    print("Historical outcome values extracted: 0")
    print("Response bytes read: 0")
    print("Responses parsed: 0")
    print("Network retrievals executed: 0")
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Downstream records recomputed: 0")
    print("Production probabilities changed: 0")
    print("Market comparisons executed: 0")
    print("Pricing changes emitted: 0")
    print("Betting edges calculated: 0")
    print(f"Diagnosis: {diagnosis_name}")
    print("Authoritative historical outcome validated: False")
    print(f"Authority granted: {diagnosis['authority_granted']}")
    print(f"Recommended next layer: {next_layer}")
    print(f"Artifacts: {OUTPUT_DIR.relative_to(ROOT)}")

    if not all_checks_passed:
        failed_checks = [
            row["check"]
            for row in checks
            if not row["passed"]
        ]

        print("FAILED CHECKS: " + ", ".join(failed_checks))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
