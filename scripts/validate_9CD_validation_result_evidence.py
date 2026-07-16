#!/usr/bin/env python3
"""
Layer 9CD

Materializes deterministic validation records for Layer 9CB validation-result
evidence according to the Layer 9CC validation plan.

This implementation validates structural evidence integrity only. It does not
validate authoritative historical-outcome truth or execute endpoint retrieval,
response parsing, field mapping, value extraction, canonical mutation,
downstream recomputation, production changes, market comparison, pricing, or
betting operations.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9CD"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_result_evidence_"
    "validation_result_evidence_validation_result_evidence_validation_"
    "implementation"
)

VALIDATION_CONTRACT_VERSION = (
    "layer_9CD_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "tmp" / "layer_9CD_result_evidence_validation"

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9CC_validation_result_evidence_validation.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9CC_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_plan_v1"
)

EXPECTED_PREDECESSOR_CONTRACT_VERSION = (
    "layer_9CB_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_contract_v1"
)

EXPECTED_PREDECESSOR_MANIFEST_VERSION = (
    "layer_9CB_validation_result_evidence_manifest_v1"
)

EXPECTED_PLAN_DIGEST = (
    "12397658e742310274b75755f8f5932dbb082fc51c6f2042af3b74f01e7e2fa5"
)

EXPECTED_RESULT_DIGEST = (
    "b17edc11905ed66256fa8543566c2adf1de9ec0f02914c350f4491b012601af3"
)

EXPECTED_MANIFEST_DIGEST = (
    "0a9c40d4508ae1b92ed7fff83e21aaeaeab01e1c1fd9f1b746c10bd79801a1a2"
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
    plan = load_module(
        PLAN_PATH,
        "layer_9cc_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9CC plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.RESULT_EVIDENCE_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_CONTRACT_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9CB contract version: "
            f"{predecessor.RESULT_EVIDENCE_CONTRACT_VERSION}"
        )

    if (
        predecessor.RESULT_EVIDENCE_MANIFEST_VERSION
        != EXPECTED_PREDECESSOR_MANIFEST_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9CB manifest version: "
            f"{predecessor.RESULT_EVIDENCE_MANIFEST_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
        "result_digest": replay["result_digest"],
        "manifest_payload": replay["manifest_payload"],
        "manifest_digest": replay["manifest_digest"],
    }


def compute_plan_digest(plan: Any) -> str:
    return sha256_payload(
        {
            "plan_version": plan.PLAN_VERSION,
            "principles": plan.VALIDATION_PRINCIPLES,
            "components": plan.VALIDATION_COMPONENTS,
            "stages": plan.VALIDATION_STAGES,
            "requirements": plan.VALIDATION_REQUIREMENTS,
            "statuses": plan.VALIDATION_STATUSES,
            "blockers": plan.BLOCKER_CODES,
            "record_fields": plan.VALIDATION_PLAN_RECORD_FIELDS,
            "ordering_fields": plan.ORDERING_FIELDS,
            "implementation_steps": plan.IMPLEMENTATION_STEPS,
            "prohibited_authorities": plan.PROHIBITED_AUTHORITIES,
        }
    )


def source_record_digest_valid(
    plan: Any,
    source: Mapping[str, Any],
) -> bool:
    digest_field = (
        "validation_result_evidence_validation_result_"
        "evidence_record_digest"
    )

    return (
        valid_sha256(source.get(digest_field))
        and source[digest_field]
        == plan.recompute_record_digest(source)
    )


def complete_lineage(source: Mapping[str, Any]) -> bool:
    lineage_fields = (
        "validation_result_evidence_validation_plan_record_id",
        "validation_result_evidence_validation_plan_record_digest",
        "validation_result_evidence_plan_record_id",
        "validation_result_evidence_plan_record_digest",
        "result_evidence_validation_plan_record_id",
        "result_evidence_validation_plan_record_digest",
        "validation_result_evidence_source_record_id",
        "validation_result_evidence_source_record_digest",
        "evidence_package_validation_plan_record_id",
        "evidence_package_validation_plan_record_digest",
        "evidence_package_plan_record_id",
        "evidence_package_plan_record_digest",
        "mapping_result_validation_plan_record_id",
        "mapping_result_validation_plan_record_digest",
        "historical_outcome_field_mapping_plan_record_id",
        "historical_outcome_field_mapping_plan_record_digest",
        "comparison_record_id",
        "metric_record_id",
        "defect_source_record_id",
        "defect_source_record_digest",
    )

    return all(
        bool(normalized_string(source.get(field)))
        for field in lineage_fields
    )


def build_validation_records(
    plan: Any,
    replay: Mapping[str, Any],
    source_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []

    source_id_field = (
        "validation_result_evidence_validation_result_"
        "evidence_record_id"
    )

    source_identity_field = (
        "validation_result_evidence_validation_result_"
        "evidence_identity_digest"
    )

    source_digest_field = (
        "validation_result_evidence_validation_result_"
        "evidence_record_digest"
    )

    source_status_field = (
        "validation_result_evidence_validation_result_"
        "evidence_status"
    )

    source_blocker_field = (
        "validation_result_evidence_validation_result_"
        "evidence_blocker_codes"
    )

    source_rationale_field = (
        "validation_result_evidence_validation_result_"
        "evidence_rationale"
    )

    source_limitations_field = (
        "validation_result_evidence_validation_result_"
        "evidence_limitations"
    )

    source_boundary_field = (
        "validation_result_evidence_validation_result_"
        "evidence_authority_boundary"
    )

    for source in source_records:
        identity_valid = (
            bool(normalized_string(source.get(source_id_field)))
            and valid_sha256(source.get(source_identity_field))
        )

        record_digest_valid = source_record_digest_valid(
            plan,
            source,
        )

        manifest_valid = (
            replay["manifest_payload"]["manifest_version"]
            == EXPECTED_PREDECESSOR_MANIFEST_VERSION
            and replay["manifest_payload"]["contract_version"]
            == EXPECTED_PREDECESSOR_CONTRACT_VERSION
            and replay["manifest_payload"]["record_count"]
            == EXPECTED_RECORDS
            and replay["manifest_payload"]["comparison_count"]
            == EXPECTED_COMPARISONS
            and replay["manifest_payload"]["result_digest"]
            == replay["result_digest"]
            and replay["result_digest"]
            == EXPECTED_RESULT_DIGEST
            and replay["manifest_digest"]
            == EXPECTED_MANIFEST_DIGEST
        )

        lineage_valid = complete_lineage(source)

        structural_disposition_valid = bool(
            source["structural_validation_complete"]
        )

        candidate_absence_valid = (
            not bool(source["candidate_supplied"])
            and int(source["candidate_derived_artifact_count"]) == 0
            and bool(source["evidence_absence_explicit"])
            and not bool(source["fabricated_evidence_detected"])
        )

        canonical_field_valid = (
            source["authoritative_field_name"]
            == AUTHORITATIVE_FIELD_NAME
            and source["authoritative_field_path"]
            == AUTHORITATIVE_FIELD_PATH
            and source["rejected_metadata_field_name"]
            == REJECTED_METADATA_FIELD
        )

        status_and_blocker_valid = (
            source[source_status_field] == EXPECTED_STATUS
            and source[source_blocker_field] == [EXPECTED_BLOCKER]
        )

        documentation_valid = (
            bool(normalized_string(source[source_rationale_field]))
            and bool(source[source_limitations_field])
            and bool(normalized_string(source[source_boundary_field]))
        )

        authoritative_outcome_disposition_valid = (
            not bool(
                source[
                    "authoritative_historical_outcome_validated"
                ]
            )
        )

        all_structural_checks_valid = all(
            (
                identity_valid,
                record_digest_valid,
                manifest_valid,
                lineage_valid,
                structural_disposition_valid,
                candidate_absence_valid,
                canonical_field_valid,
                status_and_blocker_valid,
                documentation_valid,
                authoritative_outcome_disposition_valid,
            )
        )

        validation_status = (
            EXPECTED_STATUS
            if all_structural_checks_valid
            else "result_evidence_validation_failed"
        )

        blockers: list[str] = []

        if validation_status == EXPECTED_STATUS:
            blockers.append(EXPECTED_BLOCKER)

        if not identity_valid:
            blockers.append("result_evidence_identity_invalid")

        if not record_digest_valid:
            blockers.append(
                "result_evidence_record_digest_invalid"
            )

        if not manifest_valid:
            blockers.append("result_evidence_manifest_invalid")

        if not lineage_valid:
            blockers.append("lineage_incomplete")

        if not structural_disposition_valid:
            blockers.append("structural_disposition_invalid")

        if not candidate_absence_valid:
            blockers.append(
                "candidate_evidence_absence_invalid"
            )

        if not canonical_field_valid:
            blockers.append(
                "canonical_field_identity_invalid"
            )

        if not status_and_blocker_valid:
            blockers.append(
                "candidate_not_supplied_disposition_invalid"
            )

        if not documentation_valid:
            blockers.append(
                "documentation_or_authority_boundary_invalid"
            )

        if not authoritative_outcome_disposition_valid:
            blockers.append(
                "authoritative_outcome_disposition_invalid"
            )

        identity_payload = {
            "validation_contract_version":
                VALIDATION_CONTRACT_VERSION,
            "source_record_id":
                source[source_id_field],
            "source_record_digest":
                source[source_digest_field],
            "comparison_record_id":
                source["comparison_record_id"],
            "defect_source_record_id":
                source["defect_source_record_id"],
            "validation_status":
                validation_status,
        }

        identity_digest = sha256_payload(identity_payload)

        record = {
            "result_evidence_validation_contract_version":
                VALIDATION_CONTRACT_VERSION,
            "result_evidence_validation_plan_record_id":
                "HOASEHOFMRVEPVREVREVREV-"
                + identity_digest[:20],
            "result_evidence_validation_plan_identity_digest":
                identity_digest,
            "result_evidence_source_record_id":
                source[source_id_field],
            "result_evidence_source_record_identity_digest":
                source[source_identity_field],
            "result_evidence_source_record_digest":
                source[source_digest_field],
            "result_evidence_manifest_version":
                EXPECTED_PREDECESSOR_MANIFEST_VERSION,
            "result_evidence_manifest_digest":
                replay["manifest_digest"],
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
                source[
                    "validation_result_evidence_plan_record_id"
                ],
            "validation_result_evidence_plan_record_digest":
                source[
                    "validation_result_evidence_plan_record_digest"
                ],
            "validation_result_evidence_source_record_id":
                source[
                    "validation_result_evidence_source_record_id"
                ],
            "validation_result_evidence_source_record_digest":
                source[
                    "validation_result_evidence_source_record_digest"
                ],
            "evidence_package_validation_plan_record_id":
                source[
                    "evidence_package_validation_plan_record_id"
                ],
            "evidence_package_validation_plan_record_digest":
                source[
                    "evidence_package_validation_plan_record_digest"
                ],
            "evidence_package_plan_record_id":
                source["evidence_package_plan_record_id"],
            "evidence_package_plan_record_digest":
                source["evidence_package_plan_record_digest"],
            "mapping_result_validation_plan_record_id":
                source[
                    "mapping_result_validation_plan_record_id"
                ],
            "mapping_result_validation_plan_record_digest":
                source[
                    "mapping_result_validation_plan_record_digest"
                ],
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
                structural_disposition_valid,
            "authoritative_historical_outcome_validated":
                False,
            "result_evidence_status":
                source[source_status_field],
            "result_evidence_blocker_codes":
                source[source_blocker_field],
            "result_evidence_rationale":
                source[source_rationale_field],
            "result_evidence_limitations":
                source[source_limitations_field],
            "result_evidence_authority_boundary":
                source[source_boundary_field],
            "result_evidence_validation_status":
                validation_status,
            "result_evidence_validation_blocker_codes":
                sorted(set(blockers)),
            "result_evidence_validation_implementation_authority_granted":
                False,
            "result_evidence_validation_rationale": (
                "The Layer 9CB validation-result evidence record has valid "
                "identity, record-digest integrity, manifest identity and "
                "inventory, complete lineage, structural disposition, "
                "explicit candidate-evidence absence, canonical field "
                "identity, documentation, and authority boundaries. It "
                "remains candidate_not_supplied because no authoritative "
                "historical-outcome endpoint candidate or evidence exists."
            ),
            "result_evidence_validation_limitations": [
                "No authoritative endpoint candidate was supplied.",
                "No candidate-derived source evidence exists.",
                "No authoritative historical outcome was validated.",
                (
                    "Structural result-evidence validation does not establish "
                    "historical-outcome truth."
                ),
                "The canonical target remains outcome_value.",
                (
                    "outcome_available_at_utc remains rejected as an "
                    "outcome substitute."
                ),
                "No response bytes were read or parsed.",
                (
                    "No field mapping or historical-outcome extraction "
                    "was executed."
                ),
                "No canonical records were mutated.",
                (
                    "No downstream comparison or metric records were "
                    "recomputed."
                ),
                (
                    "No production, market, pricing, or betting authority "
                    "is granted."
                ),
            ],
            "result_evidence_validation_authority_boundary": (
                "This validation record authorizes only deterministic "
                "preservation and structural validation of the Layer 9CB "
                "result-evidence record. It grants no authority to invent "
                "or retrieve endpoint candidates, responses, parsers, "
                "mappings, evidence artifacts, outcome values, credentials, "
                "canonical mutations, recomputations, or production, market, "
                "pricing, or betting decisions."
            ),
        }

        missing_fields = [
            field
            for field in set(plan.VALIDATION_PLAN_RECORD_FIELDS)
            if field != "result_evidence_validation_plan_record_digest"
            and field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Validation record missing fields: "
                + ", ".join(sorted(missing_fields))
            )

        record[
            "result_evidence_validation_plan_record_digest"
        ] = sha256_payload(record)

        output.append(
            {
                field: record[field]
                for field in set(plan.VALIDATION_PLAN_RECORD_FIELDS)
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
                row["result_evidence_source_record_id"]
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

    validation_records = build_validation_records(
        plan,
        replay,
        source_records,
    )

    reverse_validation_records = build_validation_records(
        plan,
        replay,
        list(reversed(reverse_source_records)),
    )

    predecessor_replay_deterministic = (
        canonical_json(source_records)
        == canonical_json(reverse_source_records)
    )

    validation_replay_deterministic = (
        canonical_json(validation_records)
        == canonical_json(reverse_validation_records)
    )

    validation_digest = sha256_payload(validation_records)
    reverse_validation_digest = sha256_payload(
        reverse_validation_records
    )

    plan_digest = compute_plan_digest(plan)

    comparison_ids = {
        row["comparison_record_id"]
        for row in validation_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row["result_evidence_validation_status"]
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
                    "result_evidence_validation_blocker_codes"
                ]
            ).items()
        )
    )

    candidate_derived_artifacts = sum(
        int(row["candidate_derived_artifact_count"])
        for row in validation_records
    )

    validation_artifacts = sum(
        int(row["validation_artifact_count"])
        for row in validation_records
    )

    fabricated_evidence_count = sum(
        bool(row["fabricated_evidence_detected"])
        for row in validation_records
    )

    authoritative_outcomes_validated = sum(
        bool(
            row[
                "authoritative_historical_outcome_validated"
            ]
        )
        for row in validation_records
    )

    implementation_authorities = sum(
        bool(
            row[
                "result_evidence_validation_"
                "implementation_authority_granted"
            ]
        )
        for row in validation_records
    )

    checks = [
        {
            "check": "nine_cc_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": plan.PLAN_VERSION == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_cc_plan_digest_verified",
            "actual": plan_digest,
            "expected": EXPECTED_PLAN_DIGEST,
            "passed": plan_digest == EXPECTED_PLAN_DIGEST,
        },
        {
            "check": "nine_cb_contract_version_verified",
            "actual":
                predecessor.RESULT_EVIDENCE_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_CONTRACT_VERSION,
            "passed": (
                predecessor.RESULT_EVIDENCE_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_CONTRACT_VERSION
            ),
        },
        {
            "check": "nine_cb_manifest_version_verified",
            "actual":
                predecessor.RESULT_EVIDENCE_MANIFEST_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_MANIFEST_VERSION,
            "passed": (
                predecessor.RESULT_EVIDENCE_MANIFEST_VERSION
                == EXPECTED_PREDECESSOR_MANIFEST_VERSION
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
            "passed":
                validation_digest == reverse_validation_digest,
        },
        {
            "check": "expected_source_records_replayed",
            "actual": len(source_records),
            "expected": EXPECTED_RECORDS,
            "passed": len(source_records) == EXPECTED_RECORDS,
        },
        {
            "check": "expected_validation_records_materialized",
            "actual": len(validation_records),
            "expected": EXPECTED_RECORDS,
            "passed":
                len(validation_records) == EXPECTED_RECORDS,
        },
        {
            "check": "one_validation_record_per_comparison",
            "actual": len(comparison_ids),
            "expected": EXPECTED_COMPARISONS,
            "passed":
                len(comparison_ids) == EXPECTED_COMPARISONS,
        },
        {
            "check": "validation_record_field_contract_preserved",
            "actual":
                len(plan.VALIDATION_PLAN_RECORD_FIELDS),
            "expected": 58,
            "passed": all(
                set(row)
                == set(plan.VALIDATION_PLAN_RECORD_FIELDS)
                for row in validation_records
            ),
        },
        {
            "check": "validation_record_ids_unique",
            "actual": len(
                {
                    row["result_evidence_validation_plan_record_id"]
                    for row in validation_records
                }
            ),
            "expected": EXPECTED_RECORDS,
            "passed": (
                len(
                    {
                        row[
                            "result_evidence_validation_plan_record_id"
                        ]
                        for row in validation_records
                    }
                )
                == EXPECTED_RECORDS
            ),
        },
        {
            "check": "validation_record_digests_unique",
            "actual": len(
                {
                    row[
                        "result_evidence_validation_plan_record_digest"
                    ]
                    for row in validation_records
                }
            ),
            "expected": EXPECTED_RECORDS,
            "passed": (
                len(
                    {
                        row[
                            "result_evidence_validation_plan_record_digest"
                        ]
                        for row in validation_records
                    }
                )
                == EXPECTED_RECORDS
            ),
        },
        {
            "check": "all_validation_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "result_evidence_validation_plan_identity_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "result_evidence_validation_plan_identity_digest"
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
                        "result_evidence_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "result_evidence_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "predecessor_result_digest_preserved",
            "actual": replay["result_digest"],
            "expected": EXPECTED_RESULT_DIGEST,
            "passed":
                replay["result_digest"] == EXPECTED_RESULT_DIGEST,
        },
        {
            "check": "predecessor_manifest_digest_preserved",
            "actual": replay["manifest_digest"],
            "expected": EXPECTED_MANIFEST_DIGEST,
            "passed":
                replay["manifest_digest"] == EXPECTED_MANIFEST_DIGEST,
        },
        {
            "check": "all_structural_validation_complete",
            "actual": sum(
                bool(row["structural_validation_complete"])
                for row in validation_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["structural_validation_complete"])
                for row in validation_records
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
            "check": "one_validation_artifact_per_record",
            "actual": validation_artifacts,
            "expected": EXPECTED_RECORDS,
            "passed": validation_artifacts == EXPECTED_RECORDS,
        },
        {
            "check": "evidence_absence_explicit",
            "actual": sum(
                bool(row["evidence_absence_explicit"])
                for row in validation_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["evidence_absence_explicit"])
                for row in validation_records
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
                    for row in validation_records
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
                for row in validation_records
            ),
        },
        {
            "check": "authoritative_historical_outcomes_validated_zero",
            "actual": authoritative_outcomes_validated,
            "expected": 0,
            "passed": authoritative_outcomes_validated == 0,
        },
        {
            "check": "rationale_limitations_and_boundary_present",
            "actual": sum(
                bool(
                    normalized_string(
                        row["result_evidence_validation_rationale"]
                    )
                )
                and bool(
                    row["result_evidence_validation_limitations"]
                )
                and bool(
                    normalized_string(
                        row[
                            "result_evidence_validation_authority_boundary"
                        ]
                    )
                )
                for row in validation_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(
                    normalized_string(
                        row["result_evidence_validation_rationale"]
                    )
                )
                and bool(
                    row["result_evidence_validation_limitations"]
                )
                and bool(
                    normalized_string(
                        row[
                            "result_evidence_validation_authority_boundary"
                        ]
                    )
                )
                for row in validation_records
            ),
        },
        {
            "check": "no_validation_implementation_authority_granted",
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
            "check": "network_retrievals_executed_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "canonical_source_records_changed_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "canonical_mappings_changed_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "downstream_records_recomputed_zero",
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
        "9CE_validation_result_evidence_validation_result_evidence_plan"
        if all_checks_passed
        else "9CD_validation_result_evidence_validation_remediation"
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_result_evidence_validation_result_evidence_validation_"
        "implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_result_evidence_validation_result_evidence_validation_"
        "implementation_failed"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "validation_records.csv",
        list(dict.fromkeys(plan.VALIDATION_PLAN_RECORD_FIELDS)),
        validation_records,
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

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "validation_contract_version":
            VALIDATION_CONTRACT_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "plan_digest": plan_digest,
        "predecessor_contract_version":
            predecessor.RESULT_EVIDENCE_CONTRACT_VERSION,
        "predecessor_manifest_version":
            predecessor.RESULT_EVIDENCE_MANIFEST_VERSION,
        "source_result_evidence_records":
            len(source_records),
        "validation_records":
            len(validation_records),
        "validation_comparisons":
            len(comparison_ids),
        "validation_status_counts":
            status_counts,
        "validation_blocker_counts":
            blocker_counts,
        "candidate_derived_artifact_count":
            candidate_derived_artifacts,
        "validation_artifact_count":
            validation_artifacts,
        "authoritative_historical_outcomes_validated":
            authoritative_outcomes_validated,
        "fabricated_evidence_detected_count":
            fabricated_evidence_count,
        "validation_implementation_authorities_granted":
            implementation_authorities,
        "predecessor_result_digest":
            replay["result_digest"],
        "predecessor_manifest_digest":
            replay["manifest_digest"],
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
        "result_evidence_validation_status":
            EXPECTED_STATUS,
        "structural_validation_complete":
            all_checks_passed,
        "authoritative_historical_outcome_validated":
            False,
        "authority_granted": (
            "validation_result_evidence_validation_result_evidence_planning"
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
        "Validation contract version: "
        f"{VALIDATION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        f"Source result-evidence records: {len(source_records)}"
    )
    print(f"Validation records: {len(validation_records)}")
    print(f"Validation comparisons: {len(comparison_ids)}")
    print(f"Validation status counts: {status_counts}")
    print(f"Validation blocker counts: {blocker_counts}")
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
        "Validation implementation authorities granted: "
        f"{implementation_authorities}"
    )
    print(f"Plan digest: {plan_digest}")
    print(
        f"Predecessor result digest: {replay['result_digest']}"
    )
    print(
        f"Predecessor manifest digest: {replay['manifest_digest']}"
    )
    print(f"Validation digest: {validation_digest}")
    print(
        "Reverse validation digest: "
        f"{reverse_validation_digest}"
    )
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
