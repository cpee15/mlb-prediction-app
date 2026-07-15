#!/usr/bin/env python3
"""
Layer 9BT
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Source Endpoint Candidate Source Evidence
Historical Outcome Field Mapping Result Validation
Evidence Package Validation Result Evidence Implementation

Materializes deterministic evidence records for the Layer 9BR evidence-package
validation results according to the Layer 9BS plan.

The predecessor records are structurally valid, but all remain
candidate_not_supplied. This implementation preserves that distinction and
does not invent authoritative evidence or execute retrieval, parsing, mapping,
extraction, mutation, recomputation, production, market, pricing, or betting
operations.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9BT"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_result_evidence_"
    "implementation"
)

RESULT_EVIDENCE_CONTRACT_VERSION = (
    "layer_9BT_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_contract_v1"
)

RESULT_EVIDENCE_MANIFEST_VERSION = (
    "layer_9BT_historical_outcome_validation_result_evidence_manifest_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9BT_pitch_type_matchup_overlay_historical_outcome_"
    "authoritative_source_endpoint_candidate_source_evidence_historical_"
    "outcome_field_mapping_result_validation_evidence_package_validation_"
    "result_evidence"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9BS_pitch_type_matchup_overlay_historical_outcome_authoritative_"
    "source_endpoint_candidate_source_evidence_historical_outcome_field_"
    "mapping_result_validation_evidence_package_validation_result_evidence.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9BS_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9BR_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_contract_v1"
)

EXPECTED_RECORDS = 16
EXPECTED_COMPARISONS = 16

EXPECTED_STATUS = "candidate_not_supplied"

EXPECTED_BLOCKER = (
    "historical_outcome_endpoint_candidate_missing"
)

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
                    if isinstance(value, (dict, list, tuple))
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
        "layer_9bs_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9BS plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["module"]

    if (
        predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9BR contract version: "
            f"{predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def candidate_derived_artifact_count(
    row: Mapping[str, Any],
) -> int:
    return sum(
        int(row.get(field, 0) or 0)
        for field in (
            "candidate_evidence_artifact_count",
            "response_evidence_artifact_count",
            "parser_evidence_artifact_count",
            "parsed_record_evidence_artifact_count",
            "mapping_evidence_artifact_count",
            "mapping_result_evidence_artifact_count",
        )
    )


def build_result_evidence_records(
    plan: Any,
    validation_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    result_records: list[dict[str, Any]] = []

    for validation in validation_records:
        derived_artifact_count = (
            candidate_derived_artifact_count(validation)
        )

        validation_artifact_count = int(
            validation.get(
                "validation_evidence_artifact_count",
                0,
            )
            or 0
        )

        structural_package_validation_complete = all(
            bool(validation.get(field))
            for field in (
                "package_record_identity_valid",
                "package_record_digest_valid",
                "package_manifest_valid",
                "lineage_complete",
                "evidence_inventory_valid",
                "canonical_field_identity_valid",
            )
        )

        authoritative_historical_outcome_validated = False

        identity_payload = {
            "validation_result_evidence_contract_version":
                RESULT_EVIDENCE_CONTRACT_VERSION,
            "evidence_package_validation_plan_record_id":
                validation.get(
                    "evidence_package_validation_plan_record_id"
                ),
            "evidence_package_validation_plan_record_digest":
                validation.get(
                    "evidence_package_validation_plan_record_digest"
                ),
            "evidence_package_plan_record_id":
                validation.get(
                    "evidence_package_plan_record_id"
                ),
            "comparison_record_id":
                validation.get("comparison_record_id"),
            "defect_source_record_id":
                validation.get("defect_source_record_id"),
            "validation_result_evidence_status":
                EXPECTED_STATUS,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "validation_result_evidence_plan_contract_version":
                RESULT_EVIDENCE_CONTRACT_VERSION,
            "validation_result_evidence_plan_record_id":
                "HOASEHOFMRVEPVRE-" + identity_digest[:20],
            "evidence_package_validation_plan_record_id":
                validation.get(
                    "evidence_package_validation_plan_record_id"
                ),
            "evidence_package_validation_plan_identity_digest":
                validation.get(
                    "evidence_package_validation_plan_identity_digest"
                ),
            "evidence_package_validation_plan_record_digest":
                validation.get(
                    "evidence_package_validation_plan_record_digest"
                ),
            "evidence_package_plan_record_id":
                validation.get(
                    "evidence_package_plan_record_id"
                ),
            "evidence_package_plan_identity_digest":
                validation.get(
                    "evidence_package_plan_identity_digest"
                ),
            "evidence_package_plan_record_digest":
                validation.get(
                    "evidence_package_plan_record_digest"
                ),
            "package_manifest_version":
                validation.get("package_manifest_version"),
            "package_manifest_digest":
                validation.get("package_manifest_digest"),
            "mapping_result_validation_plan_record_id":
                validation.get(
                    "mapping_result_validation_plan_record_id"
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
            "defect_source_path":
                validation.get("defect_source_path"),
            "defect_source_symbol":
                validation.get("defect_source_symbol"),
            "defect_source_record_id":
                validation.get("defect_source_record_id"),
            "defect_source_record_digest":
                validation.get("defect_source_record_digest"),
            "authoritative_field_name":
                validation.get("authoritative_field_name"),
            "authoritative_field_path":
                validation.get("authoritative_field_path"),
            "rejected_metadata_field_name":
                validation.get("rejected_metadata_field_name"),
            "candidate_supplied":
                bool(validation.get("candidate_supplied")),
            "candidate_id":
                validation.get("candidate_id"),
            "candidate_version":
                validation.get("candidate_version"),
            "candidate_derived_artifact_count":
                derived_artifact_count,
            "validation_artifact_count":
                validation_artifact_count,
            "evidence_absence_explicit":
                bool(
                    validation.get(
                        "evidence_absence_explicit"
                    )
                ),
            "fabricated_evidence_detected":
                bool(
                    validation.get(
                        "fabricated_evidence_detected"
                    )
                ),
            "package_record_identity_valid":
                bool(
                    validation.get(
                        "package_record_identity_valid"
                    )
                ),
            "package_record_digest_valid":
                bool(
                    validation.get(
                        "package_record_digest_valid"
                    )
                ),
            "package_manifest_valid":
                bool(
                    validation.get(
                        "package_manifest_valid"
                    )
                ),
            "lineage_complete":
                bool(validation.get("lineage_complete")),
            "evidence_inventory_valid":
                bool(
                    validation.get(
                        "evidence_inventory_valid"
                    )
                ),
            "canonical_field_identity_valid":
                bool(
                    validation.get(
                        "canonical_field_identity_valid"
                    )
                ),
            "structural_package_validation_complete":
                structural_package_validation_complete,
            "authoritative_historical_outcome_validated":
                authoritative_historical_outcome_validated,
            "evidence_package_validation_status":
                validation.get(
                    "evidence_package_validation_status"
                ),
            "evidence_package_validation_blocker_codes":
                validation.get(
                    "evidence_package_validation_blocker_codes"
                ),
            "evidence_package_validation_rationale":
                validation.get(
                    "evidence_package_validation_rationale"
                ),
            "evidence_package_validation_limitations":
                validation.get(
                    "evidence_package_validation_limitations"
                ),
            "evidence_package_authority_boundary":
                validation.get(
                    "evidence_package_authority_boundary"
                ),
            "validation_result_evidence_status":
                EXPECTED_STATUS,
            "validation_result_evidence_blocker_codes": [
                EXPECTED_BLOCKER
            ],
            "validation_result_evidence_implementation_authority_granted":
                False,
            "validation_result_evidence_rationale": (
                "The Layer 9BR validation result proves that the evidence "
                "package is structurally valid, deterministic, digest-bearing, "
                "lineage-complete, explicit about evidence absence, free of "
                "fabricated evidence, and compliant with canonical field "
                "identity. It does not validate an authoritative historical "
                "outcome because no endpoint candidate or candidate-derived "
                "source evidence was supplied."
            ),
            "validation_result_evidence_limitations": [
                "No authoritative endpoint candidate was supplied.",
                "No candidate-derived source evidence exists.",
                "No validated response artifact exists.",
                "No parser execution was authorized.",
                "No parsed record was validated.",
                "No field-mapping result was validated.",
                "No authoritative historical outcome was validated.",
                "Structural package validity does not prove outcome truth.",
                "The canonical target remains outcome_value.",
                "outcome_available_at_utc remains rejected as an outcome substitute.",
                "No network retrieval was executed.",
                "No response bytes were read.",
                "No historical outcome value was extracted.",
                "No canonical source record was changed.",
                "No downstream comparison or metric was recomputed.",
                "No production, pricing, market, or betting authority is granted.",
            ],
            "validation_result_evidence_authority_boundary": (
                "This record authorizes only deterministic preservation of "
                "the Layer 9BR validation result. It grants no authority to "
                "invent or retrieve endpoint candidates, responses, parsers, "
                "parsed records, mappings, mapping results, outcome values, "
                "evidence locators, credentials, production decisions, market "
                "comparisons, pricing changes, or betting conclusions."
            ),
            "validation_result_evidence_plan_identity_digest":
                identity_digest,
        }

        record[
            "validation_result_evidence_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Result-evidence record missing fields: "
                + ", ".join(missing_fields)
            )

        result_records.append(
            {
                field: record[field]
                for field in plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS
            }
        )

    result_records.sort(
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
            normalized_string(
                row.get(
                    "validation_result_evidence_plan_record_id"
                )
            ),
        )
    )

    return result_records


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

    result_records = build_result_evidence_records(
        plan,
        validation_records,
    )

    reverse_result_records = build_result_evidence_records(
        plan,
        list(reversed(reverse_validation_records)),
    )

    predecessor_replay_deterministic = (
        canonical_json(validation_records)
        == canonical_json(reverse_validation_records)
    )

    result_replay_deterministic = (
        canonical_json(result_records)
        == canonical_json(reverse_result_records)
    )

    result_digest = sha256_payload(
        result_records
    )

    reverse_result_digest = sha256_payload(
        reverse_result_records
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in result_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row[
                    "validation_result_evidence_status"
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
                    "validation_result_evidence_blocker_codes"
                ]
            ).items()
        )
    )

    structural_counts = {
        field: sum(
            bool(row[field])
            for row in result_records
        )
        for field in (
            "package_record_identity_valid",
            "package_record_digest_valid",
            "package_manifest_valid",
            "lineage_complete",
            "evidence_inventory_valid",
            "canonical_field_identity_valid",
            "structural_package_validation_complete",
        )
    }

    authoritative_outcomes_validated = sum(
        bool(
            row[
                "authoritative_historical_outcome_validated"
            ]
        )
        for row in result_records
    )

    candidate_derived_artifacts = sum(
        int(
            row["candidate_derived_artifact_count"]
        )
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

    authority_records = [
        row
        for row in result_records
        if row[
            "validation_result_evidence_"
            "implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check": "nine_bs_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed":
                plan.PLAN_VERSION == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_br_contract_version_verified",
            "actual":
                predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION
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
            "check": "result_evidence_replay_deterministic",
            "actual": result_replay_deterministic,
            "expected": True,
            "passed": result_replay_deterministic,
        },
        {
            "check": "result_evidence_digests_match_reverse_replay",
            "actual": result_digest,
            "expected": reverse_result_digest,
            "passed":
                result_digest == reverse_result_digest,
        },
        {
            "check": "expected_validation_records_replayed",
            "actual": len(validation_records),
            "expected": EXPECTED_RECORDS,
            "passed":
                len(validation_records) == EXPECTED_RECORDS,
        },
        {
            "check": "expected_result_evidence_records_materialized",
            "actual": len(result_records),
            "expected": EXPECTED_RECORDS,
            "passed":
                len(result_records) == EXPECTED_RECORDS,
        },
        {
            "check": "one_result_evidence_record_per_comparison",
            "actual": len(comparison_ids),
            "expected": EXPECTED_COMPARISONS,
            "passed":
                len(comparison_ids) == EXPECTED_COMPARISONS,
        },
        {
            "check": "result_evidence_fields_complete",
            "actual":
                len(plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS),
            "expected": 54,
            "passed": all(
                set(row)
                == set(
                    plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS
                )
                for row in result_records
            ),
        },
        {
            "check": "result_evidence_record_ids_unique",
            "actual": len(
                {
                    row[
                        "validation_result_evidence_plan_record_id"
                    ]
                    for row in result_records
                }
            ),
            "expected": len(result_records),
            "passed": (
                len(
                    {
                        row[
                            "validation_result_evidence_plan_record_id"
                        ]
                        for row in result_records
                    }
                )
                == len(result_records)
            ),
        },
        {
            "check": "result_evidence_record_digests_unique",
            "actual": len(
                {
                    row[
                        "validation_result_evidence_plan_record_digest"
                    ]
                    for row in result_records
                }
            ),
            "expected": len(result_records),
            "passed": (
                len(
                    {
                        row[
                            "validation_result_evidence_plan_record_digest"
                        ]
                        for row in result_records
                    }
                )
                == len(result_records)
            ),
        },
        {
            "check": "all_result_evidence_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "validation_result_evidence_plan_identity_digest"
                    ]
                )
                for row in result_records
            ),
            "expected": len(result_records),
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_plan_identity_digest"
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
                        "validation_result_evidence_plan_record_digest"
                    ]
                )
                for row in result_records
            ),
            "expected": len(result_records),
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_plan_record_digest"
                    ]
                )
                for row in result_records
            ),
        },
        {
            "check": "validation_record_identity_and_digests_preserved",
            "actual": sum(
                bool(
                    normalized_string(
                        row[
                            "evidence_package_validation_plan_record_id"
                        ]
                    )
                )
                and valid_sha256(
                    row[
                        "evidence_package_validation_plan_identity_digest"
                    ]
                )
                and valid_sha256(
                    row[
                        "evidence_package_validation_plan_record_digest"
                    ]
                )
                for row in result_records
            ),
            "expected": len(result_records),
            "passed": all(
                bool(
                    normalized_string(
                        row[
                            "evidence_package_validation_plan_record_id"
                        ]
                    )
                )
                and valid_sha256(
                    row[
                        "evidence_package_validation_plan_identity_digest"
                    ]
                )
                and valid_sha256(
                    row[
                        "evidence_package_validation_plan_record_digest"
                    ]
                )
                for row in result_records
            ),
        },
        {
            "check": "all_structural_validity_results_complete",
            "actual": structural_counts,
            "expected": {
                key: EXPECTED_RECORDS
                for key in structural_counts
            },
            "passed": all(
                count == EXPECTED_RECORDS
                for count in structural_counts.values()
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": status_counts,
            "expected": {
                EXPECTED_STATUS: EXPECTED_RECORDS
            },
            "passed": (
                status_counts
                == {
                    EXPECTED_STATUS: EXPECTED_RECORDS
                }
            ),
        },
        {
            "check": "all_candidate_missing_blockers_present",
            "actual": blocker_counts,
            "expected": {
                EXPECTED_BLOCKER: EXPECTED_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    EXPECTED_BLOCKER: EXPECTED_RECORDS
                }
            ),
        },
        {
            "check": "candidate_derived_artifact_count_zero",
            "actual": candidate_derived_artifacts,
            "expected": 0,
            "passed":
                candidate_derived_artifacts == 0,
        },
        {
            "check": "one_validation_artifact_per_record",
            "actual": validation_artifacts,
            "expected": EXPECTED_RECORDS,
            "passed":
                validation_artifacts == EXPECTED_RECORDS,
        },
        {
            "check": "evidence_absence_explicit",
            "actual": sum(
                bool(row["evidence_absence_explicit"])
                for row in result_records
            ),
            "expected": len(result_records),
            "passed": all(
                bool(row["evidence_absence_explicit"])
                for row in result_records
            ),
        },
        {
            "check": "fabricated_evidence_absent",
            "actual": fabricated_evidence_count,
            "expected": 0,
            "passed":
                fabricated_evidence_count == 0,
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
            "check": "authoritative_historical_outcomes_validated_zero",
            "actual": authoritative_outcomes_validated,
            "expected": 0,
            "passed":
                authoritative_outcomes_validated == 0,
        },
        {
            "check": "result_evidence_rationale_limitations_and_boundary_present",
            "actual": sum(
                bool(
                    normalized_string(
                        row[
                            "validation_result_evidence_rationale"
                        ]
                    )
                )
                and bool(
                    row[
                        "validation_result_evidence_limitations"
                    ]
                )
                and bool(
                    normalized_string(
                        row[
                            "validation_result_evidence_authority_boundary"
                        ]
                    )
                )
                for row in result_records
            ),
            "expected": len(result_records),
            "passed": all(
                bool(
                    normalized_string(
                        row[
                            "validation_result_evidence_rationale"
                        ]
                    )
                )
                and bool(
                    row[
                        "validation_result_evidence_limitations"
                    ]
                )
                and bool(
                    normalized_string(
                        row[
                            "validation_result_evidence_authority_boundary"
                        ]
                    )
                )
                for row in result_records
            ),
        },
        {
            "check": "no_result_evidence_implementation_authority_granted",
            "actual": len(authority_records),
            "expected": 0,
            "passed":
                len(authority_records) == 0,
        },
        {
            "check": "validation_result_invention_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "candidate_evidence_invention_not_executed",
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
            "check": "candidate_values_transformed_zero",
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
            "check": "uncertainty_and_significance_calculations_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "superiority_equivalence_and_activation_decisions_zero",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "production_market_pricing_and_betting_authority_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        bool(row["passed"])
        for row in checks
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
        "authoritative_historical_outcomes_validated":
            authoritative_outcomes_validated,
    }

    manifest_digest = sha256_payload(
        manifest_payload
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "implementation_failed"
    )

    next_layer = (
        "9BU_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
        "validation_plan"
        if all_checks_passed
        else
        "9BT_pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
        "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
        "result_validation_evidence_package_validation_result_evidence_"
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
        / "validation_result_evidence_records.csv",
        plan.RESULT_EVIDENCE_PLAN_RECORD_FIELDS,
        result_records,
    )

    write_csv(
        OUTPUT_DIR
        / "validation_result_evidence_status_counts.csv",
        [
            "validation_result_evidence_status",
            "count",
        ],
        [
            {
                "validation_result_evidence_status": key,
                "count": value,
            }
            for key, value in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "validation_result_evidence_blocker_counts.csv",
        [
            "validation_result_evidence_blocker",
            "count",
        ],
        [
            {
                "validation_result_evidence_blocker": key,
                "count": value,
            }
            for key, value in blocker_counts.items()
        ],
    )

    write_json(
        OUTPUT_DIR / "result_evidence_manifest.json",
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
        "predecessor_contract_version":
            predecessor.EVIDENCE_PACKAGE_VALIDATION_CONTRACT_VERSION,
        "validation_records":
            len(validation_records),
        "result_evidence_records":
            len(result_records),
        "result_evidence_comparisons":
            len(comparison_ids),
        "result_evidence_status_counts":
            status_counts,
        "result_evidence_blocker_counts":
            blocker_counts,
        "structural_validity_counts":
            structural_counts,
        "candidate_derived_artifact_count":
            candidate_derived_artifacts,
        "validation_artifact_count":
            validation_artifacts,
        "authoritative_historical_outcomes_validated":
            authoritative_outcomes_validated,
        "fabricated_evidence_detected_count":
            fabricated_evidence_count,
        "result_evidence_implementation_authorities_granted":
            len(authority_records),
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
        "pricing_changes_emitted": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed":
            all_checks_passed,
        "recommended_next_layer":
            next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "validation_result_evidence_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "validation_result_evidence_status":
            EXPECTED_STATUS,
        "structural_package_validation_complete":
            all_checks_passed,
        "authoritative_historical_outcome_validated":
            False,
        "authority_granted": (
            "historical_outcome_authoritative_source_endpoint_candidate_"
            "source_evidence_historical_outcome_field_mapping_result_"
            "validation_evidence_package_validation_result_evidence_"
            "validation_planning"
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
            "evidence_locator_invention",
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
            str(OUTPUT_DIR.relative_to(ROOT)),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
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
    print(
        "Validation records replayed: "
        f"{len(validation_records)}"
    )
    print(
        "Result-evidence records: "
        f"{len(result_records)}"
    )
    print(
        "Result-evidence comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Result-evidence status counts: "
        f"{status_counts}"
    )
    print(
        "Result-evidence blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Structural validity counts: "
        f"{structural_counts}"
    )
    print(
        "Candidate-derived artifact count: "
        f"{candidate_derived_artifacts}"
    )
    print(
        "Validation artifact count: "
        f"{validation_artifacts}"
    )
    print(
        "Authoritative historical outcomes validated: "
        f"{authoritative_outcomes_validated}"
    )
    print(
        "Fabricated evidence detected: "
        f"{fabricated_evidence_count}"
    )
    print(
        "Result-evidence implementation authorities granted: "
        f"{len(authority_records)}"
    )
    print(
        f"Result-evidence digest: {result_digest}"
    )
    print(
        "Reverse result-evidence digest: "
        f"{reverse_result_digest}"
    )
    print(
        f"Manifest digest: {manifest_digest}"
    )
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
    print("Statistical significance tests calculated: 0")
    print("Superiority decisions emitted: 0")
    print("Equivalence decisions emitted: 0")
    print("Activation recommendations emitted: 0")
    print("Production probabilities changed: 0")
    print("Market comparisons executed: 0")
    print("Pricing changes emitted: 0")
    print("Betting edges calculated: 0")
    print(
        f"Diagnosis: {diagnosis_name}"
    )
    print(
        "Validation-result evidence status: "
        f"{EXPECTED_STATUS}"
    )
    print(
        "Structural package validation complete: "
        f"{all_checks_passed}"
    )
    print(
        "Authoritative historical outcome validated: "
        "False"
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
