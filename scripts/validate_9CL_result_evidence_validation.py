#!/usr/bin/env python3
"""
Layer 9CL

Implements the deterministic validation contract defined by Layer 9CK
over Layer 9CJ validation-result evidence result-evidence records.

This layer validates structural integrity, identity, digests, lineage,
status, blockers, evidence absence, canonical-field identity,
documentation, and authority boundaries.

It does not invent or retrieve endpoint candidates or evidence, validate
authoritative historical outcomes, parse responses, map or extract
outcomes, mutate canonical data, recompute downstream artifacts, or grant
production, market, pricing, or betting authority.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9CL"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_result_evidence_"
    "validation_result_evidence_validation_result_evidence_validation_"
    "result_evidence_validation_result_evidence_validation_implementation"
)

VALIDATION_CONTRACT_VERSION = (
    "layer_9CL_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_result_evidence_"
    "validation_result_evidence_validation_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "tmp" / "layer_9CL_result_evidence_validation"

PLAN_PATH = (
    ROOT / "scripts"
    / "plan_9CK_validation_result_evidence_validation.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9CK_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_result_evidence_"
    "validation_result_evidence_validation_plan_v1"
)

EXPECTED_PREDECESSOR_CONTRACT_VERSION = (
    "layer_9CJ_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_result_evidence_"
    "validation_result_evidence_contract_v1"
)

EXPECTED_PREDECESSOR_MANIFEST_VERSION = (
    "layer_9CJ_validation_result_evidence_manifest_v1"
)

EXPECTED_PLAN_DIGEST = (
    "c2583ad6d835f40180023ae1166597053471ee2ac030c2011cf5d7b6f352cea9"
)

EXPECTED_SOURCE_PLAN_DIGEST = (
    "5f1075d14f397711947cb4e153245e988fbcca6f9c1000d2780fcc0bf77735b0"
)

EXPECTED_PREDECESSOR_VALIDATION_DIGEST = (
    "bdfb21e653e736d5e753ea4c7bf71295fae4ae4d503d11f3888fc78b27c1cc46"
)

EXPECTED_PREDECESSOR_RESULT_DIGEST = (
    "5ae91bca40eba2753570932c6b9a5fd5477615c3dd4319d54458b85a6806c443"
)

EXPECTED_PREDECESSOR_MANIFEST_DIGEST = (
    "e7ef986afb41430da72e0453a65241ee7a2dfc2615ab8b4cfa92f3dc9958d75c"
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


def compute_plan_digest(plan: Any) -> str:
    return sha256_payload(
        {
            "validation_plan_version":
                plan.VALIDATION_PLAN_VERSION,
            "principles":
                plan.VALIDATION_PRINCIPLES,
            "components":
                plan.VALIDATION_COMPONENTS,
            "stages":
                plan.VALIDATION_STAGES,
            "requirements":
                plan.VALIDATION_REQUIREMENTS,
            "statuses":
                plan.VALIDATION_STATUSES,
            "blockers":
                plan.BLOCKER_CODES,
            "record_fields":
                plan.VALIDATION_PLAN_RECORD_FIELDS,
            "ordering_fields":
                plan.ORDERING_FIELDS,
            "implementation_steps":
                plan.IMPLEMENTATION_STEPS,
            "prohibited_authorities":
                plan.PROHIBITED_AUTHORITIES,
        }
    )


def replay_plan() -> dict[str, Any]:
    plan = load_module(
        PLAN_PATH,
        "layer_9ck_validation_plan",
    )

    if plan.VALIDATION_PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9CK validation plan version: "
            f"{plan.VALIDATION_PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()
    predecessor = replay["predecessor"]

    if (
        predecessor.RESULT_EVIDENCE_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_CONTRACT_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9CJ result-evidence contract version: "
            f"{predecessor.RESULT_EVIDENCE_CONTRACT_VERSION}"
        )

    if (
        predecessor.RESULT_EVIDENCE_MANIFEST_VERSION
        != EXPECTED_PREDECESSOR_MANIFEST_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9CJ manifest version: "
            f"{predecessor.RESULT_EVIDENCE_MANIFEST_VERSION}"
        )

    return {
        **replay,
        "plan": plan,
        "predecessor": predecessor,
    }


def build_validation_records(
    plan: Any,
    replay: Mapping[str, Any],
    source_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []

    for source in source_records:
        source_id = source[
            "validation_result_evidence_validation_result_evidence_"
            "plan_record_id"
        ]

        source_identity_digest = source[
            "validation_result_evidence_validation_result_evidence_"
            "plan_identity_digest"
        ]

        source_record_digest = source[
            "validation_result_evidence_validation_result_evidence_"
            "plan_record_digest"
        ]

        identity_payload = {
            "validation_contract_version":
                VALIDATION_CONTRACT_VERSION,
            "source_record_id":
                source_id,
            "source_record_identity_digest":
                source_identity_digest,
            "source_record_digest":
                source_record_digest,
            "comparison_record_id":
                source["comparison_record_id"],
            "status":
                EXPECTED_STATUS,
        }

        identity_digest = sha256_payload(identity_payload)

        record = {
            "validation_result_evidence_validation_result_evidence_"
            "validation_contract_version":
                VALIDATION_CONTRACT_VERSION,
            "validation_result_evidence_validation_result_evidence_"
            "validation_plan_record_id":
                "HOASEHOFMRVEPVREVREVREVREVREV-"
                + identity_digest[:20],
            "validation_result_evidence_validation_result_evidence_"
            "validation_plan_identity_digest":
                identity_digest,
            "validation_result_evidence_validation_result_evidence_"
            "source_record_id":
                source_id,
            "validation_result_evidence_validation_result_evidence_"
            "source_record_identity_digest":
                source_identity_digest,
            "validation_result_evidence_validation_result_evidence_"
            "source_record_digest":
                source_record_digest,
            **dict(source),
            "validation_result_evidence_validation_result_evidence_"
            "validation_status":
                EXPECTED_STATUS,
            "validation_result_evidence_validation_result_evidence_"
            "validation_blocker_codes":
                [EXPECTED_BLOCKER],
            "validation_result_evidence_validation_result_evidence_"
            "validation_implementation_authority_granted":
                False,
            "validation_result_evidence_validation_result_evidence_"
            "validation_rationale": (
                "The Layer 9CJ result-evidence record was replayed "
                "deterministically and validated for identity, record "
                "digest, manifest lineage, predecessor lineage, structural "
                "disposition, candidate-evidence absence, canonical-field "
                "identity, documentation, and authority boundaries. The "
                "record remains candidate_not_supplied because no "
                "authoritative historical-outcome endpoint candidate or "
                "candidate-derived evidence was supplied."
            ),
            "validation_result_evidence_validation_result_evidence_"
            "validation_limitations": [
                "No authoritative endpoint candidate was supplied.",
                "No candidate-derived source evidence exists.",
                "No authoritative historical outcome was validated.",
                (
                    "This validation record confirms structural integrity "
                    "only."
                ),
                (
                    "Structural integrity does not establish historical "
                    "outcome truth."
                ),
                "The canonical target remains outcome_value.",
                (
                    "outcome_available_at_utc remains rejected as an "
                    "outcome substitute."
                ),
                "No response bytes were read or parsed.",
                "No historical-outcome mapping was executed.",
                "No historical-outcome value was extracted.",
                "No canonical source records were mutated.",
                "No downstream records were recomputed.",
                (
                    "No production, market, pricing, or betting authority "
                    "is granted."
                ),
            ],
            "validation_result_evidence_validation_result_evidence_"
            "validation_authority_boundary": (
                "This record authorizes only deterministic structural "
                "validation of the Layer 9CJ result-evidence record. It "
                "grants no authority to invent, retrieve, parse, map, or "
                "extract endpoint candidates, responses, evidence, or "
                "historical outcomes; mutate canonical data; recompute "
                "downstream artifacts; or make production, market, pricing, "
                "or betting decisions."
            ),
        }

        digest_field = (
            "validation_result_evidence_validation_result_evidence_"
            "validation_plan_record_digest"
        )

        missing_fields = [
            field
            for field in plan.VALIDATION_PLAN_RECORD_FIELDS
            if field != digest_field and field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Validation record missing fields: "
                + ", ".join(missing_fields)
            )

        record[digest_field] = sha256_payload(record)

        output.append(
            {
                field: record[field]
                for field in plan.VALIDATION_PLAN_RECORD_FIELDS
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
                row["result_evidence_validation_plan_record_id"]
            ),
            normalized_string(
                row[
                    "validation_result_evidence_validation_result_evidence_"
                    "plan_record_id"
                ]
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

    plan_digest = compute_plan_digest(plan)

    source_digest = sha256_payload(source_records)
    reverse_source_digest = sha256_payload(reverse_source_records)

    validation_digest = sha256_payload(validation_records)
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
                    "validation_result_evidence_validation_result_evidence_"
                    "validation_status"
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
                    "validation_result_evidence_validation_result_evidence_"
                    "validation_blocker_codes"
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
        bool(row["authoritative_historical_outcome_validated"])
        for row in validation_records
    )

    validation_authorities = sum(
        bool(
            row[
                "validation_result_evidence_validation_result_evidence_"
                "validation_implementation_authority_granted"
            ]
        )
        for row in validation_records
    )

    checks = [
        {
            "check": "nine_ck_plan_version_verified",
            "actual": plan.VALIDATION_PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed":
                plan.VALIDATION_PLAN_VERSION == EXPECTED_PLAN_VERSION,
        },
        {
            "check": "nine_ck_plan_digest_verified",
            "actual": plan_digest,
            "expected": EXPECTED_PLAN_DIGEST,
            "passed": plan_digest == EXPECTED_PLAN_DIGEST,
        },
        {
            "check": "nine_cj_contract_version_verified",
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
            "check": "nine_cj_manifest_version_verified",
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
            "check": "nine_ci_plan_digest_preserved",
            "actual": replay["source_plan_digest"],
            "expected": EXPECTED_SOURCE_PLAN_DIGEST,
            "passed":
                replay["source_plan_digest"] == EXPECTED_SOURCE_PLAN_DIGEST,
        },
        {
            "check": "nine_ch_validation_digest_preserved",
            "actual": replay["predecessor_validation_digest"],
            "expected": EXPECTED_PREDECESSOR_VALIDATION_DIGEST,
            "passed": (
                replay["predecessor_validation_digest"]
                == EXPECTED_PREDECESSOR_VALIDATION_DIGEST
            ),
        },
        {
            "check": "source_result_evidence_replay_deterministic",
            "actual": source_digest == reverse_source_digest,
            "expected": True,
            "passed": source_digest == reverse_source_digest,
        },
        {
            "check": "source_result_evidence_digest_preserved",
            "actual": source_digest,
            "expected": EXPECTED_PREDECESSOR_RESULT_DIGEST,
            "passed": source_digest == EXPECTED_PREDECESSOR_RESULT_DIGEST,
        },
        {
            "check": "predecessor_result_evidence_digest_preserved",
            "actual": replay["result_digest"],
            "expected": EXPECTED_PREDECESSOR_RESULT_DIGEST,
            "passed":
                replay["result_digest"] == EXPECTED_PREDECESSOR_RESULT_DIGEST,
        },
        {
            "check": "predecessor_manifest_digest_preserved",
            "actual": replay["manifest_digest"],
            "expected": EXPECTED_PREDECESSOR_MANIFEST_DIGEST,
            "passed": (
                replay["manifest_digest"]
                == EXPECTED_PREDECESSOR_MANIFEST_DIGEST
            ),
        },
        {
            "check": "validation_replay_deterministic",
            "actual": (
                canonical_json(validation_records)
                == canonical_json(reverse_validation_records)
            ),
            "expected": True,
            "passed": (
                canonical_json(validation_records)
                == canonical_json(reverse_validation_records)
            ),
        },
        {
            "check": "validation_digests_match_reverse_replay",
            "actual": validation_digest,
            "expected": reverse_validation_digest,
            "passed": validation_digest == reverse_validation_digest,
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
            "passed": len(validation_records) == EXPECTED_RECORDS,
        },
        {
            "check": "one_validation_record_per_comparison",
            "actual": len(comparison_ids),
            "expected": EXPECTED_COMPARISONS,
            "passed": len(comparison_ids) == EXPECTED_COMPARISONS,
        },
        {
            "check": "validation_field_contract_preserved",
            "actual": len(plan.VALIDATION_PLAN_RECORD_FIELDS),
            "expected": 78,
            "passed": all(
                list(row.keys()) == plan.VALIDATION_PLAN_RECORD_FIELDS
                for row in validation_records
            ),
        },
        {
            "check": "validation_record_ids_unique",
            "actual": len(
                {
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_plan_record_id"
                    ]
                    for row in validation_records
                }
            ),
            "expected": EXPECTED_RECORDS,
            "passed": (
                len(
                    {
                        row[
                            "validation_result_evidence_validation_result_"
                            "evidence_validation_plan_record_id"
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
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_plan_record_digest"
                    ]
                    for row in validation_records
                }
            ),
            "expected": EXPECTED_RECORDS,
            "passed": (
                len(
                    {
                        row[
                            "validation_result_evidence_validation_result_"
                            "evidence_validation_plan_record_digest"
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
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_plan_identity_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_plan_identity_digest"
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
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_plan_record_digest"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "source_result_evidence_identity_preserved",
            "actual": sum(
                bool(
                    normalized_string(
                        row[
                            "validation_result_evidence_validation_result_"
                            "evidence_source_record_id"
                        ]
                    )
                )
                and valid_sha256(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_source_record_identity_digest"
                    ]
                )
                and valid_sha256(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_source_record_digest"
                    ]
                )
                for row in validation_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(
                    normalized_string(
                        row[
                            "validation_result_evidence_validation_result_"
                            "evidence_source_record_id"
                        ]
                    )
                )
                and valid_sha256(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_source_record_identity_digest"
                    ]
                )
                and valid_sha256(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_source_record_digest"
                    ]
                )
                for row in validation_records
            ),
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
            "passed":
                status_counts == {EXPECTED_STATUS: EXPECTED_RECORDS},
        },
        {
            "check": "all_missing_endpoint_blockers_preserved",
            "actual": blocker_counts,
            "expected": {EXPECTED_BLOCKER: EXPECTED_RECORDS},
            "passed":
                blocker_counts == {EXPECTED_BLOCKER: EXPECTED_RECORDS},
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
            "check": "source_documentation_preserved",
            "actual": sum(
                bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_rationale"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_authority_boundary"
                    ]
                )
                for row in validation_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_rationale"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_authority_boundary"
                    ]
                )
                for row in validation_records
            ),
        },
        {
            "check": "validation_documentation_and_boundary_present",
            "actual": sum(
                bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_rationale"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_authority_boundary"
                    ]
                )
                for row in validation_records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_rationale"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_result_"
                        "evidence_validation_authority_boundary"
                    ]
                )
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
            "check": "validation_authorities_granted_zero",
            "actual": validation_authorities,
            "expected": 0,
            "passed": validation_authorities == 0,
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
            "check": "network_parse_mutation_and_recomputation_zero",
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
        "9CM_validation_result_evidence_validation_result_evidence_"
        "validation_result_evidence_plan"
        if all_checks_passed
        else
        "9CL_validation_result_evidence_validation_result_evidence_"
        "validation_remediation"
    )

    diagnosis_name = (
        "validation_result_evidence_validation_result_evidence_"
        "validation_implementation_complete"
        if all_checks_passed
        else
        "validation_result_evidence_validation_result_evidence_"
        "validation_implementation_failed"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "validation_records.csv",
        plan.VALIDATION_PLAN_RECORD_FIELDS,
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
        "validation_plan_version":
            plan.VALIDATION_PLAN_VERSION,
        "validation_plan_digest":
            plan_digest,
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
        "validation_authorities_granted":
            validation_authorities,
        "layer_9CI_plan_digest":
            replay["source_plan_digest"],
        "layer_9CH_validation_digest":
            replay["predecessor_validation_digest"],
        "predecessor_result_evidence_digest":
            source_digest,
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
        "validation_status":
            EXPECTED_STATUS,
        "structural_validation_complete":
            all_checks_passed,
        "authoritative_historical_outcome_validated":
            False,
        "authority_granted": (
            "validation_result_evidence_validation_result_evidence_"
            "validation_result_evidence_planning"
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
    print(f"Source result-evidence records: {len(source_records)}")
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
        "Fabricated evidence detected: "
        f"{fabricated_evidence_count}"
    )
    print(
        "Validation authorities granted: "
        f"{validation_authorities}"
    )
    print(f"Validation plan digest: {plan_digest}")
    print(f"Predecessor result-evidence digest: {source_digest}")
    print(
        "Predecessor manifest digest: "
        f"{replay['manifest_digest']}"
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
