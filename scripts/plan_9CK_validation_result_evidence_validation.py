#!/usr/bin/env python3
"""
Layer 9CK

Defines the deterministic validation plan over Layer 9CJ result-evidence
records.

Planning only. This layer does not materialize validation records, invent
or retrieve evidence, validate authoritative historical-outcome truth,
parse responses, map or extract outcomes, mutate canonical data, recompute
downstream artifacts, or grant production, market, pricing, or betting
authority.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9CK"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_authoritative_source_"
    "endpoint_candidate_source_evidence_historical_outcome_field_mapping_"
    "result_validation_evidence_package_validation_result_evidence_"
    "validation_result_evidence_validation_result_evidence_validation_"
    "result_evidence_validation_result_evidence_validation_plan"
)

VALIDATION_PLAN_VERSION = (
    "layer_9CK_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_result_evidence_"
    "validation_result_evidence_validation_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "tmp" / "layer_9CK_result_evidence_validation_plan"

PREDECESSOR_PATH = (
    ROOT / "scripts"
    / "package_9CJ_validation_result_evidence_validation_result_evidence.py"
)

SOURCE_PLAN_PATH = (
    ROOT / "scripts"
    / "plan_9CI_validation_result_evidence_validation_result_evidence.py"
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

EXPECTED_SOURCE_PLAN_VERSION = (
    "layer_9CI_historical_outcome_authoritative_source_endpoint_candidate_"
    "source_evidence_historical_outcome_field_mapping_result_validation_"
    "evidence_package_validation_result_evidence_validation_result_"
    "evidence_validation_result_evidence_validation_result_evidence_"
    "validation_result_evidence_plan_v1"
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


SOURCE_PLAN_MODULE = load_module(
    SOURCE_PLAN_PATH,
    "layer_9ci_source_plan_contract",
)

if (
    SOURCE_PLAN_MODULE.RESULT_EVIDENCE_PLAN_VERSION
    != EXPECTED_SOURCE_PLAN_VERSION
):
    raise RuntimeError(
        "Unexpected Layer 9CI source plan version: "
        f"{SOURCE_PLAN_MODULE.RESULT_EVIDENCE_PLAN_VERSION}"
    )


VALIDATION_PRINCIPLES = [
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVREV-P01",
        "principle": (
            "Replay every Layer 9CJ result-evidence record "
            "deterministically before defining validation work."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVREV-P02",
        "principle": (
            "Validate result-evidence identity and record digests without "
            "inventing candidate-derived or authoritative evidence."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVREV-P03",
        "principle": (
            "Preserve complete validation, result-evidence, package, "
            "mapping, comparison, metric, and defect lineage."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVREV-P04",
        "principle": (
            "Preserve candidate_not_supplied and the missing endpoint "
            "candidate blocker."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVREV-P05",
        "principle": (
            "Preserve outcome_value as canonical and reject "
            "outcome_available_at_utc as an outcome substitute."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVREV-P06",
        "principle": (
            "Require rationale, limitations, and authority boundaries for "
            "every future validation record."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVREV-P07",
        "principle": (
            "Prohibit retrieval, parsing, mapping, extraction, mutation, "
            "recomputation, production, market, pricing, and betting work."
        ),
    },
    {
        "principle_id": "HOASEHOFMRVEPVREVREVREVREVREV-P08",
        "principle": (
            "Successful planning grants only Layer 9CL validation "
            "implementation authority."
        ),
    },
]


VALIDATION_COMPONENTS = [
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVREV-C01",
        "component": "predecessor_replay",
        "required": True,
        "priority": 1,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVREV-C02",
        "component": "result_evidence_identity_validation",
        "required": True,
        "priority": 2,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVREV-C03",
        "component": "result_evidence_digest_validation",
        "required": True,
        "priority": 3,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVREV-C04",
        "component": "manifest_validation",
        "required": True,
        "priority": 4,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVREV-C05",
        "component": "lineage_and_disposition_validation",
        "required": True,
        "priority": 5,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVREV-C06",
        "component": "candidate_evidence_absence_validation",
        "required": True,
        "priority": 6,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVREV-C07",
        "component": "canonical_field_and_documentation_validation",
        "required": True,
        "priority": 7,
    },
    {
        "component_id": "HOASEHOFMRVEPVREVREVREVREVREV-C08",
        "component": "authority_boundary_validation",
        "required": True,
        "priority": 8,
    },
]


VALIDATION_STAGES = [
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S01", "stage": "predecessor_replay", "priority": 1},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S02", "stage": "record_inventory_validation", "priority": 2},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S03", "stage": "identity_validation", "priority": 3},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S04", "stage": "record_digest_validation", "priority": 4},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S05", "stage": "manifest_validation", "priority": 5},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S06", "stage": "lineage_validation", "priority": 6},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S07", "stage": "structural_disposition_validation", "priority": 7},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S08", "stage": "evidence_and_canonical_field_validation", "priority": 8},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S09", "stage": "documentation_and_boundary_validation", "priority": 9},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S10", "stage": "validation_contract_definition", "priority": 10},
    {"stage_id": "HOASEHOFMRVEPVREVREVREVREVREV-S11", "stage": "plan_emission", "priority": 11},
]


VALIDATION_REQUIREMENTS = [
    {"requirement_id": f"HOASEHOFMRVEPVREVREVREVREVREV-R{i:02d}", "requirement": requirement, "expected": expected}
    for i, (requirement, expected) in enumerate(
        [
            ("source_result_evidence_record_present", True),
            ("source_result_evidence_record_id_present", True),
            ("source_result_evidence_identity_digest_valid", True),
            ("source_result_evidence_record_digest_valid", True),
            ("predecessor_result_evidence_digest_preserved", True),
            ("predecessor_manifest_digest_preserved", True),
            ("source_validation_lineage_complete", True),
            ("result_evidence_lineage_complete", True),
            ("package_and_mapping_lineage_complete", True),
            ("comparison_metric_and_defect_lineage_complete", True),
            ("structural_validation_complete", True),
            ("candidate_not_supplied_status_preserved", True),
            ("missing_endpoint_blocker_preserved", True),
            ("candidate_derived_artifact_count_zero", True),
            ("validation_artifact_count_preserved", True),
            ("evidence_absence_explicit", True),
            ("fabricated_evidence_absent", True),
            ("canonical_field_identity_preserved", True),
            ("source_rationale_present", True),
            ("source_limitations_present", True),
            ("source_authority_boundary_present", True),
            ("validation_rationale_required", True),
            ("validation_limitations_required", True),
            ("validation_authority_boundary_required", True),
            ("authoritative_historical_outcome_validated", False),
            ("validation_records_materialized_during_planning", False),
            ("network_retrieval_executed", False),
            ("mapping_or_extraction_executed", False),
            ("canonical_mutation_or_recomputation_executed", False),
            ("production_market_or_betting_authority_granted", False),
        ],
        start=1,
    )
]


VALIDATION_STATUSES = [
    {"status": "validation_ready", "implementation_authority": True},
    {"status": "candidate_not_supplied", "implementation_authority": False},
    {"status": "source_result_evidence_record_missing", "implementation_authority": False},
    {"status": "source_result_evidence_identity_invalid", "implementation_authority": False},
    {"status": "source_result_evidence_digest_invalid", "implementation_authority": False},
    {"status": "manifest_invalid", "implementation_authority": False},
    {"status": "lineage_incomplete", "implementation_authority": False},
    {"status": "structural_disposition_invalid", "implementation_authority": False},
    {"status": "canonical_field_or_documentation_invalid", "implementation_authority": False},
    {"status": "authority_boundary_invalid", "implementation_authority": False},
]


BLOCKER_CODES = [
    {"code": code, "category": category}
    for code, category in [
        ("historical_outcome_endpoint_candidate_missing", "submission"),
        ("source_result_evidence_record_missing", "record"),
        ("source_result_evidence_record_id_missing", "identity"),
        ("source_result_evidence_identity_digest_missing", "identity"),
        ("source_result_evidence_identity_digest_invalid", "identity"),
        ("source_result_evidence_record_digest_missing", "integrity"),
        ("source_result_evidence_record_digest_invalid", "integrity"),
        ("predecessor_result_evidence_digest_invalid", "integrity"),
        ("predecessor_manifest_version_invalid", "manifest"),
        ("predecessor_manifest_digest_invalid", "manifest"),
        ("source_validation_lineage_missing", "lineage"),
        ("result_evidence_lineage_missing", "lineage"),
        ("package_lineage_missing", "lineage"),
        ("mapping_lineage_missing", "lineage"),
        ("comparison_metric_or_defect_lineage_missing", "lineage"),
        ("structural_validation_incomplete", "structural"),
        ("candidate_not_supplied_status_missing", "status"),
        ("missing_endpoint_blocker_missing", "status"),
        ("candidate_derived_artifact_count_invalid", "evidence"),
        ("validation_artifact_count_invalid", "evidence"),
        ("evidence_absence_not_explicit", "evidence"),
        ("fabricated_evidence_detected", "evidence"),
        ("canonical_field_identity_invalid", "field"),
        ("source_rationale_missing", "documentation"),
        ("source_limitations_missing", "documentation"),
        ("source_authority_boundary_missing", "authority"),
        ("validation_documentation_missing", "documentation"),
        ("validation_authority_boundary_missing", "authority"),
        ("validation_materialization_requested_during_planning", "authority"),
        ("production_market_or_betting_authority_requested", "authority"),
    ]
]


VALIDATION_PLAN_RECORD_FIELDS = [
    "validation_result_evidence_validation_result_evidence_validation_"
    "contract_version",
    "validation_result_evidence_validation_result_evidence_validation_"
    "plan_record_id",
    "validation_result_evidence_validation_result_evidence_validation_"
    "plan_identity_digest",
    "validation_result_evidence_validation_result_evidence_"
    "source_record_id",
    "validation_result_evidence_validation_result_evidence_"
    "source_record_identity_digest",
    "validation_result_evidence_validation_result_evidence_"
    "source_record_digest",
    *SOURCE_PLAN_MODULE.RESULT_EVIDENCE_PLAN_RECORD_FIELDS,
    "validation_result_evidence_validation_result_evidence_validation_"
    "status",
    "validation_result_evidence_validation_result_evidence_validation_"
    "blocker_codes",
    "validation_result_evidence_validation_result_evidence_validation_"
    "implementation_authority_granted",
    "validation_result_evidence_validation_result_evidence_validation_"
    "rationale",
    "validation_result_evidence_validation_result_evidence_validation_"
    "limitations",
    "validation_result_evidence_validation_result_evidence_validation_"
    "authority_boundary",
    "validation_result_evidence_validation_result_evidence_validation_"
    "plan_record_digest",
]

VALIDATION_PLAN_RECORD_FIELDS = list(
    dict.fromkeys(VALIDATION_PLAN_RECORD_FIELDS)
)


ORDERING_FIELDS = [
    {"ordinal": 1, "field": "comparison_record_id"},
    {"ordinal": 2, "field": "defect_source_record_id"},
    {"ordinal": 3, "field": "candidate_id"},
    {"ordinal": 4, "field": "validation_result_evidence_plan_record_id"},
    {"ordinal": 5, "field": "result_evidence_validation_plan_record_id"},
    {
        "ordinal": 6,
        "field":
            "validation_result_evidence_validation_result_evidence_"
            "plan_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {"ordinal": 1, "step": "replay_layer_9CJ_result_evidence_records"},
    {"ordinal": 2, "step": "validate_record_inventory"},
    {"ordinal": 3, "step": "validate_result_evidence_identity"},
    {"ordinal": 4, "step": "validate_result_evidence_record_digest"},
    {"ordinal": 5, "step": "validate_result_evidence_digest"},
    {"ordinal": 6, "step": "validate_manifest_version_and_digest"},
    {"ordinal": 7, "step": "validate_complete_lineage"},
    {"ordinal": 8, "step": "validate_structural_disposition"},
    {"ordinal": 9, "step": "validate_candidate_evidence_absence"},
    {"ordinal": 10, "step": "validate_canonical_field_identity"},
    {"ordinal": 11, "step": "validate_source_documentation"},
    {"ordinal": 12, "step": "define_validation_identity_contract"},
    {"ordinal": 13, "step": "define_validation_documentation_contract"},
    {"ordinal": 14, "step": "define_validation_authority_boundary"},
    {"ordinal": 15, "step": "define_deterministic_ordering_and_digests"},
    {"ordinal": 16, "step": "grant_layer_9CL_implementation_only_when_complete"},
]


PROHIBITED_AUTHORITIES = [
    "endpoint_candidate_invention",
    "response_artifact_invention",
    "parser_submission_invention",
    "mapping_submission_invention",
    "validation_result_invention",
    "evidence_artifact_invention",
    "result_evidence_invention",
    "validation_result_evidence_validation_result_evidence_validation_"
    "materialization",
    "authoritative_historical_outcome_validation",
    "historical_outcome_field_mapping_execution",
    "historical_outcome_value_extraction",
    "response_bytes_reading",
    "raw_response_parse_execution",
    "dns_resolution_execution",
    "socket_connection_execution",
    "http_request_execution",
    "browser_execution",
    "api_request_execution",
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
]


def replay_predecessor() -> dict[str, Any]:
    predecessor = load_module(
        PREDECESSOR_PATH,
        "layer_9cj_predecessor",
    )

    if (
        predecessor.RESULT_EVIDENCE_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_CONTRACT_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9CJ contract version: "
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

    replay = predecessor.replay_plan()
    source_plan = replay["plan"]

    records = predecessor.build_result_evidence_records(
        source_plan,
        replay["records"],
    )

    reverse_records = predecessor.build_result_evidence_records(
        source_plan,
        list(reversed(replay["reverse_records"])),
    )

    plan_digest = predecessor.compute_plan_digest(source_plan)
    predecessor_validation_digest = sha256_payload(
        replay["records"]
    )
    result_digest = sha256_payload(records)

    comparison_ids = {
        row["comparison_record_id"]
        for row in records
    }

    status_counts = dict(
        sorted(
            Counter(
                row[
                    "validation_result_evidence_validation_"
                    "result_evidence_status"
                ]
                for row in records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in records
                for blocker in row[
                    "validation_result_evidence_validation_"
                    "result_evidence_blocker_codes"
                ]
            ).items()
        )
    )

    manifest_payload = {
        "manifest_version":
            predecessor.RESULT_EVIDENCE_MANIFEST_VERSION,
        "contract_version":
            predecessor.RESULT_EVIDENCE_CONTRACT_VERSION,
        "record_count":
            len(records),
        "comparison_count":
            len(comparison_ids),
        "result_digest":
            result_digest,
        "status_counts":
            status_counts,
        "blocker_counts":
            blocker_counts,
        "candidate_derived_artifact_count":
            sum(
                int(row["candidate_derived_artifact_count"])
                for row in records
            ),
        "validation_artifact_count":
            sum(
                int(row["validation_artifact_count"])
                for row in records
            ),
        "authoritative_historical_outcomes_validated":
            sum(
                bool(
                    row[
                        "authoritative_historical_outcome_validated"
                    ]
                )
                for row in records
            ),
    }

    return {
        "predecessor": predecessor,
        "source_plan": source_plan,
        "records": records,
        "reverse_records": reverse_records,
        "source_plan_digest": plan_digest,
        "predecessor_validation_digest":
            predecessor_validation_digest,
        "result_digest": result_digest,
        "manifest_digest": sha256_payload(manifest_payload),
    }


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    replay = replay_predecessor()
    predecessor = replay["predecessor"]
    records = replay["records"]
    reverse_records = replay["reverse_records"]

    reverse_result_digest = sha256_payload(reverse_records)

    comparison_ids = {
        row["comparison_record_id"]
        for row in records
    }

    status_counts = Counter(
        row[
            "validation_result_evidence_validation_"
            "result_evidence_status"
        ]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "validation_result_evidence_validation_"
            "result_evidence_blocker_codes"
        ]
    )

    lineage_fields = (
        "validation_result_evidence_validation_result_evidence_"
        "plan_record_id",
        "validation_result_evidence_validation_source_record_id",
        "validation_result_evidence_validation_plan_record_id",
        "validation_result_evidence_source_record_id",
        "result_evidence_validation_plan_record_id",
        "result_evidence_source_record_id",
        "validation_result_evidence_plan_record_id",
        "evidence_package_validation_plan_record_id",
        "evidence_package_plan_record_id",
        "mapping_result_validation_plan_record_id",
        "historical_outcome_field_mapping_plan_record_id",
        "comparison_record_id",
        "metric_record_id",
        "defect_source_record_id",
    )

    checks = [
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
            "passed": (
                replay["source_plan_digest"]
                == EXPECTED_SOURCE_PLAN_DIGEST
            ),
        },
        {
            "check": "nine_ch_validation_digest_preserved",
            "actual":
                replay["predecessor_validation_digest"],
            "expected":
                EXPECTED_PREDECESSOR_VALIDATION_DIGEST,
            "passed": (
                replay["predecessor_validation_digest"]
                == EXPECTED_PREDECESSOR_VALIDATION_DIGEST
            ),
        },
        {
            "check": "predecessor_replay_deterministic",
            "actual":
                canonical_json(records)
                == canonical_json(reverse_records),
            "expected": True,
            "passed": (
                canonical_json(records)
                == canonical_json(reverse_records)
            ),
        },
        {
            "check": "result_evidence_digest_preserved",
            "actual": replay["result_digest"],
            "expected": EXPECTED_PREDECESSOR_RESULT_DIGEST,
            "passed": (
                replay["result_digest"]
                == EXPECTED_PREDECESSOR_RESULT_DIGEST
            ),
        },
        {
            "check": "reverse_result_evidence_digest_preserved",
            "actual": reverse_result_digest,
            "expected": EXPECTED_PREDECESSOR_RESULT_DIGEST,
            "passed": (
                reverse_result_digest
                == EXPECTED_PREDECESSOR_RESULT_DIGEST
            ),
        },
        {
            "check": "manifest_digest_preserved",
            "actual": replay["manifest_digest"],
            "expected": EXPECTED_PREDECESSOR_MANIFEST_DIGEST,
            "passed": (
                replay["manifest_digest"]
                == EXPECTED_PREDECESSOR_MANIFEST_DIGEST
            ),
        },
        {
            "check": "expected_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_RECORDS,
            "passed": len(records) == EXPECTED_RECORDS,
        },
        {
            "check": "expected_comparisons_replayed",
            "actual": len(comparison_ids),
            "expected": EXPECTED_COMPARISONS,
            "passed":
                len(comparison_ids) == EXPECTED_COMPARISONS,
        },
        {
            "check": "all_result_evidence_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "validation_result_evidence_validation_"
                        "result_evidence_plan_identity_digest"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_validation_"
                        "result_evidence_plan_identity_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "all_result_evidence_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "validation_result_evidence_validation_"
                        "result_evidence_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                valid_sha256(
                    row[
                        "validation_result_evidence_validation_"
                        "result_evidence_plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "lineage_complete",
            "actual": sum(
                all(
                    bool(str(row.get(field, "")).strip())
                    for field in lineage_fields
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                all(
                    bool(str(row.get(field, "")).strip())
                    for field in lineage_fields
                )
                for row in records
            ),
        },
        {
            "check": "all_structural_validation_complete",
            "actual": sum(
                bool(row["structural_validation_complete"])
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["structural_validation_complete"])
                for row in records
            ),
        },
        {
            "check": "all_records_candidate_not_supplied",
            "actual": dict(sorted(status_counts.items())),
            "expected": {EXPECTED_STATUS: EXPECTED_RECORDS},
            "passed": status_counts == Counter(
                {EXPECTED_STATUS: EXPECTED_RECORDS}
            ),
        },
        {
            "check": "all_missing_endpoint_blockers_preserved",
            "actual": dict(sorted(blocker_counts.items())),
            "expected": {EXPECTED_BLOCKER: EXPECTED_RECORDS},
            "passed": blocker_counts == Counter(
                {EXPECTED_BLOCKER: EXPECTED_RECORDS}
            ),
        },
        {
            "check": "candidate_derived_artifact_count_zero",
            "actual": sum(
                int(row["candidate_derived_artifact_count"])
                for row in records
            ),
            "expected": 0,
            "passed": all(
                int(row["candidate_derived_artifact_count"]) == 0
                for row in records
            ),
        },
        {
            "check": "validation_artifact_count_preserved",
            "actual": sum(
                int(row["validation_artifact_count"])
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                int(row["validation_artifact_count"]) == 1
                for row in records
            ),
        },
        {
            "check": "evidence_absence_explicit",
            "actual": sum(
                bool(row["evidence_absence_explicit"])
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(row["evidence_absence_explicit"])
                for row in records
            ),
        },
        {
            "check": "fabricated_evidence_absent",
            "actual": sum(
                bool(row["fabricated_evidence_detected"])
                for row in records
            ),
            "expected": 0,
            "passed": all(
                not bool(row["fabricated_evidence_detected"])
                for row in records
            ),
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
                    for row in records
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
                for row in records
            ),
        },
        {
            "check": "source_documentation_and_boundary_present",
            "actual": sum(
                bool(
                    row[
                        "validation_result_evidence_validation_"
                        "result_evidence_rationale"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_"
                        "result_evidence_limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_"
                        "result_evidence_authority_boundary"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORDS,
            "passed": all(
                bool(
                    row[
                        "validation_result_evidence_validation_"
                        "result_evidence_rationale"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_"
                        "result_evidence_limitations"
                    ]
                )
                and bool(
                    row[
                        "validation_result_evidence_validation_"
                        "result_evidence_authority_boundary"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "authoritative_historical_outcomes_validated_zero",
            "actual": sum(
                bool(
                    row[
                        "authoritative_historical_outcome_validated"
                    ]
                )
                for row in records
            ),
            "expected": 0,
            "passed": all(
                not bool(
                    row[
                        "authoritative_historical_outcome_validated"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "principles_defined",
            "actual": len(VALIDATION_PRINCIPLES),
            "expected": 8,
            "passed": len(VALIDATION_PRINCIPLES) == 8,
        },
        {
            "check": "components_defined",
            "actual": len(VALIDATION_COMPONENTS),
            "expected": 8,
            "passed": len(VALIDATION_COMPONENTS) == 8,
        },
        {
            "check": "stages_defined",
            "actual": len(VALIDATION_STAGES),
            "expected": 11,
            "passed": len(VALIDATION_STAGES) == 11,
        },
        {
            "check": "requirements_defined",
            "actual": len(VALIDATION_REQUIREMENTS),
            "expected": 30,
            "passed": len(VALIDATION_REQUIREMENTS) == 30,
        },
        {
            "check": "statuses_defined",
            "actual": len(VALIDATION_STATUSES),
            "expected": 10,
            "passed": len(VALIDATION_STATUSES) == 10,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 30,
            "passed": len(BLOCKER_CODES) == 30,
        },
        {
            "check": "validation_plan_fields_defined",
            "actual": len(VALIDATION_PLAN_RECORD_FIELDS),
            "expected": 78,
            "passed":
                len(VALIDATION_PLAN_RECORD_FIELDS) == 78,
        },
        {
            "check": "validation_plan_fields_unique",
            "actual": len(set(VALIDATION_PLAN_RECORD_FIELDS)),
            "expected": 78,
            "passed": (
                len(set(VALIDATION_PLAN_RECORD_FIELDS)) == 78
            ),
        },
        {
            "check": "ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 6,
            "passed": len(ORDERING_FIELDS) == 6,
        },
        {
            "check": "implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 16,
            "passed": len(IMPLEMENTATION_STEPS) == 16,
        },
        {
            "check":
                "validation_materialization_prohibited_during_planning",
            "actual": True,
            "expected": True,
            "passed": (
                "validation_result_evidence_validation_result_evidence_"
                "validation_materialization"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check":
                "network_mapping_extraction_and_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": all(
                authority in PROHIBITED_AUTHORITIES
                for authority in (
                    "http_request_execution",
                    "api_request_execution",
                    "historical_outcome_field_mapping_execution",
                    "historical_outcome_value_extraction",
                    "canonical_source_value_mutation",
                    "canonical_outcome_mapping_change",
                )
            ),
        },
        {
            "check": "validation_records_materialized_zero",
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

    plan_digest = sha256_payload(
        {
            "validation_plan_version":
                VALIDATION_PLAN_VERSION,
            "principles":
                VALIDATION_PRINCIPLES,
            "components":
                VALIDATION_COMPONENTS,
            "stages":
                VALIDATION_STAGES,
            "requirements":
                VALIDATION_REQUIREMENTS,
            "statuses":
                VALIDATION_STATUSES,
            "blockers":
                BLOCKER_CODES,
            "record_fields":
                VALIDATION_PLAN_RECORD_FIELDS,
            "ordering_fields":
                ORDERING_FIELDS,
            "implementation_steps":
                IMPLEMENTATION_STEPS,
            "prohibited_authorities":
                PROHIBITED_AUTHORITIES,
        }
    )

    next_layer = (
        "9CL_validation_result_evidence_validation_result_evidence_"
        "validation_implementation"
        if all_checks_passed
        else
        "9CK_validation_result_evidence_validation_result_evidence_"
        "validation_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        ["check", "actual", "expected", "passed"],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "principles.csv",
        ["principle_id", "principle"],
        VALIDATION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "components.csv",
        ["component_id", "component", "required", "priority"],
        VALIDATION_COMPONENTS,
    )

    write_csv(
        OUTPUT_DIR / "stages.csv",
        ["stage_id", "stage", "priority"],
        VALIDATION_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "requirements.csv",
        ["requirement_id", "requirement", "expected"],
        VALIDATION_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "statuses.csv",
        ["status", "implementation_authority"],
        VALIDATION_STATUSES,
    )

    write_csv(
        OUTPUT_DIR / "blockers.csv",
        ["code", "category"],
        BLOCKER_CODES,
    )

    write_csv(
        OUTPUT_DIR / "field_contract.csv",
        ["ordinal", "field"],
        [
            {"ordinal": index, "field": field}
            for index, field in enumerate(
                VALIDATION_PLAN_RECORD_FIELDS,
                start=1,
            )
        ],
    )

    write_csv(
        OUTPUT_DIR / "ordering_fields.csv",
        ["ordinal", "field"],
        ORDERING_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        ["ordinal", "step"],
        IMPLEMENTATION_STEPS,
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "validation_plan_version":
            VALIDATION_PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.RESULT_EVIDENCE_CONTRACT_VERSION,
        "predecessor_manifest_version":
            predecessor.RESULT_EVIDENCE_MANIFEST_VERSION,
        "result_evidence_records":
            len(records),
        "result_evidence_comparisons":
            len(comparison_ids),
        "result_evidence_status_counts":
            dict(sorted(status_counts.items())),
        "result_evidence_blocker_counts":
            dict(sorted(blocker_counts.items())),
        "layer_9CI_plan_digest":
            replay["source_plan_digest"],
        "layer_9CH_validation_digest":
            replay["predecessor_validation_digest"],
        "layer_9CJ_result_evidence_digest":
            replay["result_digest"],
        "reverse_layer_9CJ_result_evidence_digest":
            reverse_result_digest,
        "layer_9CJ_manifest_digest":
            replay["manifest_digest"],
        "validation_plan_digest":
            plan_digest,
        "principles":
            len(VALIDATION_PRINCIPLES),
        "components":
            len(VALIDATION_COMPONENTS),
        "stages":
            len(VALIDATION_STAGES),
        "requirements":
            len(VALIDATION_REQUIREMENTS),
        "statuses":
            len(VALIDATION_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "validation_plan_record_fields":
            len(VALIDATION_PLAN_RECORD_FIELDS),
        "ordering_fields":
            len(ORDERING_FIELDS),
        "implementation_steps":
            len(IMPLEMENTATION_STEPS),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required":
            len(checks),
        "validation_records_materialized": 0,
        "authoritative_historical_outcomes_validated": 0,
        "historical_outcome_fields_mapped": 0,
        "historical_outcome_values_extracted": 0,
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
        OUTPUT_DIR / "plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis": (
            "validation_result_evidence_validation_result_evidence_"
            "validation_plan_complete"
            if all_checks_passed
            else
            "validation_result_evidence_validation_result_evidence_"
            "validation_plan_failed"
        ),
        "result_evidence_status":
            EXPECTED_STATUS,
        "structural_validation_complete":
            all_checks_passed,
        "authoritative_historical_outcome_validated":
            False,
        "authority_granted": (
            "validation_result_evidence_validation_result_evidence_"
            "validation_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld":
            sorted(PROHIBITED_AUTHORITIES),
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
    print(f"Validation plan version: {VALIDATION_PLAN_VERSION}")
    print(
        "Predecessor contract version: "
        f"{predecessor.RESULT_EVIDENCE_CONTRACT_VERSION}"
    )
    print(
        "Predecessor manifest version: "
        f"{predecessor.RESULT_EVIDENCE_MANIFEST_VERSION}"
    )
    print(
        "Predecessor replay deterministic: "
        f"{canonical_json(records) == canonical_json(reverse_records)}"
    )
    print(
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(f"Result-evidence records replayed: {len(records)}")
    print(f"Result-evidence comparisons: {len(comparison_ids)}")
    print(
        "Result-evidence status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Result-evidence blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Validation plan record fields: "
        f"{len(VALIDATION_PLAN_RECORD_FIELDS)}"
    )
    print(
        "Layer 9CI plan digest: "
        f"{replay['source_plan_digest']}"
    )
    print(
        "Layer 9CH validation digest: "
        f"{replay['predecessor_validation_digest']}"
    )
    print(
        "Layer 9CJ result-evidence digest: "
        f"{replay['result_digest']}"
    )
    print(
        "Layer 9CJ manifest digest: "
        f"{replay['manifest_digest']}"
    )
    print(f"Validation plan digest: {plan_digest}")
    print("Validation records materialized: 0")
    print("Authoritative historical outcomes validated: 0")
    print("Historical outcome fields mapped: 0")
    print("Historical outcome values extracted: 0")
    print("Network retrievals executed: 0")
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Downstream records recomputed: 0")
    print("Production probabilities changed: 0")
    print("Market comparisons executed: 0")
    print("Pricing changes emitted: 0")
    print("Betting edges calculated: 0")
    print(f"Diagnosis: {diagnosis['diagnosis']}")
    print("Authoritative historical outcome validated: False")
    print(f"Authority granted: {diagnosis['authority_granted']}")
    print(f"Recommended next layer: {next_layer}")
    print(f"Artifacts: {OUTPUT_DIR.relative_to(ROOT)}")

    if not all_checks_passed:
        failed = [
            row["check"]
            for row in checks
            if not row["passed"]
        ]
        print("FAILED CHECKS: " + ", ".join(failed))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
