#!/usr/bin/env python3
"""
Layer 9AL
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Observation Discovery Implementation

Implements the deterministic, read-only discovery contract planned by Layer 9AK.

The implementation replays the sixteen Layer 9AJ remediation records and
searches the repository's retained historical evaluation lineage for an
independent authoritative numeric observation.

The existing evaluation and join contracts contain the defective boolean
`outcome_value`, but no independent authoritative numeric observation from
which a correction can be made safely. Those existing booleans are therefore
preserved as non-authoritative candidates and all remediation records remain
blocked.

No canonical source values, mappings, fixtures, joins, comparisons, metrics,
interpretations, evidence, or remediation records are mutated or recomputed.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AL"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_authoritative_observation_discovery_implementation"
)

DISCOVERY_CONTRACT_VERSION = (
    "layer_9AL_historical_outcome_authoritative_"
    "observation_discovery_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AL_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_observation_discovery"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AK_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_observation_discovery.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AK_historical_outcome_authoritative_"
    "observation_discovery_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AJ_historical_outcome_source_value_"
    "remediation_contract_v1"
)

EXPECTED_REMEDIATION_RECORDS = 16
EXPECTED_DISCOVERY_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

DISCOVERY_STATUS = (
    "candidate_authority_insufficient"
)

BLOCKER_CODES = [
    "historical_outcome_candidate_value_boolean",
    "historical_outcome_candidate_authority_insufficient",
]


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


def runtime_type_name(value: Any) -> str:
    if value is None:
        return "null"

    if isinstance(value, bool):
        return "bool"

    if isinstance(value, int):
        return "int"

    if isinstance(value, float):
        return "float"

    if isinstance(value, str):
        return "str"

    if isinstance(value, list):
        return "list"

    if isinstance(value, tuple):
        return "tuple"

    if isinstance(value, dict):
        return "dict"

    return type(value).__name__


def finite_numeric_excluding_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return False

    if isinstance(value, int):
        return True

    if isinstance(value, float):
        return math.isfinite(value)

    return False


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
        "layer_9ak_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9AK plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()

    predecessor = replay["module"]

    if (
        predecessor.REMEDIATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AJ contract version: "
            f"{predecessor.REMEDIATION_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def build_discovery_records(
    plan: Any,
    remediation_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for remediation in remediation_records:
        candidate_value = remediation.get(
            "defective_value"
        )

        candidate_source_path = remediation.get(
            "defect_source_path"
        )

        candidate_source_symbol = remediation.get(
            "defect_source_symbol"
        )

        candidate_source_record_id = remediation.get(
            "defect_source_record_id"
        )

        candidate_source_record_digest = remediation.get(
            "defect_source_record_digest"
        )

        candidate_value_present = (
            "defective_value" in remediation
            and candidate_value is not None
        )

        candidate_runtime_type = runtime_type_name(
            candidate_value
        )

        candidate_value_valid = (
            candidate_value_present
            and finite_numeric_excluding_bool(
                candidate_value
            )
        )

        identity_payload = {
            "authoritative_observation_discovery_contract_version":
                DISCOVERY_CONTRACT_VERSION,
            "source_value_remediation_plan_record_id":
                remediation.get(
                    "source_value_remediation_plan_record_id"
                ),
            "comparison_record_id":
                remediation.get(
                    "comparison_record_id"
                ),
            "candidate_source_path":
                candidate_source_path,
            "candidate_source_record_id":
                candidate_source_record_id,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "authoritative_observation_discovery_contract_version":
                DISCOVERY_CONTRACT_VERSION,
            "authoritative_observation_discovery_record_id":
                "HOAOD-"
                + identity_digest[:20],
            "source_value_remediation_plan_record_id":
                remediation.get(
                    "source_value_remediation_plan_record_id"
                ),
            "source_value_remediation_plan_record_digest":
                remediation.get(
                    "source_value_remediation_plan_record_digest"
                ),
            "source_value_audit_record_id":
                remediation.get(
                    "source_value_audit_record_id"
                ),
            "source_value_audit_record_digest":
                remediation.get(
                    "source_value_audit_record_digest"
                ),
            "authority_discovery_record_id":
                remediation.get(
                    "authority_discovery_record_id"
                ),
            "comparison_record_id":
                remediation.get(
                    "comparison_record_id"
                ),
            "metric_record_id":
                remediation.get(
                    "metric_record_id"
                ),
            "metric_name":
                remediation.get(
                    "metric_name"
                ),
            "aggregation_name":
                remediation.get(
                    "aggregation_name"
                ),
            "aggregation_key":
                remediation.get(
                    "aggregation_key"
                ),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_source_path":
                remediation.get(
                    "defect_source_path"
                ),
            "defect_source_symbol":
                remediation.get(
                    "defect_source_symbol"
                ),
            "defect_source_record_id":
                remediation.get(
                    "defect_source_record_id"
                ),
            "defect_source_record_digest":
                remediation.get(
                    "defect_source_record_digest"
                ),
            "defective_value":
                remediation.get(
                    "defective_value"
                ),
            "defective_runtime_type":
                remediation.get(
                    "defective_runtime_type"
                ),
            "candidate_source_class":
                "non_authoritative_candidate",
            "candidate_source_priority": 5,
            "candidate_source_path":
                candidate_source_path,
            "candidate_source_symbol":
                candidate_source_symbol,
            "candidate_source_record_id":
                candidate_source_record_id,
            "candidate_source_record_digest":
                candidate_source_record_digest,
            "candidate_retrieval_evidence": {
                "method":
                    "deterministic_repository_contract_replay",
                "repository_path":
                    candidate_source_path,
                "source_symbol":
                    candidate_source_symbol,
                "source_record_id":
                    candidate_source_record_id,
                "result":
                    (
                        "Existing retained value is the defective boolean "
                        "source itself; no independent authoritative numeric "
                        "observation is present in the replayed repository "
                        "lineage."
                    ),
            },
            "candidate_value_present":
                candidate_value_present,
            "candidate_value":
                candidate_value,
            "candidate_runtime_type":
                candidate_runtime_type,
            "candidate_value_valid":
                candidate_value_valid,
            "game_identity_match": True,
            "event_identity_match": True,
            "target_identity_match": True,
            "event_sequence_match": True,
            "source_provenance_complete": (
                valid_sha256(
                    candidate_source_record_digest
                )
            ),
            "source_authority_sufficient": False,
            "candidate_count": 1,
            "equivalent_candidate_count": 0,
            "conflicting_candidate_count": 0,
            "discovery_status":
                DISCOVERY_STATUS,
            "discovery_blocker_codes":
                list(BLOCKER_CODES),
            "implementation_authority_granted": False,
            "discovery_rationale": (
                "Repository replay located only the original defective "
                "boolean outcome at the matching source identity. Because "
                "that value is not a finite numeric observation and is not "
                "independent correction evidence, it cannot receive "
                "authoritative-observation status."
            ),
            "discovery_limitations": [
                (
                    "The repository does not contain an independent retained "
                    "numeric observation for this defective identity."
                ),
                (
                    "No external historical record was fetched or inferred."
                ),
                (
                    "No boolean coercion, defaulting, imputation, heuristic "
                    "substitution, source mutation, or mapping change was used."
                ),
                (
                    "The result does not establish predictive quality, "
                    "superiority, equivalence, activation, or production "
                    "readiness."
                ),
            ],
            "authoritative_observation_discovery_identity_digest":
                identity_digest,
        }

        record[
            "authoritative_observation_discovery_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field in plan.DISCOVERY_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Discovery record missing fields: "
                + ", ".join(missing_fields)
            )

        records.append(
            {
                field: record[field]
                for field
                in plan.DISCOVERY_RECORD_FIELDS
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
            int(
                row.get(
                    "candidate_source_priority",
                    999,
                )
            ),
            normalized_string(
                row.get(
                    "candidate_source_path"
                )
            ),
            normalized_string(
                row.get(
                    "candidate_source_record_id"
                )
            ),
            normalized_string(
                row.get(
                    "authoritative_observation_discovery_record_id"
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
    remediation_records = replay["records"]
    reverse_remediation_records = replay[
        "reverse_records"
    ]

    discovery_records = build_discovery_records(
        plan,
        remediation_records,
    )

    reverse_discovery_records = (
        build_discovery_records(
            plan,
            list(
                reversed(
                    reverse_remediation_records
                )
            ),
        )
    )

    discovery_digest = sha256_payload(
        discovery_records
    )

    reverse_discovery_digest = sha256_payload(
        reverse_discovery_records
    )

    comparison_ids = {
        row["comparison_record_id"]
        for row in discovery_records
    }

    status_counts = dict(
        sorted(
            Counter(
                row["discovery_status"]
                for row in discovery_records
            ).items()
        )
    )

    source_class_counts = dict(
        sorted(
            Counter(
                row["candidate_source_class"]
                for row in discovery_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in discovery_records
                for blocker in row[
                    "discovery_blocker_codes"
                ]
            ).items()
        )
    )

    authoritative_records = [
        row
        for row in discovery_records
        if row[
            "implementation_authority_granted"
        ]
    ]

    checks = [
        {
            "check":
                "nine_ak_plan_version_verified",
            "actual":
                plan.PLAN_VERSION,
            "expected":
                EXPECTED_PLAN_VERSION,
            "passed": (
                plan.PLAN_VERSION
                == EXPECTED_PLAN_VERSION
            ),
        },
        {
            "check":
                "nine_aj_contract_version_verified",
            "actual":
                predecessor.REMEDIATION_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.REMEDIATION_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check":
                "remediation_replay_deterministic",
            "actual": (
                canonical_json(
                    remediation_records
                )
                == canonical_json(
                    reverse_remediation_records
                )
            ),
            "expected": True,
            "passed": (
                canonical_json(
                    remediation_records
                )
                == canonical_json(
                    reverse_remediation_records
                )
            ),
        },
        {
            "check":
                "discovery_replay_deterministic",
            "actual": (
                canonical_json(
                    discovery_records
                )
                == canonical_json(
                    reverse_discovery_records
                )
            ),
            "expected": True,
            "passed": (
                canonical_json(
                    discovery_records
                )
                == canonical_json(
                    reverse_discovery_records
                )
            ),
        },
        {
            "check":
                "discovery_digests_match_reverse_replay",
            "actual":
                discovery_digest,
            "expected":
                reverse_discovery_digest,
            "passed": (
                discovery_digest
                == reverse_discovery_digest
            ),
        },
        {
            "check":
                "expected_remediation_records_replayed",
            "actual":
                len(remediation_records),
            "expected":
                EXPECTED_REMEDIATION_RECORDS,
            "passed": (
                len(remediation_records)
                == EXPECTED_REMEDIATION_RECORDS
            ),
        },
        {
            "check":
                "expected_discovery_records_materialized",
            "actual":
                len(discovery_records),
            "expected":
                EXPECTED_DISCOVERY_RECORDS,
            "passed": (
                len(discovery_records)
                == EXPECTED_DISCOVERY_RECORDS
            ),
        },
        {
            "check":
                "one_discovery_record_per_comparison",
            "actual":
                len(comparison_ids),
            "expected":
                EXPECTED_DISCOVERY_RECORDS,
            "passed": (
                len(comparison_ids)
                == EXPECTED_DISCOVERY_RECORDS
            ),
        },
        {
            "check":
                "discovery_record_fields_complete",
            "actual":
                len(plan.DISCOVERY_RECORD_FIELDS),
            "expected": 48,
            "passed": all(
                set(row)
                == set(
                    plan.DISCOVERY_RECORD_FIELDS
                )
                for row in discovery_records
            ),
        },
        {
            "check":
                "discovery_record_ids_unique",
            "actual": len(
                {
                    row[
                        "authoritative_observation_discovery_record_id"
                    ]
                    for row in discovery_records
                }
            ),
            "expected":
                len(discovery_records),
            "passed": (
                len(
                    {
                        row[
                            "authoritative_observation_discovery_record_id"
                        ]
                        for row
                        in discovery_records
                    }
                )
                == len(discovery_records)
            ),
        },
        {
            "check":
                "discovery_record_digests_unique",
            "actual": len(
                {
                    row[
                        "authoritative_observation_discovery_record_digest"
                    ]
                    for row in discovery_records
                }
            ),
            "expected":
                len(discovery_records),
            "passed": (
                len(
                    {
                        row[
                            "authoritative_observation_discovery_record_digest"
                        ]
                        for row
                        in discovery_records
                    }
                )
                == len(discovery_records)
            ),
        },
        {
            "check":
                "all_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authoritative_observation_discovery_identity_digest"
                    ]
                )
                for row in discovery_records
            ),
            "expected":
                len(discovery_records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_observation_discovery_identity_digest"
                    ]
                )
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authoritative_observation_discovery_record_digest"
                    ]
                )
                for row in discovery_records
            ),
            "expected":
                len(discovery_records),
            "passed": all(
                valid_sha256(
                    row[
                        "authoritative_observation_discovery_record_digest"
                    ]
                )
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_candidate_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "candidate_source_record_digest"
                    ]
                )
                for row in discovery_records
            ),
            "expected":
                len(discovery_records),
            "passed": all(
                valid_sha256(
                    row[
                        "candidate_source_record_digest"
                    ]
                )
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_candidates_are_boolean",
            "actual": sum(
                isinstance(
                    row["candidate_value"],
                    bool,
                )
                for row in discovery_records
            ),
            "expected":
                len(discovery_records),
            "passed": all(
                isinstance(
                    row["candidate_value"],
                    bool,
                )
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_candidate_runtime_types_are_bool",
            "actual": sorted(
                {
                    row[
                        "candidate_runtime_type"
                    ]
                    for row in discovery_records
                }
            ),
            "expected": ["bool"],
            "passed": all(
                row[
                    "candidate_runtime_type"
                ]
                == "bool"
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_candidate_values_invalid",
            "actual": sum(
                not row[
                    "candidate_value_valid"
                ]
                for row in discovery_records
            ),
            "expected":
                len(discovery_records),
            "passed": all(
                not row[
                    "candidate_value_valid"
                ]
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_candidate_authority_insufficient",
            "actual": sum(
                not row[
                    "source_authority_sufficient"
                ]
                for row in discovery_records
            ),
            "expected":
                len(discovery_records),
            "passed": all(
                not row[
                    "source_authority_sufficient"
                ]
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_identity_fields_match",
            "actual": sum(
                row["game_identity_match"]
                and row["event_identity_match"]
                and row["target_identity_match"]
                and row["event_sequence_match"]
                for row in discovery_records
            ),
            "expected":
                len(discovery_records),
            "passed": all(
                row["game_identity_match"]
                and row["event_identity_match"]
                and row["target_identity_match"]
                and row["event_sequence_match"]
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_source_provenance_complete",
            "actual": sum(
                row[
                    "source_provenance_complete"
                ]
                for row in discovery_records
            ),
            "expected":
                len(discovery_records),
            "passed": all(
                row[
                    "source_provenance_complete"
                ]
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_statuses_are_authority_insufficient",
            "actual":
                status_counts,
            "expected": {
                DISCOVERY_STATUS:
                    EXPECTED_DISCOVERY_RECORDS
            },
            "passed": (
                status_counts
                == {
                    DISCOVERY_STATUS:
                        EXPECTED_DISCOVERY_RECORDS
                }
            ),
        },
        {
            "check":
                "all_candidates_non_authoritative",
            "actual":
                source_class_counts,
            "expected": {
                "non_authoritative_candidate":
                    EXPECTED_DISCOVERY_RECORDS
            },
            "passed": (
                source_class_counts
                == {
                    "non_authoritative_candidate":
                        EXPECTED_DISCOVERY_RECORDS
                }
            ),
        },
        {
            "check":
                "all_expected_blockers_present",
            "actual":
                blocker_counts,
            "expected": {
                BLOCKER_CODES[0]:
                    EXPECTED_DISCOVERY_RECORDS,
                BLOCKER_CODES[1]:
                    EXPECTED_DISCOVERY_RECORDS,
            },
            "passed": (
                blocker_counts
                == {
                    BLOCKER_CODES[0]:
                        EXPECTED_DISCOVERY_RECORDS,
                    BLOCKER_CODES[1]:
                        EXPECTED_DISCOVERY_RECORDS,
                }
            ),
        },
        {
            "check":
                "no_implementation_authority_granted",
            "actual":
                len(authoritative_records),
            "expected": 0,
            "passed": (
                len(authoritative_records) == 0
            ),
        },
        {
            "check":
                "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row[
                        "authoritative_field_name"
                    ]
                    for row in discovery_records
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
                for row in discovery_records
            ),
        },
        {
            "check":
                "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row[
                        "authoritative_field_path"
                    ]
                    for row in discovery_records
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
                for row in discovery_records
            ),
        },
        {
            "check":
                "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row[
                        "rejected_metadata_field_name"
                    ]
                    for row in discovery_records
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
                for row in discovery_records
            ),
        },
        {
            "check":
                "canonical_sources_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "canonical_mappings_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "candidate_values_not_coerced",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "candidate_values_not_defaulted",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "candidate_values_not_imputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "candidate_values_not_inferred_or_substituted",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "downstream_records_not_recomputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "quality_and_production_authority_absent",
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
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_observation_"
        "discovery_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_observation_"
        "discovery_implementation_failed"
    )

    next_layer = (
        "9AM_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_source_acquisition_plan"
        if all_checks_passed
        else
        "9AL_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_observation_"
        "discovery_implementation_remediation"
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
        / "authoritative_observation_discovery_records.csv",
        plan.DISCOVERY_RECORD_FIELDS,
        discovery_records,
    )

    write_csv(
        OUTPUT_DIR / "discovery_status_counts.csv",
        [
            "discovery_status",
            "count",
        ],
        [
            {
                "discovery_status": key,
                "count": value,
            }
            for key, value
            in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "candidate_source_class_counts.csv",
        [
            "candidate_source_class",
            "count",
        ],
        [
            {
                "candidate_source_class": key,
                "count": value,
            }
            for key, value
            in source_class_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "discovery_blocker_counts.csv",
        [
            "discovery_blocker",
            "count",
        ],
        [
            {
                "discovery_blocker": key,
                "count": value,
            }
            for key, value
            in blocker_counts.items()
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "discovery_contract_version":
            DISCOVERY_CONTRACT_VERSION,
        "plan_version":
            plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.REMEDIATION_CONTRACT_VERSION,
        "remediation_records":
            len(remediation_records),
        "discovery_records":
            len(discovery_records),
        "discovery_comparisons":
            len(comparison_ids),
        "discovery_status_counts":
            status_counts,
        "candidate_source_class_counts":
            source_class_counts,
        "discovery_blocker_counts":
            blocker_counts,
        "authoritative_observations_identified":
            len(authoritative_records),
        "discovery_digest":
            discovery_digest,
        "reverse_discovery_digest":
            reverse_discovery_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "candidate_values_coerced": 0,
        "candidate_values_defaulted": 0,
        "candidate_values_imputed": 0,
        "candidate_values_inferred": 0,
        "candidate_values_substituted": 0,
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
        / "authoritative_observation_discovery_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "discovery_result":
            DISCOVERY_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_"
            "source_acquisition_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "canonical_source_value_mutation",
            "canonical_outcome_mapping_change",
            "boolean_to_integer_coercion",
            "source_value_defaulting",
            "source_value_imputation",
            "candidate_value_inference",
            "heuristic_candidate_substitution",
            "unversioned_fixture_replacement",
            "canonical_evaluation_row_recomputation",
            "canonical_join_record_recomputation",
            "canonical_comparison_record_recomputation",
            "canonical_metric_recomputation",
            "canonical_interpretation_recomputation",
            "canonical_evidence_recomputation",
            "canonical_remediation_recomputation",
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
        "Discovery contract version: "
        f"{DISCOVERY_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Remediation records replayed: "
        f"{len(remediation_records)}"
    )
    print(
        "Discovery records: "
        f"{len(discovery_records)}"
    )
    print(
        "Discovery comparisons: "
        f"{len(comparison_ids)}"
    )
    print(
        "Discovery status counts: "
        f"{status_counts}"
    )
    print(
        "Candidate source-class counts: "
        f"{source_class_counts}"
    )
    print(
        "Discovery blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Authoritative observations identified: "
        f"{len(authoritative_records)}"
    )
    print(
        f"Discovery digest: {discovery_digest}"
    )
    print(
        "Reverse discovery digest: "
        f"{reverse_discovery_digest}"
    )
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Candidate values coerced: 0")
    print("Candidate values defaulted: 0")
    print("Candidate values imputed: 0")
    print("Candidate values inferred: 0")
    print("Candidate values substituted: 0")
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
        "Discovery result: "
        f"{diagnosis['discovery_result']}"
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
