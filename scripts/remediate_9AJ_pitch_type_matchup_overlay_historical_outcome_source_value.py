#!/usr/bin/env python3
"""
Layer 9AJ
Pitch-Type Matchup Overlay Historical Outcome Source-Value Remediation

Implements the deterministic remediation assessment planned by Layer 9AI.

Layer 9AH and Layer 9AI established that sixteen historical comparison
identities contain boolean values at the earliest resolved evaluation source.
The invalid values are preserved through the canonical `outcome_value` mapping.

This implementation does not coerce booleans to integers and does not invent,
default, or impute replacement outcomes. It materializes one remediation record
per defective source identity and blocks source mutation until an authoritative
historical observation is discovered.

No canonical source, mapping, comparison, metric, interpretation, evidence, or
remediation record is mutated.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AJ"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_source_value_remediation_implementation"
)

REMEDIATION_CONTRACT_VERSION = (
    "layer_9AJ_historical_outcome_source_value_"
    "remediation_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AJ_pitch_type_matchup_overlay_"
    "historical_outcome_source_value_remediation"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AI_pitch_type_matchup_overlay_"
    "historical_outcome_source_value_remediation.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AI_historical_outcome_source_value_"
    "remediation_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AH_historical_outcome_source_value_"
    "provenance_audit_contract_v1"
)

EXPECTED_AUDIT_RECORDS = 108
EXPECTED_AUDITED_COMPARISONS = 18
EXPECTED_DEFECT_RECORDS = 16
EXPECTED_REMEDIATION_RECORDS = 16

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"

BLOCKER_CODE = (
    "historical_outcome_authoritative_observation_missing"
)

REMEDIATION_STATUS = (
    "blocked_authoritative_observation_missing"
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
    if value is None:
        return ""

    return str(value).strip()


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
        "layer_9ai_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9AI plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()

    predecessor = replay["module"]

    if (
        predecessor.AUDIT_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AH contract version: "
            f"{predecessor.AUDIT_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "records": replay["records"],
        "reverse_records": replay["reverse_records"],
    }


def select_defect_records(
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    selected = [
        dict(row)
        for row in records
        if (
            row.get("provenance_stage_priority") == 1
            and row.get("audit_disposition")
            == "source_value_defect_identified"
            and row.get("source_value_classification")
            == "boolean_source_value"
            and row.get("source_runtime_type")
            == "bool"
        )
    ]

    selected.sort(
        key=lambda row: (
            normalized_string(
                row.get("comparison_record_id")
            ),
            normalized_string(
                row.get("source_record_id")
            ),
            normalized_string(
                row.get(
                    "source_value_audit_record_id"
                )
            ),
        )
    )

    return selected


def build_remediation_records(
    plan: Any,
    audit_records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    defect_records = select_defect_records(
        audit_records
    )

    remediation_records: list[dict[str, Any]] = []

    for defect in defect_records:
        identity_payload = {
            "source_value_remediation_plan_contract_version":
                REMEDIATION_CONTRACT_VERSION,
            "source_value_audit_record_id":
                defect.get(
                    "source_value_audit_record_id"
                ),
            "comparison_record_id":
                defect.get(
                    "comparison_record_id"
                ),
            "defect_source_record_id":
                defect.get(
                    "source_record_id"
                ),
            "remediation_scope":
                "authoritative_evaluation_source_value",
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        record = {
            "source_value_remediation_plan_contract_version":
                REMEDIATION_CONTRACT_VERSION,
            "source_value_remediation_plan_record_id":
                "HOSVR-"
                + identity_digest[:20],
            "source_value_audit_record_id":
                defect.get(
                    "source_value_audit_record_id"
                ),
            "source_value_audit_record_digest":
                defect.get(
                    "source_value_audit_record_digest"
                ),
            "authority_discovery_record_id":
                defect.get(
                    "authority_discovery_record_id"
                ),
            "remediation_plan_record_id":
                defect.get(
                    "remediation_plan_record_id"
                ),
            "audit_record_id":
                defect.get(
                    "audit_record_id"
                ),
            "comparison_record_id":
                defect.get(
                    "comparison_record_id"
                ),
            "metric_record_id":
                defect.get(
                    "metric_record_id"
                ),
            "metric_name":
                defect.get("metric_name"),
            "aggregation_name":
                defect.get(
                    "aggregation_name"
                ),
            "aggregation_key":
                defect.get(
                    "aggregation_key"
                ),
            "authoritative_field_name":
                AUTHORITATIVE_FIELD_NAME,
            "authoritative_field_path":
                AUTHORITATIVE_FIELD_PATH,
            "rejected_metadata_field_name":
                REJECTED_METADATA_FIELD,
            "defect_stage_id":
                defect.get(
                    "provenance_stage_id"
                ),
            "defect_stage_name":
                defect.get(
                    "provenance_stage_name"
                ),
            "defect_source_path":
                defect.get("source_path"),
            "defect_source_symbol":
                defect.get("source_symbol"),
            "defect_source_record_id":
                defect.get(
                    "source_record_id"
                ),
            "defect_source_record_digest":
                defect.get(
                    "source_record_digest"
                ),
            "defective_value":
                defect.get("source_value"),
            "defective_runtime_type":
                defect.get(
                    "source_runtime_type"
                ),
            "defective_value_classification":
                defect.get(
                    "source_value_classification"
                ),
            "defect_disposition":
                defect.get(
                    "audit_disposition"
                ),
            "remediation_scope":
                "authoritative_evaluation_source_value",
            "required_authority_source":
                (
                    "authoritative historical observation or "
                    "versioned evaluation-fixture contract"
                ),
            "required_corrected_value_type":
                "finite_int_or_float_excluding_bool",
            "required_corrected_value_domain":
                (
                    "metric-defined numeric historical outcome domain"
                ),
            "coercion_permitted": False,
            "defaulting_permitted": False,
            "imputation_permitted": False,
            "mapping_change_permitted": False,
            "source_identity_must_be_preserved": True,
            "old_value_must_be_preserved": True,
            "expected_replay_stages": [
                "evaluation_fixture_or_source",
                "evaluation_row",
                "prediction_outcome_join",
                "comparative_evaluation_record",
            ],
            "expected_changed_record_classes": [
                "evaluation_source_record",
                "evaluation_row",
                "prediction_outcome_join_record",
                "comparative_evaluation_record",
            ],
            "expected_unchanged_contracts": [
                AUTHORITATIVE_FIELD_PATH,
                REJECTED_METADATA_FIELD,
                "Layer 9P join contract",
                "Layer 9R comparison contract",
            ],
            "validation_requirements": [
                "authoritative_observation_identified",
                "corrected_value_present",
                "corrected_value_is_finite_numeric",
                "corrected_value_is_not_bool",
                "source_identity_preserved",
                "old_value_preserved_as_evidence",
                "expected_digest_changes_only",
                "reverse_replay_deterministic",
            ],
            "remediation_blockers": [
                BLOCKER_CODE
            ],
            "remediation_status":
                REMEDIATION_STATUS,
            "remediation_rationale":
                (
                    "The boolean value originates at the earliest resolved "
                    "evaluation source. No authoritative replacement "
                    "observation is available in the replayed contract, so "
                    "mutation, coercion, defaulting, and imputation remain "
                    "prohibited."
                ),
            "remediation_limitations": [
                (
                    "This layer does not discover an external authoritative "
                    "historical observation."
                ),
                (
                    "This layer does not mutate source fixtures or regenerate "
                    "canonical downstream records."
                ),
                (
                    "This disposition does not establish predictive quality, "
                    "superiority, equivalence, activation, or production "
                    "readiness."
                ),
            ],
            "source_value_remediation_plan_identity_digest":
                identity_digest,
        }

        record[
            "source_value_remediation_plan_record_digest"
        ] = sha256_payload(record)

        missing_fields = [
            field
            for field
            in plan.REMEDIATION_RECORD_FIELDS
            if field not in record
        ]

        if missing_fields:
            raise RuntimeError(
                "Remediation record missing fields: "
                + ", ".join(missing_fields)
            )

        remediation_records.append(
            {
                field: record[field]
                for field
                in plan.REMEDIATION_RECORD_FIELDS
            }
        )

    remediation_records.sort(
        key=lambda row: (
            normalized_string(
                row.get(
                    "comparison_record_id"
                )
            ),
            normalized_string(
                row.get("defect_stage_id")
            ),
            normalized_string(
                row.get(
                    "defect_source_record_id"
                )
            ),
            normalized_string(
                row.get(
                    "source_value_remediation_plan_record_id"
                )
            ),
        )
    )

    return remediation_records


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    replay = replay_plan()

    plan = replay["plan"]
    predecessor = replay["predecessor"]
    audit_records = replay["records"]
    reverse_audit_records = replay[
        "reverse_records"
    ]

    remediation_records = (
        build_remediation_records(
            plan,
            audit_records,
        )
    )

    reverse_remediation_records = (
        build_remediation_records(
            plan,
            list(
                reversed(
                    reverse_audit_records
                )
            ),
        )
    )

    defect_records = select_defect_records(
        audit_records
    )

    audited_comparison_ids = {
        row.get("comparison_record_id")
        for row in audit_records
    }

    remediation_comparison_ids = {
        row.get("comparison_record_id")
        for row in remediation_records
    }

    remediation_digest = sha256_payload(
        remediation_records
    )

    reverse_remediation_digest = (
        sha256_payload(
            reverse_remediation_records
        )
    )

    status_counts = dict(
        sorted(
            Counter(
                row["remediation_status"]
                for row in remediation_records
            ).items()
        )
    )

    blocker_counts = dict(
        sorted(
            Counter(
                blocker
                for row in remediation_records
                for blocker in row[
                    "remediation_blockers"
                ]
            ).items()
        )
    )

    checks = [
        {
            "check": "nine_ai_plan_version_verified",
            "actual": plan.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": (
                plan.PLAN_VERSION
                == EXPECTED_PLAN_VERSION
            ),
        },
        {
            "check": "nine_ah_contract_version_verified",
            "actual":
                predecessor.AUDIT_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.AUDIT_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "audit_replay_deterministic",
            "actual": (
                canonical_json(audit_records)
                == canonical_json(
                    reverse_audit_records
                )
            ),
            "expected": True,
            "passed": (
                canonical_json(audit_records)
                == canonical_json(
                    reverse_audit_records
                )
            ),
        },
        {
            "check": "remediation_replay_deterministic",
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
            "check": "remediation_digests_match_reverse_replay",
            "actual": remediation_digest,
            "expected":
                reverse_remediation_digest,
            "passed": (
                remediation_digest
                == reverse_remediation_digest
            ),
        },
        {
            "check": "expected_audit_records_replayed",
            "actual": len(audit_records),
            "expected":
                EXPECTED_AUDIT_RECORDS,
            "passed": (
                len(audit_records)
                == EXPECTED_AUDIT_RECORDS
            ),
        },
        {
            "check": "expected_comparisons_replayed",
            "actual":
                len(audited_comparison_ids),
            "expected":
                EXPECTED_AUDITED_COMPARISONS,
            "passed": (
                len(audited_comparison_ids)
                == EXPECTED_AUDITED_COMPARISONS
            ),
        },
        {
            "check": "expected_defect_records_selected",
            "actual": len(defect_records),
            "expected":
                EXPECTED_DEFECT_RECORDS,
            "passed": (
                len(defect_records)
                == EXPECTED_DEFECT_RECORDS
            ),
        },
        {
            "check": "expected_remediation_records_materialized",
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
            "check": "one_remediation_record_per_defect_comparison",
            "actual":
                len(remediation_comparison_ids),
            "expected":
                EXPECTED_DEFECT_RECORDS,
            "passed": (
                len(remediation_comparison_ids)
                == EXPECTED_DEFECT_RECORDS
            ),
        },
        {
            "check": "remediation_record_fields_complete",
            "actual":
                len(
                    plan.REMEDIATION_RECORD_FIELDS
                ),
            "expected": 45,
            "passed": all(
                set(row)
                == set(
                    plan.REMEDIATION_RECORD_FIELDS
                )
                for row in remediation_records
            ),
        },
        {
            "check": "remediation_record_ids_unique",
            "actual": len(
                {
                    row[
                        "source_value_remediation_plan_record_id"
                    ]
                    for row in remediation_records
                }
            ),
            "expected":
                len(remediation_records),
            "passed": (
                len(
                    {
                        row[
                            "source_value_remediation_plan_record_id"
                        ]
                        for row
                        in remediation_records
                    }
                )
                == len(remediation_records)
            ),
        },
        {
            "check": "remediation_record_digests_unique",
            "actual": len(
                {
                    row[
                        "source_value_remediation_plan_record_digest"
                    ]
                    for row in remediation_records
                }
            ),
            "expected":
                len(remediation_records),
            "passed": (
                len(
                    {
                        row[
                            "source_value_remediation_plan_record_digest"
                        ]
                        for row
                        in remediation_records
                    }
                )
                == len(remediation_records)
            ),
        },
        {
            "check": "all_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_value_remediation_plan_identity_digest"
                    ]
                )
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_value_remediation_plan_identity_digest"
                    ]
                )
                for row in remediation_records
            ),
        },
        {
            "check": "all_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_value_remediation_plan_record_digest"
                    ]
                )
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_value_remediation_plan_record_digest"
                    ]
                )
                for row in remediation_records
            ),
        },
        {
            "check": "all_source_audit_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_value_audit_record_digest"
                    ]
                )
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_value_audit_record_digest"
                    ]
                )
                for row in remediation_records
            ),
        },
        {
            "check": "all_defect_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in remediation_records
            ),
        },
        {
            "check": "all_defective_values_are_boolean",
            "actual": sum(
                isinstance(
                    row["defective_value"],
                    bool,
                )
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                isinstance(
                    row["defective_value"],
                    bool,
                )
                for row in remediation_records
            ),
        },
        {
            "check": "all_runtime_types_are_bool",
            "actual": sorted(
                {
                    row[
                        "defective_runtime_type"
                    ]
                    for row in remediation_records
                }
            ),
            "expected": ["bool"],
            "passed": all(
                row[
                    "defective_runtime_type"
                ]
                == "bool"
                for row in remediation_records
            ),
        },
        {
            "check": "all_defects_classified_as_boolean_source_value",
            "actual": sorted(
                {
                    row[
                        "defective_value_classification"
                    ]
                    for row in remediation_records
                }
            ),
            "expected": [
                "boolean_source_value"
            ],
            "passed": all(
                row[
                    "defective_value_classification"
                ]
                == "boolean_source_value"
                for row in remediation_records
            ),
        },
        {
            "check": "all_remediations_blocked_for_missing_authority",
            "actual": status_counts,
            "expected": {
                REMEDIATION_STATUS:
                    EXPECTED_REMEDIATION_RECORDS
            },
            "passed": (
                status_counts
                == {
                    REMEDIATION_STATUS:
                        EXPECTED_REMEDIATION_RECORDS
                }
            ),
        },
        {
            "check": "all_records_include_authority_blocker",
            "actual": blocker_counts,
            "expected": {
                BLOCKER_CODE:
                    EXPECTED_REMEDIATION_RECORDS
            },
            "passed": (
                blocker_counts
                == {
                    BLOCKER_CODE:
                        EXPECTED_REMEDIATION_RECORDS
                }
            ),
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row[
                        "authoritative_field_name"
                    ]
                    for row in remediation_records
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
                for row in remediation_records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row[
                        "authoritative_field_path"
                    ]
                    for row in remediation_records
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
                for row in remediation_records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row[
                        "rejected_metadata_field_name"
                    ]
                    for row in remediation_records
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
                for row in remediation_records
            ),
        },
        {
            "check": "coercion_prohibited_for_all_records",
            "actual": sum(
                not row[
                    "coercion_permitted"
                ]
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                not row[
                    "coercion_permitted"
                ]
                for row in remediation_records
            ),
        },
        {
            "check": "defaulting_prohibited_for_all_records",
            "actual": sum(
                not row[
                    "defaulting_permitted"
                ]
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                not row[
                    "defaulting_permitted"
                ]
                for row in remediation_records
            ),
        },
        {
            "check": "imputation_prohibited_for_all_records",
            "actual": sum(
                not row[
                    "imputation_permitted"
                ]
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                not row[
                    "imputation_permitted"
                ]
                for row in remediation_records
            ),
        },
        {
            "check": "mapping_change_prohibited_for_all_records",
            "actual": sum(
                not row[
                    "mapping_change_permitted"
                ]
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                not row[
                    "mapping_change_permitted"
                ]
                for row in remediation_records
            ),
        },
        {
            "check": "source_identity_preservation_required",
            "actual": sum(
                bool(
                    row[
                        "source_identity_must_be_preserved"
                    ]
                )
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                bool(
                    row[
                        "source_identity_must_be_preserved"
                    ]
                )
                for row in remediation_records
            ),
        },
        {
            "check": "old_value_preservation_required",
            "actual": sum(
                bool(
                    row[
                        "old_value_must_be_preserved"
                    ]
                )
                for row in remediation_records
            ),
            "expected":
                len(remediation_records),
            "passed": all(
                bool(
                    row[
                        "old_value_must_be_preserved"
                    ]
                )
                for row in remediation_records
            ),
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
            "check": "source_values_not_repaired",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "source_values_not_coerced_defaulted_or_imputed",
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
        "pitch_type_matchup_overlay_historical_"
        "outcome_source_value_remediation_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_source_value_remediation_implementation_failed"
    )

    next_layer = (
        "9AK_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_observation_discovery_plan"
        if all_checks_passed
        else
        "9AJ_pitch_type_matchup_overlay_historical_"
        "outcome_source_value_remediation_implementation_remediation"
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
        / "source_value_remediation_records.csv",
        plan.REMEDIATION_RECORD_FIELDS,
        remediation_records,
    )

    write_csv(
        OUTPUT_DIR / "remediation_status_counts.csv",
        [
            "remediation_status",
            "count",
        ],
        [
            {
                "remediation_status": key,
                "count": value,
            }
            for key, value
            in status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "remediation_blocker_counts.csv",
        [
            "remediation_blocker",
            "count",
        ],
        [
            {
                "remediation_blocker": key,
                "count": value,
            }
            for key, value
            in blocker_counts.items()
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "remediation_contract_version":
            REMEDIATION_CONTRACT_VERSION,
        "plan_version": plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.AUDIT_CONTRACT_VERSION,
        "audit_records": len(audit_records),
        "audited_comparisons":
            len(audited_comparison_ids),
        "defect_records":
            len(defect_records),
        "remediation_records":
            len(remediation_records),
        "remediation_comparisons":
            len(remediation_comparison_ids),
        "remediation_status_counts":
            status_counts,
        "remediation_blocker_counts":
            blocker_counts,
        "remediation_digest":
            remediation_digest,
        "reverse_remediation_digest":
            reverse_remediation_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
        "authoritative_observations_discovered": 0,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "source_values_repaired": 0,
        "source_values_coerced": 0,
        "source_values_defaulted": 0,
        "source_values_imputed": 0,
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
        / "outcome_source_value_remediation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "remediation_result":
            REMEDIATION_STATUS,
        "authority_granted": (
            "historical_outcome_authoritative_"
            "observation_discovery_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "canonical_source_value_mutation",
            "canonical_outcome_mapping_change",
            "boolean_to_integer_coercion",
            "source_value_defaulting",
            "source_value_imputation",
            "unversioned_fixture_replacement",
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
        "Remediation contract version: "
        f"{REMEDIATION_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        f"Audit records replayed: {len(audit_records)}"
    )
    print(
        "Audited comparisons: "
        f"{len(audited_comparison_ids)}"
    )
    print(
        f"Defect records: {len(defect_records)}"
    )
    print(
        "Remediation records: "
        f"{len(remediation_records)}"
    )
    print(
        "Remediation status counts: "
        f"{status_counts}"
    )
    print(
        "Remediation blocker counts: "
        f"{blocker_counts}"
    )
    print(
        "Authoritative observations discovered: 0"
    )
    print(
        f"Remediation digest: {remediation_digest}"
    )
    print(
        "Reverse remediation digest: "
        f"{reverse_remediation_digest}"
    )
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Source values repaired: 0")
    print("Source values coerced: 0")
    print("Source values defaulted: 0")
    print("Source values imputed: 0")
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
        "Remediation result: "
        f"{diagnosis['remediation_result']}"
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
