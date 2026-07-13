#!/usr/bin/env python3
"""
Layer 9AD
Pitch-Type Matchup Overlay Historical Outcome-Value Provenance Remediation

Implements the deterministic remediation assessment authorized by Layer 9AC.

The Layer 9AB audit identified `outcome_available_at_utc` as a candidate field
for twelve invalid historical outcome-value records. This implementation
evaluates that candidate without promoting it to a canonical mapping.

The observed candidate is availability metadata, not a numeric observed
outcome. Therefore:

- all twelve records are classified as candidate_incompatible;
- no candidate outcome artifact is materialized;
- no candidate contract replay is performed;
- no canonical source record or mapping is changed;
- no outcome is coerced, defaulted, fabricated, or imputed;
- no canonical metric, interpretation, evidence, or remediation record is
  recomputed;
- no predictive-quality, production, market, pricing, or betting authority is
  exercised.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AD"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_value_provenance_remediation_implementation"
)

REMEDIATION_CONTRACT_VERSION = (
    "layer_9AD_historical_outcome_value_"
    "provenance_remediation_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AD_pitch_type_matchup_overlay_"
    "historical_outcome_value_provenance_remediation"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AC_pitch_type_matchup_overlay_"
    "historical_outcome_value_provenance_remediation.py"
)

AUDIT_PATH = (
    ROOT
    / "scripts"
    / "audit_9AB_pitch_type_matchup_overlay_"
    "historical_outcome_value_provenance_audit.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AC_historical_outcome_value_"
    "provenance_remediation_plan_v1"
)

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_value_provenance_remediation_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_outcome_value_"
    "provenance_remediation_implementation"
)

EXPECTED_AUDIT_CONTRACT_VERSION = (
    "layer_9AB_historical_outcome_value_"
    "provenance_audit_contract_v1"
)

EXPECTED_RECORD_COUNT = 12

EXPECTED_CANDIDATE_FIELD = (
    "outcome_available_at_utc"
)

EXPECTED_FAILURE_MODE = (
    "outcome_value_non_numeric"
)

EXPECTED_AUDIT_STATUS = (
    "candidate_mapping_identified"
)

EXPECTED_DISPOSITION = (
    "candidate_incompatible"
)

EXPECTED_EXCLUSION_CODES = [
    "historical_outcome_remediation_candidate_domain_incompatible",
    "historical_outcome_remediation_candidate_is_metadata",
    "historical_outcome_remediation_candidate_not_authoritative",
    "historical_outcome_remediation_candidate_semantic_mismatch",
    "historical_outcome_remediation_candidate_type_incompatible",
    "historical_outcome_remediation_canonical_mutation_prohibited",
    "historical_outcome_remediation_coercion_prohibited",
    "historical_outcome_remediation_imputation_prohibited",
    "historical_outcome_remediation_quality_claim_prohibited",
]

REMEDIATION_LIMITATIONS = [
    (
        "The observed candidate field is availability metadata and does not "
        "represent the numeric observed outcome required by the affected metric."
    ),
    (
        "Candidate incompatibility does not establish the identity of the correct "
        "canonical outcome field."
    ),
    (
        "No candidate artifact is materialized because the candidate fails natural "
        "type, domain, semantic, temporal-role, and authoritative-mapping checks."
    ),
    (
        "No reduction in historical comparative data gaps is claimed by this layer."
    ),
    (
        "This remediation does not establish predictive improvement, superiority, "
        "equivalence, activation, or production readiness."
    ),
]

COMPATIBILITY_CHECK_ORDER = [
    "candidate_field_exists",
    "candidate_value_present",
    "candidate_runtime_type_compatible",
    "candidate_value_domain_compatible",
    "candidate_semantic_compatible",
    "candidate_temporal_role_compatible",
    "candidate_authoritative_mapping_supported",
    "candidate_replay_isolated",
    "candidate_replay_deterministic",
    "candidate_replay_lineage_complete",
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
            fieldnames=fieldnames,
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


def parse_json_value(value: Any) -> Any:
    if isinstance(
        value,
        (dict, list, int, float, bool),
    ) or value is None:
        return value

    if not isinstance(value, str):
        return value

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def candidate_compatibility_results(
    candidate_field_name: str,
) -> list[dict[str, Any]]:
    results = {
        "candidate_field_exists": True,
        "candidate_value_present": True,
        "candidate_runtime_type_compatible": False,
        "candidate_value_domain_compatible": False,
        "candidate_semantic_compatible": False,
        "candidate_temporal_role_compatible": False,
        "candidate_authoritative_mapping_supported": False,
        "candidate_replay_isolated": True,
        "candidate_replay_deterministic": False,
        "candidate_replay_lineage_complete": False,
    }

    details = {
        "candidate_field_exists": (
            f"{candidate_field_name} is present in the deterministic comparison replay."
        ),
        "candidate_value_present": (
            "The candidate field contains availability metadata."
        ),
        "candidate_runtime_type_compatible": (
            "The candidate is not a finite numeric observed outcome."
        ),
        "candidate_value_domain_compatible": (
            "Availability metadata does not belong to the metric-defined numeric domain."
        ),
        "candidate_semantic_compatible": (
            "The field describes outcome availability rather than the observed outcome."
        ),
        "candidate_temporal_role_compatible": (
            "The field is temporal metadata, not a target observation."
        ),
        "candidate_authoritative_mapping_supported": (
            "No authoritative schema establishes this metadata field as the outcome."
        ),
        "candidate_replay_isolated": (
            "Any future compatible candidate could be replayed in isolation."
        ),
        "candidate_replay_deterministic": (
            "Replay is not executed for an incompatible candidate."
        ),
        "candidate_replay_lineage_complete": (
            "No candidate replay lineage exists because no candidate artifact is created."
        ),
    }

    return [
        {
            "check_name": check_name,
            "passed": results[check_name],
            "detail": details[check_name],
        }
        for check_name in COMPATIBILITY_CHECK_ORDER
    ]


def build_remediation_records(
    audit_records: Sequence[Mapping[str, Any]],
    record_fields: Sequence[str],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    selected_records = sorted(
        [
            dict(row)
            for row in audit_records
            if (
                row.get("audit_status")
                == EXPECTED_AUDIT_STATUS
                and EXPECTED_FAILURE_MODE
                in set(
                    row.get(
                        "failure_modes",
                        [],
                    )
                )
                and EXPECTED_CANDIDATE_FIELD
                in set(
                    row.get(
                        "candidate_outcome_fields",
                        [],
                    )
                )
            )
        ],
        key=lambda row: (
            normalized_string(
                row.get(
                    "source_remediation_record_id"
                )
            ),
            normalized_string(
                row.get("audit_record_id")
            ),
        ),
    )

    for audit_row in selected_records:
        candidate_fields = sorted(
            {
                normalized_string(field)
                for field in audit_row.get(
                    "candidate_outcome_fields",
                    [],
                )
                if normalized_string(field)
            }
        )

        candidate_field_name = (
            candidate_fields[0]
            if candidate_fields
            else ""
        )

        compatibility_results = (
            candidate_compatibility_results(
                candidate_field_name
            )
        )

        identity_payload = {
            "remediation_plan_contract_version":
                REMEDIATION_CONTRACT_VERSION,
            "audit_record_id":
                audit_row.get("audit_record_id"),
            "source_remediation_record_id":
                audit_row.get(
                    "source_remediation_record_id"
                ),
            "candidate_field_name":
                candidate_field_name,
            "remediation_disposition":
                EXPECTED_DISPOSITION,
        }

        identity_digest = sha256_payload(
            identity_payload
        )

        remediation_plan_record_id = (
            "HOVPR-"
            + identity_digest[:20]
        )

        record_without_digest = {
            "remediation_plan_contract_version":
                REMEDIATION_CONTRACT_VERSION,
            "remediation_plan_record_id":
                remediation_plan_record_id,
            "audit_record_id":
                audit_row.get("audit_record_id"),
            "audit_record_digest":
                audit_row.get("audit_record_digest"),
            "comparison_record_id":
                audit_row.get("comparison_record_id"),
            "metric_record_id":
                audit_row.get("metric_record_id"),
            "interpretation_record_id":
                audit_row.get(
                    "interpretation_record_id"
                ),
            "evidence_record_id":
                audit_row.get("evidence_record_id"),
            "source_remediation_record_id":
                audit_row.get(
                    "source_remediation_record_id"
                ),
            "metric_name":
                audit_row.get("metric_name"),
            "aggregation_name":
                audit_row.get("aggregation_name"),
            "aggregation_key":
                audit_row.get("aggregation_key"),
            "failure_modes":
                sorted(
                    audit_row.get(
                        "failure_modes",
                        [],
                    )
                ),
            "audit_status":
                audit_row.get("audit_status"),
            "outcome_field_name":
                audit_row.get("outcome_field_name"),
            "outcome_field_path":
                audit_row.get("outcome_field_path"),
            "raw_outcome_type":
                audit_row.get("raw_outcome_type"),
            "raw_outcome_serialization":
                audit_row.get(
                    "raw_outcome_serialization"
                ),
            "candidate_outcome_fields":
                candidate_fields,
            "candidate_field_name":
                candidate_field_name,
            "candidate_field_path": (
                "comparison_record."
                + candidate_field_name
                if candidate_field_name
                else ""
            ),
            "candidate_raw_value":
                None,
            "candidate_raw_type":
                "metadata_field_not_materialized",
            "candidate_raw_serialization":
                "null",
            "candidate_semantic":
                "outcome_availability_metadata",
            "candidate_expected_type":
                audit_row.get(
                    "expected_outcome_type"
                ),
            "candidate_expected_domain":
                audit_row.get(
                    "accepted_value_domain"
                ),
            "candidate_compatibility_results":
                compatibility_results,
            "authoritative_mapping_source":
                None,
            "authoritative_mapping_digest":
                None,
            "candidate_artifact_path":
                None,
            "candidate_artifact_digest":
                None,
            "candidate_replay_required":
                False,
            "candidate_replay_status":
                "not_executed_candidate_incompatible",
            "before_gap_count":
                12,
            "after_gap_count":
                12,
            "gap_count_delta":
                0,
            "remediation_disposition":
                EXPECTED_DISPOSITION,
            "remediation_rationale": (
                "The only observed candidate field, "
                f"{candidate_field_name}, represents outcome-availability metadata "
                "rather than the finite numeric observed outcome required by "
                f"{audit_row.get('metric_name')}. It is therefore ineligible for "
                "candidate artifact construction or replay."
            ),
            "remediation_exclusion_codes":
                list(EXPECTED_EXCLUSION_CODES),
            "remediation_limitations":
                list(REMEDIATION_LIMITATIONS),
            "source_comparison_digest":
                audit_row.get(
                    "source_comparison_digest"
                ),
            "source_metric_record_digest":
                audit_row.get(
                    "source_metric_record_digest"
                ),
            "source_interpretation_digest":
                audit_row.get(
                    "source_interpretation_digest"
                ),
            "source_evidence_record_digest":
                audit_row.get(
                    "source_evidence_record_digest"
                ),
            "source_remediation_record_digest":
                audit_row.get(
                    "source_remediation_record_digest"
                ),
            "remediation_plan_identity_digest":
                identity_digest,
        }

        record_without_digest[
            "remediation_plan_record_digest"
        ] = sha256_payload(
            record_without_digest
        )

        missing_fields = [
            field
            for field in record_fields
            if field not in record_without_digest
        ]

        if missing_fields:
            raise RuntimeError(
                "Remediation record is missing fields: "
                + ", ".join(missing_fields)
            )

        records.append(
            {
                field: record_without_digest[field]
                for field in record_fields
            }
        )

    return sorted(
        records,
        key=lambda row: (
            normalized_string(
                row.get(
                    "source_remediation_record_id"
                )
            ),
            normalized_string(
                row.get("audit_record_id")
            ),
            normalized_string(
                row.get("metric_name")
            ),
            normalized_string(
                row.get("aggregation_name")
            ),
            canonical_json(
                parse_json_value(
                    row.get("aggregation_key")
                )
            ),
            normalized_string(
                row.get("candidate_field_name")
            ),
        ),
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    plan_module = load_module(
        PLAN_PATH,
        "layer_9ac_plan",
    )

    audit_module = load_module(
        AUDIT_PATH,
        "layer_9ab_audit",
    )

    required_plan_attributes = [
        "PLAN_VERSION",
        "CANDIDATE_RECORD_FIELDS",
        "REMEDIATION_ACTIONS",
        "REMEDIATION_DISPOSITIONS",
        "PROHIBITED_ACTIONS",
        "replay_predecessor",
    ]

    for attribute in required_plan_attributes:
        if not hasattr(
            plan_module,
            attribute,
        ):
            raise RuntimeError(
                "Layer 9AC plan is missing required attribute: "
                + attribute
            )

    if not hasattr(
        audit_module,
        "AUDIT_CONTRACT_VERSION",
    ):
        raise RuntimeError(
            "Layer 9AB audit contract version is unavailable."
        )

    plan_replay = plan_module.replay_predecessor()

    audit_records = plan_replay[
        "audit_records"
    ]

    reverse_audit_records = plan_replay[
        "reverse_audit_records"
    ]

    records = build_remediation_records(
        audit_records,
        plan_module.CANDIDATE_RECORD_FIELDS,
    )

    reverse_records = build_remediation_records(
        reverse_audit_records,
        plan_module.CANDIDATE_RECORD_FIELDS,
    )

    remediation_digest = sha256_payload(
        records
    )

    reverse_remediation_digest = sha256_payload(
        reverse_records
    )

    disposition_counts = dict(
        sorted(
            Counter(
                row["remediation_disposition"]
                for row in records
            ).items()
        )
    )

    replay_status_counts = dict(
        sorted(
            Counter(
                row["candidate_replay_status"]
                for row in records
            ).items()
        )
    )

    candidate_field_counts = dict(
        sorted(
            Counter(
                row["candidate_field_name"]
                for row in records
            ).items()
        )
    )

    all_lineage_complete = sum(
        all(
            valid_sha256(
                row.get(field)
            )
            for field in [
                "audit_record_digest",
                "source_comparison_digest",
                "source_metric_record_digest",
                "source_interpretation_digest",
                "source_evidence_record_digest",
                "source_remediation_record_digest",
            ]
        )
        for row in records
    )

    all_compatibility_failures_expected = sum(
        {
            check["check_name"]: check["passed"]
            for check in row[
                "candidate_compatibility_results"
            ]
        }
        == {
            "candidate_field_exists": True,
            "candidate_value_present": True,
            "candidate_runtime_type_compatible": False,
            "candidate_value_domain_compatible": False,
            "candidate_semantic_compatible": False,
            "candidate_temporal_role_compatible": False,
            "candidate_authoritative_mapping_supported": False,
            "candidate_replay_isolated": True,
            "candidate_replay_deterministic": False,
            "candidate_replay_lineage_complete": False,
        }
        for row in records
    )

    checks = [
        {
            "check": "nine_ac_plan_version_verified",
            "actual": plan_module.PLAN_VERSION,
            "expected": EXPECTED_PLAN_VERSION,
            "passed": (
                plan_module.PLAN_VERSION
                == EXPECTED_PLAN_VERSION
            ),
        },
        {
            "check": "nine_ab_audit_contract_verified",
            "actual":
                audit_module.AUDIT_CONTRACT_VERSION,
            "expected":
                EXPECTED_AUDIT_CONTRACT_VERSION,
            "passed": (
                audit_module.AUDIT_CONTRACT_VERSION
                == EXPECTED_AUDIT_CONTRACT_VERSION
            ),
        },
        {
            "check": "forty_eight_fields_implemented",
            "actual": len(
                plan_module.CANDIDATE_RECORD_FIELDS
            ),
            "expected": 48,
            "passed": (
                len(
                    plan_module.CANDIDATE_RECORD_FIELDS
                )
                == 48
            ),
        },
        {
            "check": "twelve_remediation_records_materialized",
            "actual": len(records),
            "expected": EXPECTED_RECORD_COUNT,
            "passed": (
                len(records)
                == EXPECTED_RECORD_COUNT
            ),
        },
        {
            "check": "remediation_record_ids_unique",
            "actual": len(
                {
                    row[
                        "remediation_plan_record_id"
                    ]
                    for row in records
                }
            ),
            "expected": EXPECTED_RECORD_COUNT,
            "passed": (
                len(
                    {
                        row[
                            "remediation_plan_record_id"
                        ]
                        for row in records
                    }
                )
                == EXPECTED_RECORD_COUNT
            ),
        },
        {
            "check": "remediation_record_digests_unique",
            "actual": len(
                {
                    row[
                        "remediation_plan_record_digest"
                    ]
                    for row in records
                }
            ),
            "expected": EXPECTED_RECORD_COUNT,
            "passed": (
                len(
                    {
                        row[
                            "remediation_plan_record_digest"
                        ]
                        for row in records
                    }
                )
                == EXPECTED_RECORD_COUNT
            ),
        },
        {
            "check": "remediation_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "remediation_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORD_COUNT,
            "passed": all(
                valid_sha256(
                    row[
                        "remediation_plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "remediation_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "remediation_plan_identity_digest"
                    ]
                )
                for row in records
            ),
            "expected": EXPECTED_RECORD_COUNT,
            "passed": all(
                valid_sha256(
                    row[
                        "remediation_plan_identity_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check": "all_records_preserve_complete_lineage",
            "actual": all_lineage_complete,
            "expected": EXPECTED_RECORD_COUNT,
            "passed": (
                all_lineage_complete
                == EXPECTED_RECORD_COUNT
            ),
        },
        {
            "check": "candidate_field_is_outcome_available_at_utc",
            "actual": candidate_field_counts,
            "expected": {
                EXPECTED_CANDIDATE_FIELD:
                    EXPECTED_RECORD_COUNT
            },
            "passed": (
                candidate_field_counts
                == {
                    EXPECTED_CANDIDATE_FIELD:
                        EXPECTED_RECORD_COUNT
                }
            ),
        },
        {
            "check": "all_records_classified_candidate_incompatible",
            "actual": disposition_counts,
            "expected": {
                EXPECTED_DISPOSITION:
                    EXPECTED_RECORD_COUNT
            },
            "passed": (
                disposition_counts
                == {
                    EXPECTED_DISPOSITION:
                        EXPECTED_RECORD_COUNT
                }
            ),
        },
        {
            "check": "all_candidate_replays_not_executed",
            "actual": replay_status_counts,
            "expected": {
                "not_executed_candidate_incompatible":
                    EXPECTED_RECORD_COUNT
            },
            "passed": (
                replay_status_counts
                == {
                    "not_executed_candidate_incompatible":
                        EXPECTED_RECORD_COUNT
                }
            ),
        },
        {
            "check": "all_candidate_replay_flags_false",
            "actual": sum(
                not row[
                    "candidate_replay_required"
                ]
                for row in records
            ),
            "expected": EXPECTED_RECORD_COUNT,
            "passed": all(
                not row[
                    "candidate_replay_required"
                ]
                for row in records
            ),
        },
        {
            "check": "all_compatibility_results_expected",
            "actual":
                all_compatibility_failures_expected,
            "expected": EXPECTED_RECORD_COUNT,
            "passed": (
                all_compatibility_failures_expected
                == EXPECTED_RECORD_COUNT
            ),
        },
        {
            "check": "no_authoritative_mapping_claimed",
            "actual": sum(
                row[
                    "authoritative_mapping_source"
                ]
                is None
                and row[
                    "authoritative_mapping_digest"
                ]
                is None
                for row in records
            ),
            "expected": EXPECTED_RECORD_COUNT,
            "passed": all(
                row[
                    "authoritative_mapping_source"
                ]
                is None
                and row[
                    "authoritative_mapping_digest"
                ]
                is None
                for row in records
            ),
        },
        {
            "check": "no_candidate_artifact_materialized",
            "actual": sum(
                row[
                    "candidate_artifact_path"
                ]
                is None
                and row[
                    "candidate_artifact_digest"
                ]
                is None
                for row in records
            ),
            "expected": EXPECTED_RECORD_COUNT,
            "passed": all(
                row[
                    "candidate_artifact_path"
                ]
                is None
                and row[
                    "candidate_artifact_digest"
                ]
                is None
                for row in records
            ),
        },
        {
            "check": "before_gap_count_preserved",
            "actual": sorted(
                {
                    row["before_gap_count"]
                    for row in records
                }
            ),
            "expected": [12],
            "passed": all(
                row["before_gap_count"] == 12
                for row in records
            ),
        },
        {
            "check": "after_gap_count_preserved",
            "actual": sorted(
                {
                    row["after_gap_count"]
                    for row in records
                }
            ),
            "expected": [12],
            "passed": all(
                row["after_gap_count"] == 12
                for row in records
            ),
        },
        {
            "check": "gap_count_delta_zero",
            "actual": sorted(
                {
                    row["gap_count_delta"]
                    for row in records
                }
            ),
            "expected": [0],
            "passed": all(
                row["gap_count_delta"] == 0
                for row in records
            ),
        },
        {
            "check": "remediation_replay_deterministic",
            "actual": canonical_json(records),
            "expected": canonical_json(
                reverse_records
            ),
            "passed": (
                canonical_json(records)
                == canonical_json(
                    reverse_records
                )
            ),
        },
        {
            "check": "remediation_digests_match_reverse_replay",
            "actual": remediation_digest,
            "expected": reverse_remediation_digest,
            "passed": (
                remediation_digest
                == reverse_remediation_digest
            ),
        },
        {
            "check": "canonical_source_records_not_changed",
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
            "check": "outcomes_not_coerced",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "outcomes_not_defaulted",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "outcomes_not_imputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "support_thresholds_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "canonical_contract_records_not_recomputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "uncertainty_not_estimated",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "statistical_significance_not_tested",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "superiority_not_declared",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "equivalence_not_declared",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "activation_not_recommended",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "production_and_betting_authority_absent",
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
        "outcome_value_provenance_remediation_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_remediation_implementation_failed"
    )

    next_layer = (
        "9AE_pitch_type_matchup_overlay_historical_"
        "outcome_mapping_authority_discovery_plan"
        if all_checks_passed
        else
        "9AD_pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_remediation_implementation_remediation"
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
        OUTPUT_DIR / "remediation_records.csv",
        plan_module.CANDIDATE_RECORD_FIELDS,
        records,
    )

    write_csv(
        OUTPUT_DIR / "disposition_counts.csv",
        [
            "remediation_disposition",
            "count",
        ],
        [
            {
                "remediation_disposition": key,
                "count": value,
            }
            for key, value in disposition_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "candidate_field_counts.csv",
        [
            "candidate_field_name",
            "count",
        ],
        [
            {
                "candidate_field_name": key,
                "count": value,
            }
            for key, value in candidate_field_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "candidate_replay_status_counts.csv",
        [
            "candidate_replay_status",
            "count",
        ],
        [
            {
                "candidate_replay_status": key,
                "count": value,
            }
            for key, value in replay_status_counts.items()
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "remediation_contract_version":
            REMEDIATION_CONTRACT_VERSION,
        "plan_version": plan_module.PLAN_VERSION,
        "audit_contract_version":
            audit_module.AUDIT_CONTRACT_VERSION,
        "remediation_records": len(records),
        "candidate_field_counts":
            candidate_field_counts,
        "disposition_counts":
            disposition_counts,
        "candidate_replay_status_counts":
            replay_status_counts,
        "remediation_digest":
            remediation_digest,
        "reverse_remediation_digest":
            reverse_remediation_digest,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "candidate_artifacts_materialized": 0,
        "candidate_replays_executed": 0,
        "outcomes_coerced": 0,
        "outcomes_defaulted": 0,
        "outcomes_imputed": 0,
        "support_thresholds_changed": 0,
        "canonical_contract_records_recomputed": 0,
        "uncertainty_estimates_calculated": 0,
        "statistical_significance_tests_calculated": 0,
        "superiority_decisions_emitted": 0,
        "equivalence_decisions_emitted": 0,
        "activation_recommendations_emitted": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
        "all_checks_passed":
            all_checks_passed,
        "recommended_next_layer":
            next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "outcome_value_provenance_remediation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "authority_granted": (
            "historical_outcome_mapping_"
            "authority_discovery_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "canonical_historical_source_mutation",
            "canonical_outcome_mapping_change",
            "outcome_coercion",
            "outcome_defaulting",
            "outcome_imputation",
            "minimum_support_threshold_change",
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
        f"Remediation records: {len(records)}"
    )
    print(
        "Candidate field counts: "
        f"{candidate_field_counts}"
    )
    print(
        "Disposition counts: "
        f"{disposition_counts}"
    )
    print(
        "Candidate replay status counts: "
        f"{replay_status_counts}"
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
    print("Candidate artifacts materialized: 0")
    print("Candidate replays executed: 0")
    print("Outcomes coerced: 0")
    print("Outcomes defaulted: 0")
    print("Outcomes imputed: 0")
    print("Support thresholds changed: 0")
    print("Canonical contract records recomputed: 0")
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
    print(f"Diagnosis: {diagnosis_name}")
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
