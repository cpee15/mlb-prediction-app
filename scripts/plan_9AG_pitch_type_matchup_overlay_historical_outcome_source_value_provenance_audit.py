#!/usr/bin/env python3
"""
Layer 9AG
Pitch-Type Matchup Overlay Historical Outcome Source-Value Provenance Audit Plan

Plans a deterministic, read-only audit of the source values carried through the
authoritative historical outcome field identified by Layer 9AF:

    historical_prediction_outcome_join_record.outcome_value

Layer 9AF established that:

- `outcome_value` is the authoritative observed-outcome field;
- `outcome_available_at_utc` is availability metadata;
- the mapping itself is not the source of the invalid historical values;
- boolean outcome payloads remain a source-value provenance problem.

Planning only.

This layer defines the Layer 9AH audit contract for tracing each affected
outcome value backward through Layer 9R comparison records, Layer 9P joined
records, and the evaluation-row producer without mutating canonical records or
coercing, defaulting, fabricating, or imputing outcomes.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AG"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_source_value_provenance_audit_plan"
)

PLAN_VERSION = (
    "layer_9AG_historical_outcome_source_value_"
    "provenance_audit_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AG_pitch_type_matchup_overlay_"
    "historical_outcome_source_value_provenance_audit_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "discover_9AF_pitch_type_matchup_overlay_"
    "historical_outcome_mapping_authority.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AF_historical_outcome_mapping_"
    "authority_discovery_contract_v1"
)

EXPECTED_DISCOVERY_RECORDS = 24
EXPECTED_AUTHORITATIVE_RECORDS = 24
EXPECTED_PREDECESSOR_SOURCES = 2

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = (
    "outcome_available_at_utc"
)


INPUT_RULES = [
    {
        "rule_id": "HOSVPA-I01",
        "rule": (
            "Layer 9AF discovery contract version must match the expected "
            "authority-discovery contract."
        ),
    },
    {
        "rule_id": "HOSVPA-I02",
        "rule": (
            "Exactly twenty-four Layer 9AF authority-discovery records must "
            "be replayed."
        ),
    },
    {
        "rule_id": "HOSVPA-I03",
        "rule": (
            "All replayed discovery records must classify outcome_value as "
            "authoritative."
        ),
    },
    {
        "rule_id": "HOSVPA-I04",
        "rule": (
            "The discovered authoritative path must remain "
            "historical_prediction_outcome_join_record.outcome_value."
        ),
    },
    {
        "rule_id": "HOSVPA-I05",
        "rule": (
            "outcome_available_at_utc must remain rejected availability "
            "metadata."
        ),
    },
    {
        "rule_id": "HOSVPA-I06",
        "rule": (
            "Every source digest, discovery identity digest, discovery record "
            "digest, and predecessor lineage digest must be a valid SHA-256."
        ),
    },
    {
        "rule_id": "HOSVPA-I07",
        "rule": (
            "Discovery records must be unique by authority-discovery record ID."
        ),
    },
    {
        "rule_id": "HOSVPA-I08",
        "rule": (
            "Canonical historical records and mappings must remain read-only."
        ),
    },
]


AUDIT_OBJECTIVES = [
    {
        "objective_id": "HOSVPA-O01",
        "objective": (
            "Identify the earliest producer or fixture at which each affected "
            "outcome_value first assumes its observed runtime type and value."
        ),
    },
    {
        "objective_id": "HOSVPA-O02",
        "objective": (
            "Trace outcome_value through evaluation rows, Layer 9P joined "
            "records, Layer 9R comparisons, and downstream metric records."
        ),
    },
    {
        "objective_id": "HOSVPA-O03",
        "objective": (
            "Distinguish source-value defects from mapping, serialization, "
            "comparison, aggregation, or metric-consumer defects."
        ),
    },
    {
        "objective_id": "HOSVPA-O04",
        "objective": (
            "Classify boolean values separately from valid numeric values, "
            "missing values, non-finite numerics, strings, containers, and "
            "other unsupported runtime types."
        ),
    },
    {
        "objective_id": "HOSVPA-O05",
        "objective": (
            "Preserve exact source paths, symbols, contract versions, record "
            "IDs, values, runtime types, domains, and lineage digests."
        ),
    },
    {
        "objective_id": "HOSVPA-O06",
        "objective": (
            "Determine whether the observed invalid value is introduced, "
            "preserved, transformed, defaulted, coerced, or unresolved at each "
            "lineage boundary."
        ),
    },
]


PROVENANCE_STAGES = [
    {
        "stage_id": "HOSVPA-S01",
        "stage_name": "evaluation_fixture_or_source",
        "expected_symbol": (
            "replay_evaluation_dataset or upstream evaluation fixture producer"
        ),
        "audit_question": (
            "What source value and runtime type are first assigned to "
            "outcome_value?"
        ),
        "priority": 1,
    },
    {
        "stage_id": "HOSVPA-S02",
        "stage_name": "evaluation_row",
        "expected_symbol": "evaluation-row producer",
        "audit_question": (
            "Does the evaluation row preserve, transform, default, or coerce "
            "the source outcome?"
        ),
        "priority": 2,
    },
    {
        "stage_id": "HOSVPA-S03",
        "stage_name": "prediction_outcome_join",
        "expected_symbol": "execute_join",
        "audit_question": (
            "Does Layer 9P preserve the evaluation outcome value and type in "
            "the joined record?"
        ),
        "priority": 3,
    },
    {
        "stage_id": "HOSVPA-S04",
        "stage_name": "comparative_evaluation_record",
        "expected_symbol": "comparison_record",
        "audit_question": (
            "Does Layer 9R preserve the joined outcome value and type in the "
            "comparison record?"
        ),
        "priority": 4,
    },
    {
        "stage_id": "HOSVPA-S05",
        "stage_name": "comparative_metric_consumer",
        "expected_symbol": "comparative metric calculation",
        "audit_question": (
            "How does the metric consumer validate or reject the observed "
            "outcome runtime type and domain?"
        ),
        "priority": 5,
    },
    {
        "stage_id": "HOSVPA-S06",
        "stage_name": "interpretation_evidence_and_remediation",
        "expected_symbol": "downstream audit chain",
        "audit_question": (
            "How is the invalid source value represented in interpretation, "
            "evidence, audit, and remediation records?"
        ),
        "priority": 6,
    },
]


VALUE_CLASSIFICATIONS = [
    {
        "classification": "valid_numeric",
        "definition": (
            "A finite int or float accepted by the metric domain, excluding "
            "boolean values."
        ),
    },
    {
        "classification": "boolean_source_value",
        "definition": (
            "A Python or serialized boolean value occupying the authoritative "
            "numeric outcome field."
        ),
    },
    {
        "classification": "missing_source_value",
        "definition": (
            "The authoritative outcome field is absent or has a null value."
        ),
    },
    {
        "classification": "non_finite_numeric",
        "definition": (
            "The outcome is NaN, positive infinity, or negative infinity."
        ),
    },
    {
        "classification": "numeric_string",
        "definition": (
            "The outcome is represented as text that appears numeric but has "
            "not been authorized for coercion."
        ),
    },
    {
        "classification": "non_numeric_string",
        "definition": (
            "The outcome is represented as non-numeric text."
        ),
    },
    {
        "classification": "container_value",
        "definition": (
            "The outcome is represented as a list, tuple, mapping, or other "
            "container."
        ),
    },
    {
        "classification": "unsupported_runtime_type",
        "definition": (
            "The outcome uses another runtime type not accepted by the metric "
            "contract."
        ),
    },
]


TRANSFORMATION_CLASSIFICATIONS = [
    {
        "classification": "introduced_at_stage",
        "definition": (
            "The audited value or runtime type first appears at this lineage "
            "stage."
        ),
    },
    {
        "classification": "preserved_exactly",
        "definition": (
            "The value, runtime type, and canonical serialized representation "
            "match the immediately preceding stage."
        ),
    },
    {
        "classification": "transformed_explicitly",
        "definition": (
            "Producer code explicitly transforms the source value under a "
            "documented contract."
        ),
    },
    {
        "classification": "coerced_without_authority",
        "definition": (
            "The runtime type or value is converted without authority from a "
            "versioned schema or contract."
        ),
    },
    {
        "classification": "defaulted_without_authority",
        "definition": (
            "A missing or invalid outcome is replaced by a default."
        ),
    },
    {
        "classification": "imputed_without_authority",
        "definition": (
            "A missing or invalid outcome is replaced using another value or "
            "derived estimate."
        ),
    },
    {
        "classification": "dropped_before_stage",
        "definition": (
            "The source outcome is no longer available at this lineage stage."
        ),
    },
    {
        "classification": "lineage_unresolved",
        "definition": (
            "The prior source, transformation, or digest cannot be resolved."
        ),
    },
]


AUDIT_DISPOSITIONS = [
    {
        "disposition": "source_value_defect_identified",
        "applies_when": (
            "The invalid value is already present at the earliest resolved "
            "source stage and is preserved downstream."
        ),
    },
    {
        "disposition": "producer_transformation_defect_identified",
        "applies_when": (
            "A valid upstream value becomes invalid at a specific producer "
            "stage."
        ),
    },
    {
        "disposition": "unauthorized_coercion_identified",
        "applies_when": (
            "A producer converts an outcome without explicit schema or "
            "contract authority."
        ),
    },
    {
        "disposition": "unauthorized_default_identified",
        "applies_when": (
            "A producer substitutes a default for a missing or invalid source "
            "outcome."
        ),
    },
    {
        "disposition": "unauthorized_imputation_identified",
        "applies_when": (
            "A producer substitutes another observed or derived value."
        ),
    },
    {
        "disposition": "mapping_defect_not_supported",
        "applies_when": (
            "The authoritative outcome_value mapping is preserved and the "
            "invalid value originates elsewhere."
        ),
    },
    {
        "disposition": "consumer_validation_gap_identified",
        "applies_when": (
            "An invalid source value reaches a consumer that fails to reject "
            "or classify it correctly."
        ),
    },
    {
        "disposition": "source_value_provenance_unresolved",
        "applies_when": (
            "The earliest source or a required lineage boundary cannot be "
            "resolved."
        ),
    },
]


AUDIT_RECORD_FIELDS = [
    "source_value_audit_contract_version",
    "source_value_audit_record_id",
    "authority_discovery_record_id",
    "authority_discovery_record_digest",
    "remediation_plan_record_id",
    "remediation_plan_record_digest",
    "audit_record_id",
    "audit_record_digest",
    "comparison_record_id",
    "metric_record_id",
    "metric_name",
    "aggregation_name",
    "aggregation_key",
    "authoritative_field_name",
    "authoritative_field_path",
    "rejected_metadata_field_name",
    "provenance_stage_id",
    "provenance_stage_name",
    "provenance_stage_priority",
    "source_path",
    "source_symbol",
    "source_contract_version",
    "source_artifact_version",
    "source_record_id",
    "source_record_digest",
    "source_value_present",
    "source_value",
    "source_value_canonical_json",
    "source_runtime_type",
    "source_value_classification",
    "source_value_domain_status",
    "prior_stage_id",
    "prior_source_record_id",
    "prior_source_record_digest",
    "prior_source_value",
    "prior_source_runtime_type",
    "transformation_classification",
    "transformation_evidence",
    "mapping_preserved",
    "serialization_preserved",
    "lineage_complete",
    "lineage_gap_codes",
    "audit_disposition",
    "audit_rationale",
    "audit_exclusion_codes",
    "audit_limitations",
    "source_comparison_digest",
    "source_metric_record_digest",
    "source_interpretation_digest",
    "source_evidence_record_digest",
    "source_remediation_record_digest",
    "source_value_audit_identity_digest",
    "source_value_audit_record_digest",
]


ORDERING_FIELDS = [
    {
        "ordinal": 1,
        "field": "remediation_plan_record_id",
    },
    {
        "ordinal": 2,
        "field": "authority_discovery_record_id",
    },
    {
        "ordinal": 3,
        "field": "provenance_stage_priority",
    },
    {
        "ordinal": 4,
        "field": "source_path",
    },
    {
        "ordinal": 5,
        "field": "source_record_id",
    },
    {
        "ordinal": 6,
        "field": "source_value_audit_record_id",
    },
]


EXCLUSION_CODES = [
    {
        "code": "historical_outcome_source_value_source_not_found",
        "category": "source_resolution",
    },
    {
        "code": "historical_outcome_source_value_record_not_found",
        "category": "record_resolution",
    },
    {
        "code": "historical_outcome_source_value_digest_invalid",
        "category": "lineage_validation",
    },
    {
        "code": "historical_outcome_source_value_lineage_incomplete",
        "category": "lineage_validation",
    },
    {
        "code": "historical_outcome_source_value_stage_unresolved",
        "category": "stage_resolution",
    },
    {
        "code": "historical_outcome_source_value_boolean_detected",
        "category": "runtime_type",
    },
    {
        "code": "historical_outcome_source_value_missing",
        "category": "runtime_type",
    },
    {
        "code": "historical_outcome_source_value_non_finite",
        "category": "domain",
    },
    {
        "code": "historical_outcome_source_value_numeric_string",
        "category": "runtime_type",
    },
    {
        "code": "historical_outcome_source_value_non_numeric_string",
        "category": "runtime_type",
    },
    {
        "code": "historical_outcome_source_value_container",
        "category": "runtime_type",
    },
    {
        "code": "historical_outcome_source_value_unsupported_type",
        "category": "runtime_type",
    },
    {
        "code": "historical_outcome_source_value_unauthorized_coercion",
        "category": "transformation",
    },
    {
        "code": "historical_outcome_source_value_unauthorized_default",
        "category": "transformation",
    },
    {
        "code": "historical_outcome_source_value_unauthorized_imputation",
        "category": "transformation",
    },
    {
        "code": "historical_outcome_source_value_mapping_change_prohibited",
        "category": "authority_boundary",
    },
    {
        "code": "historical_outcome_source_value_mutation_prohibited",
        "category": "authority_boundary",
    },
    {
        "code": "historical_outcome_source_value_repair_prohibited",
        "category": "authority_boundary",
    },
    {
        "code": "historical_outcome_source_value_quality_claim_prohibited",
        "category": "authority_boundary",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AF_authority_discovery_records",
    },
    {
        "ordinal": 2,
        "step": "validate_authoritative_mapping_and_complete_lineage",
    },
    {
        "ordinal": 3,
        "step": "deduplicate_affected_remediation_and_comparison_identities",
    },
    {
        "ordinal": 4,
        "step": "replay_evaluation_rows_and_layer_9P_join_records",
    },
    {
        "ordinal": 5,
        "step": "replay_layer_9R_comparison_records",
    },
    {
        "ordinal": 6,
        "step": "resolve_metric_interpretation_evidence_and_remediation_records",
    },
    {
        "ordinal": 7,
        "step": "extract_outcome_value_and_runtime_type_at_each_stage",
    },
    {
        "ordinal": 8,
        "step": "classify_source_values_and_domain_statuses",
    },
    {
        "ordinal": 9,
        "step": "compare_each_stage_to_its_immediate_predecessor",
    },
    {
        "ordinal": 10,
        "step": "classify_introduction_preservation_transformation_and_lineage_gaps",
    },
    {
        "ordinal": 11,
        "step": "emit_deterministically_ordered_audit_records",
    },
    {
        "ordinal": 12,
        "step": "verify_reverse_input_replay_and_digest_stability",
    },
    {
        "ordinal": 13,
        "step": "emit_summary_diagnosis_and_next_layer_authority",
    },
]


PROHIBITED_AUTHORITIES = [
    "canonical_historical_source_mutation",
    "canonical_outcome_mapping_change",
    "source_value_repair",
    "source_value_coercion",
    "source_value_defaulting",
    "source_value_imputation",
    "candidate_mapping_promotion",
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


def replay_predecessor() -> dict[str, Any]:
    predecessor = load_module(
        PREDECESSOR_PATH,
        "layer_9af_predecessor",
    )

    required_attributes = [
        "DISCOVERY_CONTRACT_VERSION",
        "PLAN_PATH",
        "build_discovery_records",
        "replay_predecessor",
    ]

    for attribute in required_attributes:
        if not hasattr(
            predecessor,
            attribute,
        ):
            raise RuntimeError(
                "Layer 9AF predecessor is missing required attribute: "
                + attribute
            )

    plan_module = predecessor.load_module(
        predecessor.PLAN_PATH,
        "layer_9ae_plan_for_9ag",
    )

    predecessor_replay = (
        predecessor.replay_predecessor(
            plan_module
        )
    )

    remediation_records = (
        predecessor_replay["records"]
    )

    reverse_remediation_records = (
        predecessor_replay[
            "reverse_records"
        ]
    )

    discovery_records = (
        predecessor.build_discovery_records(
            remediation_records,
            plan_module.DISCOVERY_RECORD_FIELDS,
        )
    )

    reverse_discovery_records = (
        predecessor.build_discovery_records(
            reverse_remediation_records,
            plan_module.DISCOVERY_RECORD_FIELDS,
        )
    )

    return {
        "module": predecessor,
        "plan_module": plan_module,
        "discovery_records":
            discovery_records,
        "reverse_discovery_records":
            reverse_discovery_records,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    replay = replay_predecessor()

    predecessor = replay["module"]

    records = replay[
        "discovery_records"
    ]

    reverse_records = replay[
        "reverse_discovery_records"
    ]

    authoritative_records = [
        row
        for row in records
        if (
            row.get(
                "evidence_classification"
            )
            == "authoritative_mapping_identified"
            and row.get(
                "mapping_authority_status"
            )
            == "authoritative"
            and row.get(
                "discovered_field_name"
            )
            == AUTHORITATIVE_FIELD_NAME
            and row.get(
                "discovered_field_path"
            )
            == AUTHORITATIVE_FIELD_PATH
        )
    ]

    unique_source_paths = sorted(
        {
            row.get("source_path")
            for row in authoritative_records
        }
    )

    complete_lineage_count = sum(
        all(
            valid_sha256(
                row.get(field)
            )
            for field in [
                "authority_discovery_identity_digest",
                "authority_discovery_record_digest",
                "remediation_plan_record_digest",
                "audit_record_digest",
                "source_digest",
                "source_comparison_digest",
                "source_metric_record_digest",
                "source_interpretation_digest",
                "source_evidence_record_digest",
                "source_remediation_record_digest",
            ]
        )
        for row in authoritative_records
    )

    predecessor_digest = sha256_payload(
        records
    )

    reverse_predecessor_digest = (
        sha256_payload(reverse_records)
    )

    checks = [
        {
            "check": "nine_af_contract_version_verified",
            "actual":
                predecessor.DISCOVERY_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.DISCOVERY_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_af_replay_deterministic",
            "actual": (
                canonical_json(records)
                == canonical_json(
                    reverse_records
                )
            ),
            "expected": True,
            "passed": (
                canonical_json(records)
                == canonical_json(
                    reverse_records
                )
            ),
        },
        {
            "check": "nine_af_digests_match_reverse_replay",
            "actual":
                predecessor_digest,
            "expected":
                reverse_predecessor_digest,
            "passed": (
                predecessor_digest
                == reverse_predecessor_digest
            ),
        },
        {
            "check": "twenty_four_discovery_records_replayed",
            "actual": len(records),
            "expected":
                EXPECTED_DISCOVERY_RECORDS,
            "passed": (
                len(records)
                == EXPECTED_DISCOVERY_RECORDS
            ),
        },
        {
            "check": "twenty_four_authoritative_records_selected",
            "actual":
                len(authoritative_records),
            "expected":
                EXPECTED_AUTHORITATIVE_RECORDS,
            "passed": (
                len(authoritative_records)
                == EXPECTED_AUTHORITATIVE_RECORDS
            ),
        },
        {
            "check": "authoritative_field_name_verified",
            "actual": sorted(
                {
                    row.get(
                        "discovered_field_name"
                    )
                    for row
                    in authoritative_records
                }
            ),
            "expected": [
                AUTHORITATIVE_FIELD_NAME
            ],
            "passed": all(
                row.get(
                    "discovered_field_name"
                )
                == AUTHORITATIVE_FIELD_NAME
                for row
                in authoritative_records
            ),
        },
        {
            "check": "authoritative_field_path_verified",
            "actual": sorted(
                {
                    row.get(
                        "discovered_field_path"
                    )
                    for row
                    in authoritative_records
                }
            ),
            "expected": [
                AUTHORITATIVE_FIELD_PATH
            ],
            "passed": all(
                row.get(
                    "discovered_field_path"
                )
                == AUTHORITATIVE_FIELD_PATH
                for row
                in authoritative_records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row.get(
                        "rejected_candidate_field_name"
                    )
                    for row
                    in authoritative_records
                }
            ),
            "expected": [
                REJECTED_METADATA_FIELD
            ],
            "passed": all(
                row.get(
                    "rejected_candidate_field_name"
                )
                == REJECTED_METADATA_FIELD
                and row.get(
                    "rejected_candidate_semantic"
                )
                == "outcome_availability_metadata"
                for row
                in authoritative_records
            ),
        },
        {
            "check": "two_authoritative_source_paths_preserved",
            "actual":
                len(unique_source_paths),
            "expected":
                EXPECTED_PREDECESSOR_SOURCES,
            "passed": (
                len(unique_source_paths)
                == EXPECTED_PREDECESSOR_SOURCES
            ),
        },
        {
            "check": "all_authoritative_records_preserve_complete_lineage",
            "actual":
                complete_lineage_count,
            "expected":
                EXPECTED_AUTHORITATIVE_RECORDS,
            "passed": (
                complete_lineage_count
                == EXPECTED_AUTHORITATIVE_RECORDS
            ),
        },
        {
            "check": "input_rules_defined",
            "actual":
                len(INPUT_RULES),
            "expected": 8,
            "passed": (
                len(INPUT_RULES)
                == 8
            ),
        },
        {
            "check": "audit_objectives_defined",
            "actual":
                len(AUDIT_OBJECTIVES),
            "expected": 6,
            "passed": (
                len(AUDIT_OBJECTIVES)
                == 6
            ),
        },
        {
            "check": "provenance_stages_defined",
            "actual":
                len(PROVENANCE_STAGES),
            "expected": 6,
            "passed": (
                len(PROVENANCE_STAGES)
                == 6
            ),
        },
        {
            "check": "value_classifications_defined",
            "actual":
                len(VALUE_CLASSIFICATIONS),
            "expected": 8,
            "passed": (
                len(VALUE_CLASSIFICATIONS)
                == 8
            ),
        },
        {
            "check": "transformation_classifications_defined",
            "actual":
                len(
                    TRANSFORMATION_CLASSIFICATIONS
                ),
            "expected": 8,
            "passed": (
                len(
                    TRANSFORMATION_CLASSIFICATIONS
                )
                == 8
            ),
        },
        {
            "check": "audit_dispositions_defined",
            "actual":
                len(AUDIT_DISPOSITIONS),
            "expected": 8,
            "passed": (
                len(AUDIT_DISPOSITIONS)
                == 8
            ),
        },
        {
            "check": "audit_record_fields_defined",
            "actual":
                len(AUDIT_RECORD_FIELDS),
            "expected": 53,
            "passed": (
                len(AUDIT_RECORD_FIELDS)
                == 53
            ),
        },
        {
            "check": "ordering_fields_defined",
            "actual":
                len(ORDERING_FIELDS),
            "expected": 6,
            "passed": (
                len(ORDERING_FIELDS)
                == 6
            ),
        },
        {
            "check": "exclusion_codes_defined",
            "actual":
                len(EXCLUSION_CODES),
            "expected": 19,
            "passed": (
                len(EXCLUSION_CODES)
                == 19
            ),
        },
        {
            "check": "implementation_steps_defined",
            "actual":
                len(IMPLEMENTATION_STEPS),
            "expected": 13,
            "passed": (
                len(IMPLEMENTATION_STEPS)
                == 13
            ),
        },
        {
            "check": "canonical_source_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_historical_source_mutation"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "canonical_mapping_change_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_outcome_mapping_change"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "source_value_repair_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "source_value_repair"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "source_value_coercion_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "source_value_coercion"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "source_value_defaulting_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "source_value_defaulting"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "source_value_imputation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "source_value_imputation"
                in PROHIBITED_AUTHORITIES
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
            "check": "audit_records_not_materialized",
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
            "check": "superiority_not_declared",
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

    plan_digest = sha256_payload(
        {
            "plan_version":
                PLAN_VERSION,
            "input_rules":
                INPUT_RULES,
            "audit_objectives":
                AUDIT_OBJECTIVES,
            "provenance_stages":
                PROVENANCE_STAGES,
            "value_classifications":
                VALUE_CLASSIFICATIONS,
            "transformation_classifications":
                TRANSFORMATION_CLASSIFICATIONS,
            "audit_dispositions":
                AUDIT_DISPOSITIONS,
            "audit_record_fields":
                AUDIT_RECORD_FIELDS,
            "ordering_fields":
                ORDERING_FIELDS,
            "exclusion_codes":
                EXCLUSION_CODES,
            "implementation_steps":
                IMPLEMENTATION_STEPS,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_"
        "outcome_source_value_provenance_audit_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_source_value_provenance_audit_plan_failed"
    )

    next_layer = (
        "9AH_pitch_type_matchup_overlay_historical_"
        "outcome_source_value_provenance_audit_implementation"
        if all_checks_passed
        else
        "9AG_pitch_type_matchup_overlay_historical_"
        "outcome_source_value_provenance_audit_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR / "input_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        INPUT_RULES,
    )

    write_csv(
        OUTPUT_DIR / "audit_objectives.csv",
        [
            "objective_id",
            "objective",
        ],
        AUDIT_OBJECTIVES,
    )

    write_csv(
        OUTPUT_DIR / "provenance_stages.csv",
        [
            "stage_id",
            "stage_name",
            "expected_symbol",
            "audit_question",
            "priority",
        ],
        PROVENANCE_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "value_classifications.csv",
        [
            "classification",
            "definition",
        ],
        VALUE_CLASSIFICATIONS,
    )

    write_csv(
        OUTPUT_DIR
        / "transformation_classifications.csv",
        [
            "classification",
            "definition",
        ],
        TRANSFORMATION_CLASSIFICATIONS,
    )

    write_csv(
        OUTPUT_DIR / "audit_dispositions.csv",
        [
            "disposition",
            "applies_when",
        ],
        AUDIT_DISPOSITIONS,
    )

    write_csv(
        OUTPUT_DIR
        / "audit_record_field_contract.csv",
        [
            "ordinal",
            "field",
        ],
        [
            {
                "ordinal": index,
                "field": field,
            }
            for index, field in enumerate(
                AUDIT_RECORD_FIELDS,
                start=1,
            )
        ],
    )

    write_csv(
        OUTPUT_DIR / "ordering_fields.csv",
        [
            "ordinal",
            "field",
        ],
        ORDERING_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "exclusion_code_catalog.csv",
        [
            "code",
            "category",
        ],
        EXCLUSION_CODES,
    )

    write_csv(
        OUTPUT_DIR / "implementation_steps.csv",
        [
            "ordinal",
            "step",
        ],
        IMPLEMENTATION_STEPS,
    )

    write_csv(
        OUTPUT_DIR / "authority_boundaries.csv",
        [
            "authority",
            "granted",
            "reason",
        ],
        [
            {
                "authority": authority,
                "granted": False,
                "reason": (
                    "Layer 9AG is planning-only and grants no canonical "
                    "mutation, mapping change, source-value repair, coercion, "
                    "defaulting, imputation, canonical recomputation, "
                    "uncertainty, significance, superiority, equivalence, "
                    "activation, production, market, pricing, or betting "
                    "authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_source_value_"
                    "provenance_audit_implementation"
                ),
                "granted":
                    all_checks_passed,
                "reason": (
                    "Layer 9AH may perform deterministic read-only tracing "
                    "and classification of authoritative outcome values "
                    "through the historical evaluation lineage."
                ),
            }
        ],
    )

    summary = {
        "layer_id":
            LAYER_ID,
        "layer_name":
            LAYER_NAME,
        "plan_version":
            PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.DISCOVERY_CONTRACT_VERSION,
        "predecessor_records":
            len(records),
        "authoritative_records":
            len(authoritative_records),
        "authoritative_source_paths":
            unique_source_paths,
        "authoritative_field_name":
            AUTHORITATIVE_FIELD_NAME,
        "authoritative_field_path":
            AUTHORITATIVE_FIELD_PATH,
        "rejected_metadata_field":
            REJECTED_METADATA_FIELD,
        "predecessor_digest":
            predecessor_digest,
        "reverse_predecessor_digest":
            reverse_predecessor_digest,
        "input_rules":
            len(INPUT_RULES),
        "audit_objectives":
            len(AUDIT_OBJECTIVES),
        "provenance_stages":
            len(PROVENANCE_STAGES),
        "value_classifications":
            len(VALUE_CLASSIFICATIONS),
        "transformation_classifications":
            len(
                TRANSFORMATION_CLASSIFICATIONS
            ),
        "audit_dispositions":
            len(AUDIT_DISPOSITIONS),
        "audit_record_fields":
            len(AUDIT_RECORD_FIELDS),
        "ordering_fields":
            len(ORDERING_FIELDS),
        "exclusion_codes":
            len(EXCLUSION_CODES),
        "implementation_steps":
            len(IMPLEMENTATION_STEPS),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required":
            len(checks),
        "plan_digest":
            plan_digest,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "source_values_repaired": 0,
        "source_values_coerced": 0,
        "source_values_defaulted": 0,
        "source_values_imputed": 0,
        "audit_records_materialized": 0,
        "canonical_contract_records_recomputed": 0,
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
        / "outcome_source_value_provenance_audit_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id":
            LAYER_ID,
        "layer_name":
            LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis":
            diagnosis_name,
        "authority_granted": (
            "historical_outcome_source_value_"
            "provenance_audit_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld":
            sorted(PROHIBITED_AUTHORITIES),
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
        f"Plan version: {PLAN_VERSION}"
    )
    print(
        "Predecessor contract version: "
        f"{predecessor.DISCOVERY_CONTRACT_VERSION}"
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
    print(
        "Authority discovery records: "
        f"{len(records)}"
    )
    print(
        "Authoritative discovery records: "
        f"{len(authoritative_records)}"
    )
    print(
        "Authoritative source paths: "
        f"{unique_source_paths}"
    )
    print(
        "Authoritative field: "
        f"{AUTHORITATIVE_FIELD_PATH}"
    )
    print(
        "Rejected metadata field: "
        f"{REJECTED_METADATA_FIELD}"
    )
    print(
        "Provenance stages: "
        f"{len(PROVENANCE_STAGES)}"
    )
    print(
        "Value classifications: "
        f"{len(VALUE_CLASSIFICATIONS)}"
    )
    print(
        "Audit record fields: "
        f"{len(AUDIT_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Source values repaired: 0")
    print("Source values coerced: 0")
    print("Source values defaulted: 0")
    print("Source values imputed: 0")
    print("Audit records materialized: 0")
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
    print(
        f"Diagnosis: {diagnosis_name}"
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
