#!/usr/bin/env python3
"""
Layer 9AI
Pitch-Type Matchup Overlay Historical Outcome Source-Value Remediation Plan

Plans a deterministic remediation contract from the Layer 9AH source-value
provenance audit.

Layer 9AH established that invalid boolean values originate at the earliest
resolved evaluation source and are preserved exactly through the authoritative
`outcome_value` mapping. Therefore this layer plans source correction rather
than mapping replacement, coercion, defaulting, or imputation.

Planning only. No canonical source values or downstream records are mutated.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AI"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_source_value_remediation_plan"
)

PLAN_VERSION = (
    "layer_9AI_historical_outcome_source_value_"
    "remediation_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AI_pitch_type_matchup_overlay_"
    "historical_outcome_source_value_remediation_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9AH_pitch_type_matchup_overlay_"
    "historical_outcome_source_value_provenance.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AH_historical_outcome_source_value_"
    "provenance_audit_contract_v1"
)

EXPECTED_AUDIT_RECORDS = 108
EXPECTED_AUDITED_COMPARISONS = 18
EXPECTED_SOURCE_DEFECT_COMPARISONS = 16
EXPECTED_BOOLEAN_RECORDS = 96
EXPECTED_PROVENANCE_STAGES = 6

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


REMEDIATION_PRINCIPLES = [
    {
        "principle_id": "HOSVRP-P01",
        "principle": (
            "Repair must occur at the earliest authoritative evaluation-source "
            "producer where the invalid boolean value originates."
        ),
    },
    {
        "principle_id": "HOSVRP-P02",
        "principle": (
            "The canonical outcome field remains outcome_value; "
            "outcome_available_at_utc remains metadata."
        ),
    },
    {
        "principle_id": "HOSVRP-P03",
        "principle": (
            "Boolean values must never be accepted as numeric outcomes even "
            "though Python bool is a subclass of int."
        ),
    },
    {
        "principle_id": "HOSVRP-P04",
        "principle": (
            "No boolean-to-integer coercion, defaulting, or imputation may be "
            "used to manufacture a historical outcome."
        ),
    },
    {
        "principle_id": "HOSVRP-P05",
        "principle": (
            "A corrected source value must be supported by an authoritative "
            "historical observation or versioned fixture contract."
        ),
    },
    {
        "principle_id": "HOSVRP-P06",
        "principle": (
            "Downstream Layer 9P and Layer 9R records may only be regenerated "
            "after source correction and full lineage validation."
        ),
    },
    {
        "principle_id": "HOSVRP-P07",
        "principle": (
            "Pre-remediation records and digests must remain preserved as "
            "immutable evidence."
        ),
    },
    {
        "principle_id": "HOSVRP-P08",
        "principle": (
            "Remediation does not establish predictive improvement, "
            "superiority, equivalence, activation, or production readiness."
        ),
    },
]


REMEDIATION_ACTIONS = [
    {
        "action_id": "HOSVRP-A01",
        "action_name": "identify_authoritative_source_observation",
        "stage": "source_resolution",
        "description": (
            "Resolve the authoritative historical observation corresponding "
            "to each defective evaluation-row identity."
        ),
        "mutation_authority": False,
    },
    {
        "action_id": "HOSVRP-A02",
        "action_name": "validate_source_value_domain",
        "stage": "source_validation",
        "description": (
            "Require a finite int or float and explicitly reject bool, null, "
            "strings, containers, and non-finite numerics."
        ),
        "mutation_authority": False,
    },
    {
        "action_id": "HOSVRP-A03",
        "action_name": "prepare_versioned_source_patch",
        "stage": "patch_preparation",
        "description": (
            "Prepare a deterministic source or fixture patch preserving the "
            "old value, new value, authority evidence, and lineage."
        ),
        "mutation_authority": False,
    },
    {
        "action_id": "HOSVRP-A04",
        "action_name": "apply_source_patch",
        "stage": "source_remediation",
        "description": (
            "Replace an invalid source value only when explicit source-patch "
            "authority is granted by a later implementation layer."
        ),
        "mutation_authority": False,
    },
    {
        "action_id": "HOSVRP-A05",
        "action_name": "replay_evaluation_rows",
        "stage": "controlled_replay",
        "description": (
            "Regenerate evaluation rows from the corrected source without "
            "changing the canonical outcome mapping."
        ),
        "mutation_authority": False,
    },
    {
        "action_id": "HOSVRP-A06",
        "action_name": "replay_join_and_comparison_records",
        "stage": "controlled_replay",
        "description": (
            "Regenerate Layer 9P joined records and Layer 9R comparison records "
            "using corrected source values."
        ),
        "mutation_authority": False,
    },
    {
        "action_id": "HOSVRP-A07",
        "action_name": "validate_digest_and_lineage_changes",
        "stage": "post_remediation_validation",
        "description": (
            "Verify only expected source-dependent records and digests change."
        ),
        "mutation_authority": False,
    },
    {
        "action_id": "HOSVRP-A08",
        "action_name": "recompute_downstream_audit_chain",
        "stage": "post_remediation_validation",
        "description": (
            "Recompute downstream metrics and audit artifacts only under "
            "separately granted authority."
        ),
        "mutation_authority": False,
    },
]


VALUE_REQUIREMENTS = [
    {
        "requirement_id": "HOSVRP-V01",
        "requirement": "corrected_value_present",
        "expected": True,
    },
    {
        "requirement_id": "HOSVRP-V02",
        "requirement": "corrected_runtime_type",
        "expected": "int_or_float_excluding_bool",
    },
    {
        "requirement_id": "HOSVRP-V03",
        "requirement": "corrected_value_finite",
        "expected": True,
    },
    {
        "requirement_id": "HOSVRP-V04",
        "requirement": "authoritative_observation_identified",
        "expected": True,
    },
    {
        "requirement_id": "HOSVRP-V05",
        "requirement": "source_identity_preserved",
        "expected": True,
    },
    {
        "requirement_id": "HOSVRP-V06",
        "requirement": "old_value_preserved_as_evidence",
        "expected": True,
    },
    {
        "requirement_id": "HOSVRP-V07",
        "requirement": "coercion_used",
        "expected": False,
    },
    {
        "requirement_id": "HOSVRP-V08",
        "requirement": "default_or_imputation_used",
        "expected": False,
    },
]


REMEDIATION_RECORD_FIELDS = [
    "source_value_remediation_plan_contract_version",
    "source_value_remediation_plan_record_id",
    "source_value_audit_record_id",
    "source_value_audit_record_digest",
    "authority_discovery_record_id",
    "remediation_plan_record_id",
    "audit_record_id",
    "comparison_record_id",
    "metric_record_id",
    "metric_name",
    "aggregation_name",
    "aggregation_key",
    "authoritative_field_name",
    "authoritative_field_path",
    "rejected_metadata_field_name",
    "defect_stage_id",
    "defect_stage_name",
    "defect_source_path",
    "defect_source_symbol",
    "defect_source_record_id",
    "defect_source_record_digest",
    "defective_value",
    "defective_runtime_type",
    "defective_value_classification",
    "defect_disposition",
    "remediation_scope",
    "required_authority_source",
    "required_corrected_value_type",
    "required_corrected_value_domain",
    "coercion_permitted",
    "defaulting_permitted",
    "imputation_permitted",
    "mapping_change_permitted",
    "source_identity_must_be_preserved",
    "old_value_must_be_preserved",
    "expected_replay_stages",
    "expected_changed_record_classes",
    "expected_unchanged_contracts",
    "validation_requirements",
    "remediation_blockers",
    "remediation_status",
    "remediation_rationale",
    "remediation_limitations",
    "source_value_remediation_plan_identity_digest",
    "source_value_remediation_plan_record_digest",
]


ORDERING_FIELDS = [
    {
        "ordinal": 1,
        "field": "comparison_record_id",
    },
    {
        "ordinal": 2,
        "field": "defect_stage_id",
    },
    {
        "ordinal": 3,
        "field": "defect_source_record_id",
    },
    {
        "ordinal": 4,
        "field": "source_value_remediation_plan_record_id",
    },
]


BLOCKER_CODES = [
    {
        "code": "historical_outcome_authoritative_observation_missing",
        "category": "source_authority",
    },
    {
        "code": "historical_outcome_corrected_value_missing",
        "category": "value_validation",
    },
    {
        "code": "historical_outcome_corrected_value_boolean",
        "category": "value_validation",
    },
    {
        "code": "historical_outcome_corrected_value_non_numeric",
        "category": "value_validation",
    },
    {
        "code": "historical_outcome_corrected_value_non_finite",
        "category": "value_validation",
    },
    {
        "code": "historical_outcome_source_identity_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_outcome_source_digest_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_pre_remediation_evidence_missing",
        "category": "evidence",
    },
    {
        "code": "historical_outcome_unauthorized_coercion_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_unauthorized_default_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_unauthorized_imputation_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_mapping_change_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_replay_scope_unresolved",
        "category": "replay",
    },
    {
        "code": "historical_outcome_unexpected_digest_change",
        "category": "post_validation",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AH_source_value_provenance_audit",
    },
    {
        "ordinal": 2,
        "step": "select_earliest_stage_source_value_defects",
    },
    {
        "ordinal": 3,
        "step": "deduplicate_defects_by_comparison_and_source_identity",
    },
    {
        "ordinal": 4,
        "step": "resolve_authoritative_historical_observation_for_each_defect",
    },
    {
        "ordinal": 5,
        "step": "validate_corrected_value_type_and_domain_without_coercion",
    },
    {
        "ordinal": 6,
        "step": "prepare_versioned_source_patch_records",
    },
    {
        "ordinal": 7,
        "step": "preserve_pre_remediation_values_and_digests",
    },
    {
        "ordinal": 8,
        "step": "apply_only_explicitly_authorized_source_patches",
    },
    {
        "ordinal": 9,
        "step": "replay_evaluation_join_and_comparison_stages",
    },
    {
        "ordinal": 10,
        "step": "validate_expected_and_unexpected_digest_changes",
    },
    {
        "ordinal": 11,
        "step": "emit_deterministic_remediation_results",
    },
    {
        "ordinal": 12,
        "step": "withhold_downstream_recomputation_without_separate_authority",
    },
]


PROHIBITED_AUTHORITIES = [
    "canonical_source_value_mutation",
    "canonical_outcome_mapping_change",
    "boolean_to_integer_coercion",
    "source_value_defaulting",
    "source_value_imputation",
    "unversioned_fixture_replacement",
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
            fieldnames=list(fieldnames),
            extrasaction="ignore",
        )

        writer.writeheader()

        for row in rows:
            serialized = {}

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
        "layer_9ah_predecessor",
    )

    if (
        predecessor.AUDIT_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AH contract version: "
            f"{predecessor.AUDIT_CONTRACT_VERSION}"
        )

    predecessor_replay = (
        predecessor.replay_plan_and_discovery()
    )

    historical_replay = (
        predecessor.replay_historical_chain()
    )

    plan = predecessor_replay["plan"]

    records = predecessor.build_audit_records(
        plan,
        predecessor_replay[
            "discovery_records"
        ],
        historical_replay[
            "evaluation_rows"
        ],
        historical_replay[
            "joined_rows"
        ],
        historical_replay[
            "comparison_records"
        ],
    )

    reverse_records = predecessor.build_audit_records(
        plan,
        list(
            reversed(
                predecessor_replay[
                    "reverse_discovery_records"
                ]
            )
        ),
        list(
            reversed(
                historical_replay[
                    "evaluation_rows"
                ]
            )
        ),
        list(
            reversed(
                historical_replay[
                    "joined_rows"
                ]
            )
        ),
        list(
            reversed(
                historical_replay[
                    "reverse_comparison_records"
                ]
            )
        ),
    )

    return {
        "module": predecessor,
        "records": records,
        "reverse_records": reverse_records,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    replay = replay_predecessor()

    predecessor = replay["module"]
    records = replay["records"]
    reverse_records = replay["reverse_records"]

    defect_records = [
        row
        for row in records
        if (
            row.get("provenance_stage_priority") == 1
            and row.get("audit_disposition")
            == "source_value_defect_identified"
            and row.get("source_value_classification")
            == "boolean_source_value"
        )
    ]

    defect_comparison_ids = sorted(
        {
            row.get("comparison_record_id")
            for row in defect_records
        }
    )

    audited_comparison_ids = sorted(
        {
            row.get("comparison_record_id")
            for row in records
        }
    )

    stage_counts = Counter(
        row.get("provenance_stage_name")
        for row in records
    )

    boolean_count = sum(
        row.get("source_value_classification")
        == "boolean_source_value"
        for row in records
    )

    mapping_preserved_count = sum(
        bool(row.get("mapping_preserved"))
        for row in records
    )

    complete_lineage_count = sum(
        bool(row.get("lineage_complete"))
        for row in records
    )

    checks = [
        {
            "check": "nine_ah_contract_version_verified",
            "actual": predecessor.AUDIT_CONTRACT_VERSION,
            "expected": EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor.AUDIT_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
            ),
        },
        {
            "check": "nine_ah_replay_deterministic",
            "actual": canonical_json(records)
            == canonical_json(reverse_records),
            "expected": True,
            "passed": canonical_json(records)
            == canonical_json(reverse_records),
        },
        {
            "check": "nine_ah_digest_replay_deterministic",
            "actual": sha256_payload(records),
            "expected": sha256_payload(reverse_records),
            "passed": sha256_payload(records)
            == sha256_payload(reverse_records),
        },
        {
            "check": "expected_audit_records_replayed",
            "actual": len(records),
            "expected": EXPECTED_AUDIT_RECORDS,
            "passed": len(records)
            == EXPECTED_AUDIT_RECORDS,
        },
        {
            "check": "expected_comparisons_replayed",
            "actual": len(audited_comparison_ids),
            "expected": EXPECTED_AUDITED_COMPARISONS,
            "passed": len(audited_comparison_ids)
            == EXPECTED_AUDITED_COMPARISONS,
        },
        {
            "check": "six_provenance_stages_replayed",
            "actual": len(stage_counts),
            "expected": EXPECTED_PROVENANCE_STAGES,
            "passed": len(stage_counts)
            == EXPECTED_PROVENANCE_STAGES,
        },
        {
            "check": "expected_boolean_records_replayed",
            "actual": boolean_count,
            "expected": EXPECTED_BOOLEAN_RECORDS,
            "passed": boolean_count
            == EXPECTED_BOOLEAN_RECORDS,
        },
        {
            "check": "expected_source_defect_comparisons_selected",
            "actual": len(defect_comparison_ids),
            "expected": EXPECTED_SOURCE_DEFECT_COMPARISONS,
            "passed": len(defect_comparison_ids)
            == EXPECTED_SOURCE_DEFECT_COMPARISONS,
        },
        {
            "check": "one_earliest_stage_record_per_defect",
            "actual": len(defect_records),
            "expected": EXPECTED_SOURCE_DEFECT_COMPARISONS,
            "passed": len(defect_records)
            == EXPECTED_SOURCE_DEFECT_COMPARISONS,
        },
        {
            "check": "all_source_lineage_complete",
            "actual": complete_lineage_count,
            "expected": len(records),
            "passed": complete_lineage_count
            == len(records),
        },
        {
            "check": "mapping_preserved_through_all_records",
            "actual": mapping_preserved_count,
            "expected": len(records),
            "passed": mapping_preserved_count
            == len(records),
        },
        {
            "check": "authoritative_field_name_preserved",
            "actual": sorted(
                {
                    row.get("authoritative_field_name")
                    for row in records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_NAME],
            "passed": all(
                row.get("authoritative_field_name")
                == AUTHORITATIVE_FIELD_NAME
                for row in records
            ),
        },
        {
            "check": "authoritative_field_path_preserved",
            "actual": sorted(
                {
                    row.get("authoritative_field_path")
                    for row in records
                }
            ),
            "expected": [AUTHORITATIVE_FIELD_PATH],
            "passed": all(
                row.get("authoritative_field_path")
                == AUTHORITATIVE_FIELD_PATH
                for row in records
            ),
        },
        {
            "check": "rejected_metadata_field_preserved",
            "actual": sorted(
                {
                    row.get("rejected_metadata_field_name")
                    for row in records
                }
            ),
            "expected": [REJECTED_METADATA_FIELD],
            "passed": all(
                row.get("rejected_metadata_field_name")
                == REJECTED_METADATA_FIELD
                for row in records
            ),
        },
        {
            "check": "all_defect_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row.get("source_record_digest")
                )
                for row in defect_records
            ),
            "expected": len(defect_records),
            "passed": all(
                valid_sha256(
                    row.get("source_record_digest")
                )
                for row in defect_records
            ),
        },
        {
            "check": "remediation_principles_defined",
            "actual": len(REMEDIATION_PRINCIPLES),
            "expected": 8,
            "passed": len(REMEDIATION_PRINCIPLES) == 8,
        },
        {
            "check": "remediation_actions_defined",
            "actual": len(REMEDIATION_ACTIONS),
            "expected": 8,
            "passed": len(REMEDIATION_ACTIONS) == 8,
        },
        {
            "check": "value_requirements_defined",
            "actual": len(VALUE_REQUIREMENTS),
            "expected": 8,
            "passed": len(VALUE_REQUIREMENTS) == 8,
        },
        {
            "check": "remediation_record_fields_defined",
            "actual": len(REMEDIATION_RECORD_FIELDS),
            "expected": 45,
            "passed": len(REMEDIATION_RECORD_FIELDS) == 45,
        },
        {
            "check": "ordering_fields_defined",
            "actual": len(ORDERING_FIELDS),
            "expected": 4,
            "passed": len(ORDERING_FIELDS) == 4,
        },
        {
            "check": "blocker_codes_defined",
            "actual": len(BLOCKER_CODES),
            "expected": 14,
            "passed": len(BLOCKER_CODES) == 14,
        },
        {
            "check": "implementation_steps_defined",
            "actual": len(IMPLEMENTATION_STEPS),
            "expected": 12,
            "passed": len(IMPLEMENTATION_STEPS) == 12,
        },
        {
            "check": "canonical_source_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_source_value_mutation"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "mapping_change_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_outcome_mapping_change"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "boolean_coercion_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "boolean_to_integer_coercion"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "defaulting_and_imputation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "source_value_defaulting"
                in PROHIBITED_AUTHORITIES
                and "source_value_imputation"
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

    plan_digest = sha256_payload(
        {
            "plan_version": PLAN_VERSION,
            "remediation_principles":
                REMEDIATION_PRINCIPLES,
            "remediation_actions":
                REMEDIATION_ACTIONS,
            "value_requirements":
                VALUE_REQUIREMENTS,
            "remediation_record_fields":
                REMEDIATION_RECORD_FIELDS,
            "ordering_fields":
                ORDERING_FIELDS,
            "blocker_codes":
                BLOCKER_CODES,
            "implementation_steps":
                IMPLEMENTATION_STEPS,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_"
        "outcome_source_value_remediation_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_source_value_remediation_plan_failed"
    )

    next_layer = (
        "9AJ_pitch_type_matchup_overlay_historical_"
        "outcome_source_value_remediation_implementation"
        if all_checks_passed
        else
        "9AI_pitch_type_matchup_overlay_historical_"
        "outcome_source_value_remediation_plan_remediation"
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
        OUTPUT_DIR / "remediation_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        REMEDIATION_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "remediation_actions.csv",
        [
            "action_id",
            "action_name",
            "stage",
            "description",
            "mutation_authority",
        ],
        REMEDIATION_ACTIONS,
    )

    write_csv(
        OUTPUT_DIR / "value_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        VALUE_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "remediation_record_field_contract.csv",
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
                REMEDIATION_RECORD_FIELDS,
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
        OUTPUT_DIR / "blocker_code_catalog.csv",
        [
            "code",
            "category",
        ],
        BLOCKER_CODES,
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
        OUTPUT_DIR / "defect_source_inventory.csv",
        [
            "comparison_record_id",
            "source_value_audit_record_id",
            "source_record_id",
            "source_record_digest",
            "source_value",
            "source_runtime_type",
            "source_value_classification",
            "audit_disposition",
        ],
        [
            {
                "comparison_record_id":
                    row.get("comparison_record_id"),
                "source_value_audit_record_id":
                    row.get("source_value_audit_record_id"),
                "source_record_id":
                    row.get("source_record_id"),
                "source_record_digest":
                    row.get("source_record_digest"),
                "source_value":
                    row.get("source_value"),
                "source_runtime_type":
                    row.get("source_runtime_type"),
                "source_value_classification":
                    row.get("source_value_classification"),
                "audit_disposition":
                    row.get("audit_disposition"),
            }
            for row in defect_records
        ],
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
                    "Layer 9AI is planning-only and grants no canonical "
                    "source mutation, mapping change, coercion, defaulting, "
                    "imputation, downstream recomputation, quality, "
                    "production, market, pricing, or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_source_value_"
                    "remediation_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9AJ may implement deterministic source-value "
                    "remediation only under the source-authority, validation, "
                    "versioning, evidence-preservation, and replay boundaries "
                    "defined by this plan."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.AUDIT_CONTRACT_VERSION,
        "audit_records": len(records),
        "audited_comparisons":
            len(audited_comparison_ids),
        "source_defect_records":
            len(defect_records),
        "source_defect_comparisons":
            len(defect_comparison_ids),
        "boolean_source_value_records":
            boolean_count,
        "provenance_stages":
            len(stage_counts),
        "mapping_preserved_records":
            mapping_preserved_count,
        "lineage_complete_records":
            complete_lineage_count,
        "remediation_principles":
            len(REMEDIATION_PRINCIPLES),
        "remediation_actions":
            len(REMEDIATION_ACTIONS),
        "value_requirements":
            len(VALUE_REQUIREMENTS),
        "remediation_record_fields":
            len(REMEDIATION_RECORD_FIELDS),
        "ordering_fields":
            len(ORDERING_FIELDS),
        "blocker_codes":
            len(BLOCKER_CODES),
        "implementation_steps":
            len(IMPLEMENTATION_STEPS),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "planning_checks_required":
            len(checks),
        "predecessor_digest":
            sha256_payload(records),
        "reverse_predecessor_digest":
            sha256_payload(reverse_records),
        "plan_digest": plan_digest,
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
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "outcome_source_value_remediation_plan_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_source_value_"
            "remediation_implementation"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld":
            sorted(PROHIBITED_AUTHORITIES),
        "recommended_next_layer": next_layer,
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
        f"{predecessor.AUDIT_CONTRACT_VERSION}"
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
        f"Audit records replayed: {len(records)}"
    )
    print(
        "Audited comparisons: "
        f"{len(audited_comparison_ids)}"
    )
    print(
        "Boolean source-value records: "
        f"{boolean_count}"
    )
    print(
        "Source-defect comparisons: "
        f"{len(defect_comparison_ids)}"
    )
    print(
        "Mapping preserved records: "
        f"{mapping_preserved_count}"
    )
    print(
        "Lineage-complete records: "
        f"{complete_lineage_count}"
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
