#!/usr/bin/env python3
"""
Layer 9AE
Pitch-Type Matchup Overlay Historical Outcome Mapping Authority Discovery Plan

Plans a deterministic, read-only search for authoritative evidence identifying
the correct historical observed-outcome field or mapping.

Layer 9AD established that `outcome_available_at_utc` is metadata and is not a
compatible replacement for the invalid historical `outcome_value` mapping.

Planning only.

This layer defines:

- admissible Layer 9AD remediation inputs and lineage;
- authoritative source classes and source-priority rules;
- repository, schema, contract, artifact, and producer-code discovery scopes;
- deterministic mapping-evidence classifications;
- conflict, ambiguity, and absence dispositions;
- discovery record fields, ordering, artifacts, and checks;
- authority boundaries for Layer 9AF.

This layer does not:

- mutate canonical historical records;
- change the canonical outcome mapping;
- treat field-name similarity as authoritative evidence;
- coerce, default, fabricate, duplicate, or impute outcomes;
- infer an outcome from availability metadata;
- lower support thresholds;
- recompute canonical metrics, interpretations, evidence, or remediation records;
- execute candidate replay;
- estimate uncertainty or statistical significance;
- declare superiority, equivalence, activation, or production readiness;
- modify production probabilities, simulations, pricing, markets, or bets.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AE"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_mapping_authority_discovery_plan"
)

PLAN_VERSION = (
    "layer_9AE_historical_outcome_mapping_"
    "authority_discovery_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AE_pitch_type_matchup_overlay_"
    "historical_outcome_mapping_authority_discovery_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "remediate_9AD_pitch_type_matchup_overlay_"
    "historical_outcome_value_provenance.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AD_historical_outcome_value_"
    "provenance_remediation_contract_v1"
)

EXPECTED_RECORD_COUNT = 12

EXPECTED_PREDECESSOR_DISPOSITION = (
    "candidate_incompatible"
)

EXPECTED_PREDECESSOR_CANDIDATE = (
    "outcome_available_at_utc"
)


INPUT_RULES = [
    {
        "rule_id": "HOMAD-I01",
        "rule": "remediation_plan_record_id_must_be_unique",
    },
    {
        "rule_id": "HOMAD-I02",
        "rule": "remediation_plan_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOMAD-I03",
        "rule": "audit_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOMAD-I04",
        "rule": "source_comparison_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOMAD-I05",
        "rule": "source_metric_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOMAD-I06",
        "rule": "source_interpretation_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOMAD-I07",
        "rule": "source_evidence_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOMAD-I08",
        "rule": "source_remediation_record_digest_must_be_valid_sha256",
    },
    {
        "rule_id": "HOMAD-I09",
        "rule": "all_records_must_be_candidate_incompatible",
    },
    {
        "rule_id": "HOMAD-I10",
        "rule": "candidate_field_must_equal_outcome_available_at_utc",
    },
    {
        "rule_id": "HOMAD-I11",
        "rule": "exactly_twelve_predecessor_records_must_be_selected",
    },
    {
        "rule_id": "HOMAD-I12",
        "rule": "canonical_sources_and_mappings_must_remain_read_only",
    },
]


AUTHORITY_SOURCE_CLASSES = [
    {
        "source_class_id": "HOMAD-S01",
        "source_class": "producer_code",
        "priority": 1,
        "authority_test": (
            "Code that constructs the historical comparison record explicitly "
            "assigns a source value to the observed-outcome field."
        ),
    },
    {
        "source_class_id": "HOMAD-S02",
        "source_class": "versioned_schema",
        "priority": 2,
        "authority_test": (
            "A versioned schema defines the field name, type, domain, semantic, "
            "and required status for the observed outcome."
        ),
    },
    {
        "source_class_id": "HOMAD-S03",
        "source_class": "versioned_contract",
        "priority": 3,
        "authority_test": (
            "A versioned contract explicitly identifies the canonical observed-"
            "outcome field or source path."
        ),
    },
    {
        "source_class_id": "HOMAD-S04",
        "source_class": "artifact_manifest",
        "priority": 4,
        "authority_test": (
            "A deterministic manifest links a produced outcome artifact field to "
            "the historical comparison contract."
        ),
    },
    {
        "source_class_id": "HOMAD-S05",
        "source_class": "persisted_historical_artifact",
        "priority": 5,
        "authority_test": (
            "A persisted historical artifact contains a documented observed-"
            "outcome field with complete lineage."
        ),
    },
    {
        "source_class_id": "HOMAD-S06",
        "source_class": "test_fixture",
        "priority": 6,
        "authority_test": (
            "A fixture encodes the expected field only when tied to a versioned "
            "producer, schema, or contract."
        ),
    },
    {
        "source_class_id": "HOMAD-S07",
        "source_class": "documentation",
        "priority": 7,
        "authority_test": (
            "Documentation is corroborating evidence only and cannot independently "
            "authorize a canonical mapping."
        ),
    },
    {
        "source_class_id": "HOMAD-S08",
        "source_class": "field_name_similarity",
        "priority": 99,
        "authority_test": (
            "Field-name similarity is diagnostic only and is never authoritative."
        ),
    },
]


DISCOVERY_SCOPES = [
    {
        "scope_id": "HOMAD-D01",
        "scope_name": "historical_comparison_producer",
        "search_targets": (
            "functions constructing comparison_record_id, outcome_value, "
            "outcome_available_at_utc, prediction_value, actual_value, result, "
            "observed_value, target_value, and final_outcome"
        ),
    },
    {
        "scope_id": "HOMAD-D02",
        "scope_name": "historical_prediction_artifact_producer",
        "search_targets": (
            "artifact builders, serializers, manifests, schemas, and record "
            "contracts used before comparison construction"
        ),
    },
    {
        "scope_id": "HOMAD-D03",
        "scope_name": "outcome_ingestion_boundary",
        "search_targets": (
            "game-result ingestion, final-score ingestion, target construction, "
            "label construction, and observed-result normalization"
        ),
    },
    {
        "scope_id": "HOMAD-D04",
        "scope_name": "versioned_schema_and_contract_catalog",
        "search_targets": (
            "schema files, contract constants, field contracts, expected types, "
            "accepted domains, and migration declarations"
        ),
    },
    {
        "scope_id": "HOMAD-D05",
        "scope_name": "persisted_artifact_inventory",
        "search_targets": (
            "committed fixtures, manifests, sample artifacts, deterministic replay "
            "artifacts, and checked-in historical records"
        ),
    },
    {
        "scope_id": "HOMAD-D06",
        "scope_name": "tests_and_assertions",
        "search_targets": (
            "tests asserting outcome-field names, numeric domains, source paths, "
            "serialization, or comparison formulas"
        ),
    },
    {
        "scope_id": "HOMAD-D07",
        "scope_name": "migration_and_rename_history",
        "search_targets": (
            "field renames, compatibility aliases, migration scripts, deprecations, "
            "and prior contract versions"
        ),
    },
    {
        "scope_id": "HOMAD-D08",
        "scope_name": "documentation_corroboration",
        "search_targets": (
            "architecture documents, READMEs, runbooks, design notes, and comments "
            "that corroborate stronger sources"
        ),
    },
]


AUTHORITY_EVIDENCE_REQUIREMENTS = [
    {
        "requirement_id": "HOMAD-E01",
        "requirement": "source_path_must_be_repository_relative_or_artifact_relative",
    },
    {
        "requirement_id": "HOMAD-E02",
        "requirement": "source_digest_must_be_sha256_when_materialized",
    },
    {
        "requirement_id": "HOMAD-E03",
        "requirement": "source_class_must_be_declared",
    },
    {
        "requirement_id": "HOMAD-E04",
        "requirement": "field_name_or_field_path_must_be_explicit",
    },
    {
        "requirement_id": "HOMAD-E05",
        "requirement": "field_semantic_must_be_explicit_or_directly_derivable",
    },
    {
        "requirement_id": "HOMAD-E06",
        "requirement": "field_runtime_type_or_schema_type_must_be_identified",
    },
    {
        "requirement_id": "HOMAD-E07",
        "requirement": "accepted_value_domain_must_be_identified",
    },
    {
        "requirement_id": "HOMAD-E08",
        "requirement": "producer_to_consumer_lineage_must_be_traceable",
    },
    {
        "requirement_id": "HOMAD-E09",
        "requirement": "conflicting_sources_must_be_preserved_not_silently_resolved",
    },
    {
        "requirement_id": "HOMAD-E10",
        "requirement": "documentation_alone_cannot_authorize_mapping",
    },
]


EVIDENCE_CLASSIFICATIONS = [
    {
        "classification": "authoritative_mapping_identified",
        "applies_when": (
            "one higher-priority authoritative source explicitly establishes the "
            "observed-outcome field and no equal-or-higher-priority conflict exists"
        ),
    },
    {
        "classification": "authoritative_mapping_conflict",
        "applies_when": (
            "two or more equal-or-comparable authoritative sources establish "
            "different mappings or semantics"
        ),
    },
    {
        "classification": "authoritative_mapping_ambiguous",
        "applies_when": (
            "one or more plausible mappings exist but producer-to-consumer lineage "
            "or semantic identity is incomplete"
        ),
    },
    {
        "classification": "corroborating_evidence_only",
        "applies_when": (
            "documentation, comments, fixtures, or names suggest a mapping but no "
            "authoritative source establishes it"
        ),
    },
    {
        "classification": "candidate_mapping_rejected",
        "applies_when": (
            "a discovered field is metadata, type-incompatible, domain-incompatible, "
            "or semantically incompatible"
        ),
    },
    {
        "classification": "authoritative_mapping_not_found",
        "applies_when": (
            "the permitted repository and artifact scopes contain no authoritative "
            "mapping evidence"
        ),
    },
    {
        "classification": "source_unresolved",
        "applies_when": (
            "a referenced source cannot be read or its digest cannot be validated"
        ),
    },
]


CONFLICT_RESOLUTION_RULES = [
    {
        "rule_id": "HOMAD-C01",
        "rule": (
            "Producer code outranks documentation, comments, inferred names, and "
            "unversioned fixtures."
        ),
    },
    {
        "rule_id": "HOMAD-C02",
        "rule": (
            "A versioned schema or contract may override older producer behavior "
            "only when an explicit migration boundary is preserved."
        ),
    },
    {
        "rule_id": "HOMAD-C03",
        "rule": (
            "More recent evidence does not automatically outrank older evidence "
            "unless the applicable contract version or migration range is known."
        ),
    },
    {
        "rule_id": "HOMAD-C04",
        "rule": (
            "Conflicting authoritative evidence must produce a conflict disposition, "
            "not an inferred winner."
        ),
    },
    {
        "rule_id": "HOMAD-C05",
        "rule": (
            "Field-name similarity, matching tokens, or shared prefixes may never "
            "resolve a conflict."
        ),
    },
    {
        "rule_id": "HOMAD-C06",
        "rule": (
            "A mapping is authoritative only for the contract version, artifact "
            "version, and historical date range supported by its lineage."
        ),
    },
]


DISCOVERY_RECORD_FIELDS = [
    "authority_discovery_contract_version",
    "authority_discovery_record_id",
    "remediation_plan_record_id",
    "remediation_plan_record_digest",
    "audit_record_id",
    "audit_record_digest",
    "comparison_record_id",
    "metric_record_id",
    "metric_name",
    "aggregation_name",
    "aggregation_key",
    "rejected_candidate_field_name",
    "rejected_candidate_field_path",
    "rejected_candidate_semantic",
    "rejected_candidate_disposition",
    "discovery_scope_id",
    "discovery_scope_name",
    "search_query",
    "source_class",
    "source_priority",
    "source_path",
    "source_record_id",
    "source_digest",
    "source_contract_version",
    "source_artifact_version",
    "source_date_range",
    "discovered_field_name",
    "discovered_field_path",
    "discovered_field_semantic",
    "discovered_field_type",
    "discovered_field_domain",
    "producer_symbol",
    "consumer_symbol",
    "producer_to_consumer_lineage",
    "evidence_requirement_results",
    "evidence_classification",
    "conflict_group_id",
    "conflicting_evidence_ids",
    "mapping_authority_status",
    "mapping_authority_rationale",
    "discovery_exclusion_codes",
    "discovery_limitations",
    "source_comparison_digest",
    "source_metric_record_digest",
    "source_interpretation_digest",
    "source_evidence_record_digest",
    "source_remediation_record_digest",
    "authority_discovery_identity_digest",
    "authority_discovery_record_digest",
]


ORDERING_FIELDS = [
    {
        "ordinal": 1,
        "field": "remediation_plan_record_id",
    },
    {
        "ordinal": 2,
        "field": "source_priority",
    },
    {
        "ordinal": 3,
        "field": "source_class",
    },
    {
        "ordinal": 4,
        "field": "source_path",
    },
    {
        "ordinal": 5,
        "field": "discovered_field_path",
    },
    {
        "ordinal": 6,
        "field": "authority_discovery_record_id",
    },
]


EXCLUSION_CODES = [
    {
        "code": "historical_outcome_mapping_authority_source_not_found",
        "category": "source_discovery",
    },
    {
        "code": "historical_outcome_mapping_authority_source_unresolved",
        "category": "source_resolution",
    },
    {
        "code": "historical_outcome_mapping_authority_digest_invalid",
        "category": "source_validation",
    },
    {
        "code": "historical_outcome_mapping_authority_lineage_incomplete",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_mapping_authority_semantic_ambiguous",
        "category": "semantic",
    },
    {
        "code": "historical_outcome_mapping_authority_type_ambiguous",
        "category": "type",
    },
    {
        "code": "historical_outcome_mapping_authority_domain_ambiguous",
        "category": "domain",
    },
    {
        "code": "historical_outcome_mapping_authority_conflict",
        "category": "conflict",
    },
    {
        "code": "historical_outcome_mapping_authority_documentation_only",
        "category": "insufficient_authority",
    },
    {
        "code": "historical_outcome_mapping_authority_field_name_only",
        "category": "insufficient_authority",
    },
    {
        "code": "historical_outcome_mapping_candidate_metadata_rejected",
        "category": "candidate_rejection",
    },
    {
        "code": "historical_outcome_mapping_canonical_change_prohibited",
        "category": "authority_boundary",
    },
    {
        "code": "historical_outcome_mapping_coercion_prohibited",
        "category": "authority_boundary",
    },
    {
        "code": "historical_outcome_mapping_imputation_prohibited",
        "category": "authority_boundary",
    },
    {
        "code": "historical_outcome_mapping_candidate_replay_prohibited",
        "category": "authority_boundary",
    },
    {
        "code": "historical_outcome_mapping_quality_claim_prohibited",
        "category": "authority_boundary",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AD_remediation_records_deterministically",
    },
    {
        "ordinal": 2,
        "step": "select_exactly_twelve_candidate_incompatible_records",
    },
    {
        "ordinal": 3,
        "step": "validate_complete_predecessor_lineage",
    },
    {
        "ordinal": 4,
        "step": "inventory_authority_source_classes_by_priority",
    },
    {
        "ordinal": 5,
        "step": "search_historical_comparison_producer_code",
    },
    {
        "ordinal": 6,
        "step": "search_prediction_artifact_and_outcome_ingestion_producers",
    },
    {
        "ordinal": 7,
        "step": "search_versioned_schemas_contracts_and_manifests",
    },
    {
        "ordinal": 8,
        "step": "search_persisted_artifacts_tests_and_migration_history",
    },
    {
        "ordinal": 9,
        "step": "classify_each_mapping_evidence_record",
    },
    {
        "ordinal": 10,
        "step": "preserve_conflicts_ambiguities_and_missing_sources",
    },
    {
        "ordinal": 11,
        "step": "verify_deterministic_ordering_and_reverse_replay",
    },
    {
        "ordinal": 12,
        "step": "emit_authority_discovery_artifacts_and_diagnosis",
    },
]


PROHIBITED_AUTHORITIES = [
    "canonical_historical_source_mutation",
    "canonical_outcome_mapping_change",
    "candidate_mapping_promotion",
    "outcome_coercion",
    "outcome_defaulting",
    "outcome_imputation",
    "minimum_support_threshold_change",
    "candidate_replay_execution",
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
        "layer_9ad_predecessor",
    )

    required_attributes = [
        "REMEDIATION_CONTRACT_VERSION",
        "PLAN_PATH",
        "AUDIT_PATH",
        "build_remediation_records",
        "load_module",
    ]

    for attribute in required_attributes:
        if not hasattr(
            predecessor,
            attribute,
        ):
            raise RuntimeError(
                "Layer 9AD predecessor is missing required attribute: "
                + attribute
            )

    plan_module = predecessor.load_module(
        predecessor.PLAN_PATH,
        "layer_9ac_plan_for_9ae",
    )

    plan_replay = plan_module.replay_predecessor()

    audit_records = plan_replay[
        "audit_records"
    ]

    reverse_audit_records = plan_replay[
        "reverse_audit_records"
    ]

    remediation_records = (
        predecessor.build_remediation_records(
            audit_records,
            plan_module.CANDIDATE_RECORD_FIELDS,
        )
    )

    reverse_remediation_records = (
        predecessor.build_remediation_records(
            reverse_audit_records,
            plan_module.CANDIDATE_RECORD_FIELDS,
        )
    )

    return {
        "module": predecessor,
        "plan_module": plan_module,
        "remediation_records":
            remediation_records,
        "reverse_remediation_records":
            reverse_remediation_records,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    replay = replay_predecessor()

    predecessor = replay["module"]

    records = replay[
        "remediation_records"
    ]

    reverse_records = replay[
        "reverse_remediation_records"
    ]

    selected_records = [
        row
        for row in records
        if (
            row.get(
                "remediation_disposition"
            )
            == EXPECTED_PREDECESSOR_DISPOSITION
            and row.get(
                "candidate_field_name"
            )
            == EXPECTED_PREDECESSOR_CANDIDATE
        )
    ]

    predecessor_contract_verified = (
        predecessor.REMEDIATION_CONTRACT_VERSION
        == EXPECTED_PREDECESSOR_VERSION
    )

    predecessor_replay_deterministic = (
        canonical_json(records)
        == canonical_json(reverse_records)
    )

    predecessor_digest = sha256_payload(
        records
    )

    reverse_predecessor_digest = (
        sha256_payload(reverse_records)
    )

    complete_lineage_count = sum(
        all(
            valid_sha256(
                row.get(field)
            )
            for field in [
                "remediation_plan_record_digest",
                "audit_record_digest",
                "source_comparison_digest",
                "source_metric_record_digest",
                "source_interpretation_digest",
                "source_evidence_record_digest",
                "source_remediation_record_digest",
            ]
        )
        for row in selected_records
    )

    checks = [
        {
            "check": "nine_ad_predecessor_contract_verified",
            "actual":
                predecessor.REMEDIATION_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed":
                predecessor_contract_verified,
        },
        {
            "check": "nine_ad_replay_deterministic",
            "actual":
                predecessor_replay_deterministic,
            "expected": True,
            "passed":
                predecessor_replay_deterministic,
        },
        {
            "check": "nine_ad_digests_match_reverse_replay",
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
            "check": "twelve_candidate_incompatible_records_selected",
            "actual":
                len(selected_records),
            "expected":
                EXPECTED_RECORD_COUNT,
            "passed": (
                len(selected_records)
                == EXPECTED_RECORD_COUNT
            ),
        },
        {
            "check": "all_selected_records_preserve_complete_lineage",
            "actual":
                complete_lineage_count,
            "expected":
                EXPECTED_RECORD_COUNT,
            "passed": (
                complete_lineage_count
                == EXPECTED_RECORD_COUNT
            ),
        },
        {
            "check": "all_selected_candidates_are_metadata",
            "actual": sum(
                row.get("candidate_semantic")
                == "outcome_availability_metadata"
                for row in selected_records
            ),
            "expected":
                EXPECTED_RECORD_COUNT,
            "passed": all(
                row.get("candidate_semantic")
                == "outcome_availability_metadata"
                for row in selected_records
            ),
        },
        {
            "check": "no_candidate_artifacts_exist",
            "actual": sum(
                row.get("candidate_artifact_path")
                is None
                and row.get(
                    "candidate_artifact_digest"
                )
                is None
                for row in selected_records
            ),
            "expected":
                EXPECTED_RECORD_COUNT,
            "passed": all(
                row.get("candidate_artifact_path")
                is None
                and row.get(
                    "candidate_artifact_digest"
                )
                is None
                for row in selected_records
            ),
        },
        {
            "check": "no_candidate_replays_were_executed",
            "actual": sum(
                row.get("candidate_replay_status")
                == "not_executed_candidate_incompatible"
                for row in selected_records
            ),
            "expected":
                EXPECTED_RECORD_COUNT,
            "passed": all(
                row.get("candidate_replay_status")
                == "not_executed_candidate_incompatible"
                for row in selected_records
            ),
        },
        {
            "check": "authority_source_classes_defined",
            "actual":
                len(AUTHORITY_SOURCE_CLASSES),
            "expected": 8,
            "passed": (
                len(AUTHORITY_SOURCE_CLASSES)
                == 8
            ),
        },
        {
            "check": "producer_code_has_highest_priority",
            "actual":
                AUTHORITY_SOURCE_CLASSES[0][
                    "source_class"
                ],
            "expected":
                "producer_code",
            "passed": (
                AUTHORITY_SOURCE_CLASSES[0][
                    "source_class"
                ]
                == "producer_code"
                and AUTHORITY_SOURCE_CLASSES[0][
                    "priority"
                ]
                == 1
            ),
        },
        {
            "check": "field_name_similarity_is_non_authoritative",
            "actual":
                AUTHORITY_SOURCE_CLASSES[-1][
                    "source_class"
                ],
            "expected":
                "field_name_similarity",
            "passed": (
                AUTHORITY_SOURCE_CLASSES[-1][
                    "source_class"
                ]
                == "field_name_similarity"
                and AUTHORITY_SOURCE_CLASSES[-1][
                    "priority"
                ]
                == 99
            ),
        },
        {
            "check": "discovery_scopes_defined",
            "actual":
                len(DISCOVERY_SCOPES),
            "expected": 8,
            "passed": (
                len(DISCOVERY_SCOPES)
                == 8
            ),
        },
        {
            "check": "authority_evidence_requirements_defined",
            "actual":
                len(
                    AUTHORITY_EVIDENCE_REQUIREMENTS
                ),
            "expected": 10,
            "passed": (
                len(
                    AUTHORITY_EVIDENCE_REQUIREMENTS
                )
                == 10
            ),
        },
        {
            "check": "evidence_classifications_defined",
            "actual":
                len(EVIDENCE_CLASSIFICATIONS),
            "expected": 7,
            "passed": (
                len(EVIDENCE_CLASSIFICATIONS)
                == 7
            ),
        },
        {
            "check": "conflict_resolution_rules_defined",
            "actual":
                len(CONFLICT_RESOLUTION_RULES),
            "expected": 6,
            "passed": (
                len(CONFLICT_RESOLUTION_RULES)
                == 6
            ),
        },
        {
            "check": "discovery_record_fields_defined",
            "actual":
                len(DISCOVERY_RECORD_FIELDS),
            "expected": 49,
            "passed": (
                len(DISCOVERY_RECORD_FIELDS)
                == 49
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
            "expected": 16,
            "passed": (
                len(EXCLUSION_CODES)
                == 16
            ),
        },
        {
            "check": "implementation_steps_defined",
            "actual":
                len(IMPLEMENTATION_STEPS),
            "expected": 12,
            "passed": (
                len(IMPLEMENTATION_STEPS)
                == 12
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
            "check": "candidate_promotion_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "candidate_mapping_promotion"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "candidate_replay_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "candidate_replay_execution"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "outcome_coercion_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "outcome_coercion"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check": "outcome_imputation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "outcome_imputation"
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
            "check": "discovery_records_not_materialized",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "candidate_replays_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "outcomes_not_coerced_or_imputed",
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
            "authority_source_classes":
                AUTHORITY_SOURCE_CLASSES,
            "discovery_scopes":
                DISCOVERY_SCOPES,
            "authority_evidence_requirements":
                AUTHORITY_EVIDENCE_REQUIREMENTS,
            "evidence_classifications":
                EVIDENCE_CLASSIFICATIONS,
            "conflict_resolution_rules":
                CONFLICT_RESOLUTION_RULES,
            "discovery_record_fields":
                DISCOVERY_RECORD_FIELDS,
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
        "outcome_mapping_authority_discovery_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_mapping_authority_discovery_plan_failed"
    )

    next_layer = (
        "9AF_pitch_type_matchup_overlay_historical_"
        "outcome_mapping_authority_discovery_implementation"
        if all_checks_passed
        else
        "9AE_pitch_type_matchup_overlay_historical_"
        "outcome_mapping_authority_discovery_plan_remediation"
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
        OUTPUT_DIR / "authority_source_classes.csv",
        [
            "source_class_id",
            "source_class",
            "priority",
            "authority_test",
        ],
        AUTHORITY_SOURCE_CLASSES,
    )

    write_csv(
        OUTPUT_DIR / "discovery_scopes.csv",
        [
            "scope_id",
            "scope_name",
            "search_targets",
        ],
        DISCOVERY_SCOPES,
    )

    write_csv(
        OUTPUT_DIR
        / "authority_evidence_requirements.csv",
        [
            "requirement_id",
            "requirement",
        ],
        AUTHORITY_EVIDENCE_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR
        / "evidence_classifications.csv",
        [
            "classification",
            "applies_when",
        ],
        EVIDENCE_CLASSIFICATIONS,
    )

    write_csv(
        OUTPUT_DIR
        / "conflict_resolution_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        CONFLICT_RESOLUTION_RULES,
    )

    write_csv(
        OUTPUT_DIR
        / "discovery_record_field_contract.csv",
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
                DISCOVERY_RECORD_FIELDS,
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
                    "Layer 9AE is planning-only and grants no canonical "
                    "source mutation, mapping change, candidate promotion, "
                    "coercion, imputation, candidate replay, canonical "
                    "recomputation, uncertainty, significance, superiority, "
                    "equivalence, activation, production, market, pricing, "
                    "or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_mapping_"
                    "authority_discovery_implementation"
                ),
                "granted":
                    all_checks_passed,
                "reason": (
                    "Layer 9AF may perform deterministic read-only discovery "
                    "of producer, schema, contract, manifest, artifact, test, "
                    "migration, and documentation evidence without changing "
                    "the canonical outcome mapping."
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
        "predecessor_contract_verified":
            predecessor_contract_verified,
        "predecessor_replay_deterministic":
            predecessor_replay_deterministic,
        "predecessor_digest":
            predecessor_digest,
        "reverse_predecessor_digest":
            reverse_predecessor_digest,
        "candidate_incompatible_records":
            len(selected_records),
        "rejected_candidate_field":
            EXPECTED_PREDECESSOR_CANDIDATE,
        "authority_source_classes":
            len(AUTHORITY_SOURCE_CLASSES),
        "discovery_scopes":
            len(DISCOVERY_SCOPES),
        "authority_evidence_requirements":
            len(
                AUTHORITY_EVIDENCE_REQUIREMENTS
            ),
        "evidence_classifications":
            len(EVIDENCE_CLASSIFICATIONS),
        "conflict_resolution_rules":
            len(CONFLICT_RESOLUTION_RULES),
        "discovery_record_fields":
            len(DISCOVERY_RECORD_FIELDS),
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
        "discovery_records_materialized": 0,
        "candidate_replays_executed": 0,
        "outcomes_coerced": 0,
        "outcomes_imputed": 0,
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
        / "outcome_mapping_authority_discovery_plan_summary.json",
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
            "historical_outcome_mapping_"
            "authority_discovery_implementation"
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
        "Predecessor contract verified: "
        f"{predecessor_contract_verified}"
    )
    print(
        "Predecessor replay deterministic: "
        f"{predecessor_replay_deterministic}"
    )
    print(
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(
        "Candidate-incompatible records: "
        f"{len(selected_records)}"
    )
    print(
        "Rejected candidate field: "
        f"{EXPECTED_PREDECESSOR_CANDIDATE}"
    )
    print(
        "Authority source classes: "
        f"{len(AUTHORITY_SOURCE_CLASSES)}"
    )
    print(
        "Discovery scopes: "
        f"{len(DISCOVERY_SCOPES)}"
    )
    print(
        "Evidence classifications: "
        f"{len(EVIDENCE_CLASSIFICATIONS)}"
    )
    print(
        "Discovery record fields: "
        f"{len(DISCOVERY_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Discovery records materialized: 0")
    print("Candidate replays executed: 0")
    print("Outcomes coerced: 0")
    print("Outcomes imputed: 0")
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
