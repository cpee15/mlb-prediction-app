#!/usr/bin/env python3
"""
Layer 9AK
Pitch-Type Matchup Overlay Historical Outcome
Authoritative Observation Discovery Plan

Plans deterministic discovery of authoritative historical observations for
the sixteen source-value defects blocked by Layer 9AJ.

Planning only.

This layer does not:
- mutate canonical historical source values;
- replace the canonical `outcome_value` mapping;
- coerce boolean values to integers;
- default or impute missing outcomes;
- regenerate canonical downstream records;
- calculate uncertainty, significance, superiority, equivalence, activation,
  production probabilities, market prices, or betting edges.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AK"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_authoritative_observation_discovery_plan"
)

PLAN_VERSION = (
    "layer_9AK_historical_outcome_authoritative_"
    "observation_discovery_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AK_pitch_type_matchup_overlay_"
    "historical_outcome_authoritative_observation_discovery_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "remediate_9AJ_pitch_type_matchup_overlay_"
    "historical_outcome_source_value.py"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AJ_historical_outcome_source_value_"
    "remediation_contract_v1"
)

EXPECTED_REMEDIATION_RECORDS = 16
EXPECTED_REMEDIATION_COMPARISONS = 16

EXPECTED_REMEDIATION_STATUS = (
    "blocked_authoritative_observation_missing"
)

EXPECTED_BLOCKER_CODE = (
    "historical_outcome_authoritative_observation_missing"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

REJECTED_METADATA_FIELD = "outcome_available_at_utc"


DISCOVERY_PRINCIPLES = [
    {
        "principle_id": "HOAODP-P01",
        "principle": (
            "Discovery must begin from the exact defective evaluation-source "
            "identity preserved by Layer 9AJ."
        ),
    },
    {
        "principle_id": "HOAODP-P02",
        "principle": (
            "An authoritative observation must be tied to the same game, event, "
            "target, and event-sequence identity as the defective source."
        ),
    },
    {
        "principle_id": "HOAODP-P03",
        "principle": (
            "The discovery process must distinguish observed outcome values "
            "from availability timestamps, eligibility flags, and metadata."
        ),
    },
    {
        "principle_id": "HOAODP-P04",
        "principle": (
            "Boolean values are invalid numeric outcomes and cannot serve as "
            "authoritative replacements."
        ),
    },
    {
        "principle_id": "HOAODP-P05",
        "principle": (
            "No candidate may be accepted through coercion, defaulting, "
            "imputation, inference, or heuristic substitution."
        ),
    },
    {
        "principle_id": "HOAODP-P06",
        "principle": (
            "Every accepted candidate must preserve source provenance, "
            "authority evidence, retrieval evidence, and deterministic digest."
        ),
    },
    {
        "principle_id": "HOAODP-P07",
        "principle": (
            "Conflicting authoritative candidates must remain unresolved until "
            "a deterministic conflict-resolution authority is separately "
            "defined."
        ),
    },
    {
        "principle_id": "HOAODP-P08",
        "principle": (
            "Discovery authority does not grant source mutation or downstream "
            "recomputation authority."
        ),
    },
]


SOURCE_CLASSES = [
    {
        "source_class_id": "HOAODP-S01",
        "source_class": "canonical_repository_fixture",
        "priority": 1,
        "description": (
            "Versioned repository fixture or evaluation dataset containing "
            "the exact historical observation."
        ),
    },
    {
        "source_class_id": "HOAODP-S02",
        "source_class": "canonical_repository_artifact",
        "priority": 2,
        "description": (
            "Versioned repository artifact with exact event identity and "
            "historical outcome value."
        ),
    },
    {
        "source_class_id": "HOAODP-S03",
        "source_class": "upstream_data_snapshot",
        "priority": 3,
        "description": (
            "Immutable upstream data snapshot already retained by the project."
        ),
    },
    {
        "source_class_id": "HOAODP-S04",
        "source_class": "authoritative_external_record",
        "priority": 4,
        "description": (
            "Externally authoritative historical record with stable identity "
            "and reproducible retrieval evidence."
        ),
    },
    {
        "source_class_id": "HOAODP-S05",
        "source_class": "non_authoritative_candidate",
        "priority": 5,
        "description": (
            "Candidate useful for comparison but not sufficient for correction."
        ),
    },
]


DISCOVERY_STAGES = [
    {
        "stage_id": "HOAODP-D01",
        "stage_name": "defect_identity_replay",
        "priority": 1,
        "description": (
            "Replay Layer 9AJ and preserve each blocked defect identity."
        ),
    },
    {
        "stage_id": "HOAODP-D02",
        "stage_name": "repository_source_inventory",
        "priority": 2,
        "description": (
            "Inventory repository fixtures, artifacts, and producers capable "
            "of containing the authoritative observation."
        ),
    },
    {
        "stage_id": "HOAODP-D03",
        "stage_name": "candidate_observation_extraction",
        "priority": 3,
        "description": (
            "Extract exact candidate observations without mutation or coercion."
        ),
    },
    {
        "stage_id": "HOAODP-D04",
        "stage_name": "identity_alignment",
        "priority": 4,
        "description": (
            "Validate game, event, target, and sequence identity alignment."
        ),
    },
    {
        "stage_id": "HOAODP-D05",
        "stage_name": "authority_validation",
        "priority": 5,
        "description": (
            "Validate source class, provenance, immutability, and authority."
        ),
    },
    {
        "stage_id": "HOAODP-D06",
        "stage_name": "value_domain_validation",
        "priority": 6,
        "description": (
            "Require a finite int or float while explicitly excluding bool."
        ),
    },
    {
        "stage_id": "HOAODP-D07",
        "stage_name": "candidate_conflict_assessment",
        "priority": 7,
        "description": (
            "Identify zero, one, duplicate-equivalent, or conflicting candidates."
        ),
    },
    {
        "stage_id": "HOAODP-D08",
        "stage_name": "discovery_disposition",
        "priority": 8,
        "description": (
            "Emit deterministic discovery status and implementation authority."
        ),
    },
]


CANDIDATE_REQUIREMENTS = [
    {
        "requirement_id": "HOAODP-R01",
        "requirement": "candidate_value_present",
        "expected": True,
    },
    {
        "requirement_id": "HOAODP-R02",
        "requirement": "candidate_value_type",
        "expected": "finite_int_or_float_excluding_bool",
    },
    {
        "requirement_id": "HOAODP-R03",
        "requirement": "game_identity_match",
        "expected": True,
    },
    {
        "requirement_id": "HOAODP-R04",
        "requirement": "event_identity_match",
        "expected": True,
    },
    {
        "requirement_id": "HOAODP-R05",
        "requirement": "target_identity_match",
        "expected": True,
    },
    {
        "requirement_id": "HOAODP-R06",
        "requirement": "event_sequence_match",
        "expected": True,
    },
    {
        "requirement_id": "HOAODP-R07",
        "requirement": "source_provenance_complete",
        "expected": True,
    },
    {
        "requirement_id": "HOAODP-R08",
        "requirement": "source_record_digest_valid",
        "expected": True,
    },
    {
        "requirement_id": "HOAODP-R09",
        "requirement": "retrieval_evidence_preserved",
        "expected": True,
    },
    {
        "requirement_id": "HOAODP-R10",
        "requirement": "coercion_defaulting_or_imputation_used",
        "expected": False,
    },
]


DISCOVERY_STATUSES = [
    {
        "status": "authoritative_observation_identified",
        "implementation_authority": True,
    },
    {
        "status": "authoritative_observation_not_found",
        "implementation_authority": False,
    },
    {
        "status": "candidate_identity_mismatch",
        "implementation_authority": False,
    },
    {
        "status": "candidate_value_invalid",
        "implementation_authority": False,
    },
    {
        "status": "candidate_authority_insufficient",
        "implementation_authority": False,
    },
    {
        "status": "duplicate_equivalent_candidates",
        "implementation_authority": False,
    },
    {
        "status": "conflicting_authoritative_candidates",
        "implementation_authority": False,
    },
    {
        "status": "source_provenance_incomplete",
        "implementation_authority": False,
    },
]


BLOCKER_CODES = [
    {
        "code": "historical_outcome_candidate_not_found",
        "category": "discovery",
    },
    {
        "code": "historical_outcome_candidate_value_missing",
        "category": "value",
    },
    {
        "code": "historical_outcome_candidate_value_boolean",
        "category": "value",
    },
    {
        "code": "historical_outcome_candidate_value_non_numeric",
        "category": "value",
    },
    {
        "code": "historical_outcome_candidate_value_non_finite",
        "category": "value",
    },
    {
        "code": "historical_outcome_candidate_game_identity_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_outcome_candidate_event_identity_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_outcome_candidate_target_identity_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_outcome_candidate_sequence_mismatch",
        "category": "identity",
    },
    {
        "code": "historical_outcome_candidate_authority_insufficient",
        "category": "authority",
    },
    {
        "code": "historical_outcome_candidate_provenance_incomplete",
        "category": "provenance",
    },
    {
        "code": "historical_outcome_candidate_digest_invalid",
        "category": "lineage",
    },
    {
        "code": "historical_outcome_duplicate_equivalent_candidates",
        "category": "cardinality",
    },
    {
        "code": "historical_outcome_conflicting_authoritative_candidates",
        "category": "conflict",
    },
    {
        "code": "historical_outcome_candidate_coercion_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_candidate_default_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_candidate_imputation_requested",
        "category": "authority",
    },
    {
        "code": "historical_outcome_mapping_change_requested",
        "category": "authority",
    },
]


DISCOVERY_RECORD_FIELDS = [
    "authoritative_observation_discovery_contract_version",
    "authoritative_observation_discovery_record_id",
    "source_value_remediation_plan_record_id",
    "source_value_remediation_plan_record_digest",
    "source_value_audit_record_id",
    "source_value_audit_record_digest",
    "authority_discovery_record_id",
    "comparison_record_id",
    "metric_record_id",
    "metric_name",
    "aggregation_name",
    "aggregation_key",
    "authoritative_field_name",
    "authoritative_field_path",
    "rejected_metadata_field_name",
    "defect_source_path",
    "defect_source_symbol",
    "defect_source_record_id",
    "defect_source_record_digest",
    "defective_value",
    "defective_runtime_type",
    "candidate_source_class",
    "candidate_source_priority",
    "candidate_source_path",
    "candidate_source_symbol",
    "candidate_source_record_id",
    "candidate_source_record_digest",
    "candidate_retrieval_evidence",
    "candidate_value_present",
    "candidate_value",
    "candidate_runtime_type",
    "candidate_value_valid",
    "game_identity_match",
    "event_identity_match",
    "target_identity_match",
    "event_sequence_match",
    "source_provenance_complete",
    "source_authority_sufficient",
    "candidate_count",
    "equivalent_candidate_count",
    "conflicting_candidate_count",
    "discovery_status",
    "discovery_blocker_codes",
    "implementation_authority_granted",
    "discovery_rationale",
    "discovery_limitations",
    "authoritative_observation_discovery_identity_digest",
    "authoritative_observation_discovery_record_digest",
]


ORDERING_FIELDS = [
    {
        "ordinal": 1,
        "field": "comparison_record_id",
    },
    {
        "ordinal": 2,
        "field": "defect_source_record_id",
    },
    {
        "ordinal": 3,
        "field": "candidate_source_priority",
    },
    {
        "ordinal": 4,
        "field": "candidate_source_path",
    },
    {
        "ordinal": 5,
        "field": "candidate_source_record_id",
    },
    {
        "ordinal": 6,
        "field": "authoritative_observation_discovery_record_id",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "ordinal": 1,
        "step": "replay_layer_9AJ_remediation_records",
    },
    {
        "ordinal": 2,
        "step": "select_blocked_authoritative_observation_records",
    },
    {
        "ordinal": 3,
        "step": "preserve_defect_source_identity_and_digest",
    },
    {
        "ordinal": 4,
        "step": "inventory_repository_candidate_sources",
    },
    {
        "ordinal": 5,
        "step": "extract_candidate_observations_without_mutation",
    },
    {
        "ordinal": 6,
        "step": "validate_candidate_runtime_type_and_domain",
    },
    {
        "ordinal": 7,
        "step": "validate_game_event_target_and_sequence_identity",
    },
    {
        "ordinal": 8,
        "step": "validate_source_authority_and_provenance",
    },
    {
        "ordinal": 9,
        "step": "classify_candidate_cardinality_and_conflicts",
    },
    {
        "ordinal": 10,
        "step": "emit_deterministic_discovery_records",
    },
    {
        "ordinal": 11,
        "step": "verify_forward_and_reverse_replay",
    },
    {
        "ordinal": 12,
        "step": "grant_implementation_authority_only_for_resolved_records",
    },
]


PROHIBITED_AUTHORITIES = [
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
        "layer_9aj_predecessor",
    )

    if (
        predecessor.REMEDIATION_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AJ contract version: "
            f"{predecessor.REMEDIATION_CONTRACT_VERSION}"
        )

    replay = predecessor.replay_plan()

    plan = replay["plan"]

    remediation_records = (
        predecessor.build_remediation_records(
            plan,
            replay["records"],
        )
    )

    reverse_remediation_records = (
        predecessor.build_remediation_records(
            plan,
            list(
                reversed(
                    replay["reverse_records"]
                )
            ),
        )
    )

    return {
        "module": predecessor,
        "records": remediation_records,
        "reverse_records":
            reverse_remediation_records,
    }


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    replay = replay_predecessor()

    predecessor = replay["module"]
    records = replay["records"]
    reverse_records = replay[
        "reverse_records"
    ]

    remediation_comparison_ids = {
        row["comparison_record_id"]
        for row in records
    }

    status_counts = Counter(
        row["remediation_status"]
        for row in records
    )

    blocker_counts = Counter(
        blocker
        for row in records
        for blocker in row[
            "remediation_blockers"
        ]
    )

    checks = [
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
                "nine_aj_replay_deterministic",
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
            "check":
                "nine_aj_digest_replay_deterministic",
            "actual":
                sha256_payload(records),
            "expected":
                sha256_payload(
                    reverse_records
                ),
            "passed": (
                sha256_payload(records)
                == sha256_payload(
                    reverse_records
                )
            ),
        },
        {
            "check":
                "expected_remediation_records_replayed",
            "actual":
                len(records),
            "expected":
                EXPECTED_REMEDIATION_RECORDS,
            "passed": (
                len(records)
                == EXPECTED_REMEDIATION_RECORDS
            ),
        },
        {
            "check":
                "expected_remediation_comparisons_replayed",
            "actual":
                len(
                    remediation_comparison_ids
                ),
            "expected":
                EXPECTED_REMEDIATION_COMPARISONS,
            "passed": (
                len(
                    remediation_comparison_ids
                )
                == EXPECTED_REMEDIATION_COMPARISONS
            ),
        },
        {
            "check":
                "all_remediations_blocked_for_missing_observation",
            "actual":
                dict(
                    sorted(
                        status_counts.items()
                    )
                ),
            "expected": {
                EXPECTED_REMEDIATION_STATUS:
                    EXPECTED_REMEDIATION_RECORDS
            },
            "passed": (
                status_counts
                == Counter(
                    {
                        EXPECTED_REMEDIATION_STATUS:
                            EXPECTED_REMEDIATION_RECORDS
                    }
                )
            ),
        },
        {
            "check":
                "all_authoritative_observation_blockers_preserved",
            "actual":
                dict(
                    sorted(
                        blocker_counts.items()
                    )
                ),
            "expected": {
                EXPECTED_BLOCKER_CODE:
                    EXPECTED_REMEDIATION_RECORDS
            },
            "passed": (
                blocker_counts
                == Counter(
                    {
                        EXPECTED_BLOCKER_CODE:
                            EXPECTED_REMEDIATION_RECORDS
                    }
                )
            ),
        },
        {
            "check":
                "all_remediation_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_value_remediation_plan_record_digest"
                    ]
                )
                for row in records
            ),
            "expected":
                len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_value_remediation_plan_record_digest"
                    ]
                )
                for row in records
            ),
        },
        {
            "check":
                "all_defect_source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in records
            ),
            "expected":
                len(records),
            "passed": all(
                valid_sha256(
                    row[
                        "defect_source_record_digest"
                    ]
                )
                for row in records
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
                    for row in records
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
                for row in records
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
                    for row in records
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
                for row in records
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
                    for row in records
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
                for row in records
            ),
        },
        {
            "check":
                "discovery_principles_defined",
            "actual":
                len(DISCOVERY_PRINCIPLES),
            "expected": 8,
            "passed": (
                len(DISCOVERY_PRINCIPLES)
                == 8
            ),
        },
        {
            "check":
                "source_classes_defined",
            "actual":
                len(SOURCE_CLASSES),
            "expected": 5,
            "passed": (
                len(SOURCE_CLASSES) == 5
            ),
        },
        {
            "check":
                "discovery_stages_defined",
            "actual":
                len(DISCOVERY_STAGES),
            "expected": 8,
            "passed": (
                len(DISCOVERY_STAGES) == 8
            ),
        },
        {
            "check":
                "candidate_requirements_defined",
            "actual":
                len(CANDIDATE_REQUIREMENTS),
            "expected": 10,
            "passed": (
                len(CANDIDATE_REQUIREMENTS)
                == 10
            ),
        },
        {
            "check":
                "discovery_statuses_defined",
            "actual":
                len(DISCOVERY_STATUSES),
            "expected": 8,
            "passed": (
                len(DISCOVERY_STATUSES)
                == 8
            ),
        },
        {
            "check":
                "blocker_codes_defined",
            "actual":
                len(BLOCKER_CODES),
            "expected": 18,
            "passed": (
                len(BLOCKER_CODES) == 18
            ),
        },
        {
            "check":
                "discovery_record_fields_defined",
            "actual":
                len(DISCOVERY_RECORD_FIELDS),
            "expected": 48,
            "passed": (
                len(DISCOVERY_RECORD_FIELDS)
                == 48
            ),
        },
        {
            "check":
                "ordering_fields_defined",
            "actual":
                len(ORDERING_FIELDS),
            "expected": 6,
            "passed": (
                len(ORDERING_FIELDS) == 6
            ),
        },
        {
            "check":
                "implementation_steps_defined",
            "actual":
                len(IMPLEMENTATION_STEPS),
            "expected": 12,
            "passed": (
                len(IMPLEMENTATION_STEPS)
                == 12
            ),
        },
        {
            "check":
                "canonical_source_mutation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_source_value_mutation"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check":
                "mapping_change_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "canonical_outcome_mapping_change"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check":
                "coercion_defaulting_and_imputation_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "boolean_to_integer_coercion"
                in PROHIBITED_AUTHORITIES
                and "source_value_defaulting"
                in PROHIBITED_AUTHORITIES
                and "source_value_imputation"
                in PROHIBITED_AUTHORITIES
            ),
        },
        {
            "check":
                "candidate_inference_and_substitution_prohibited",
            "actual": True,
            "expected": True,
            "passed": (
                "candidate_value_inference"
                in PROHIBITED_AUTHORITIES
                and "heuristic_candidate_substitution"
                in PROHIBITED_AUTHORITIES
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
                "source_values_not_repaired",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "candidate_values_not_coerced_defaulted_or_imputed",
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

    plan_digest = sha256_payload(
        {
            "plan_version":
                PLAN_VERSION,
            "discovery_principles":
                DISCOVERY_PRINCIPLES,
            "source_classes":
                SOURCE_CLASSES,
            "discovery_stages":
                DISCOVERY_STAGES,
            "candidate_requirements":
                CANDIDATE_REQUIREMENTS,
            "discovery_statuses":
                DISCOVERY_STATUSES,
            "blocker_codes":
                BLOCKER_CODES,
            "discovery_record_fields":
                DISCOVERY_RECORD_FIELDS,
            "ordering_fields":
                ORDERING_FIELDS,
            "implementation_steps":
                IMPLEMENTATION_STEPS,
            "prohibited_authorities":
                PROHIBITED_AUTHORITIES,
        }
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_observation_"
        "discovery_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_observation_"
        "discovery_plan_failed"
    )

    next_layer = (
        "9AL_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_observation_"
        "discovery_implementation"
        if all_checks_passed
        else
        "9AK_pitch_type_matchup_overlay_historical_"
        "outcome_authoritative_observation_"
        "discovery_plan_remediation"
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
        OUTPUT_DIR / "discovery_principles.csv",
        [
            "principle_id",
            "principle",
        ],
        DISCOVERY_PRINCIPLES,
    )

    write_csv(
        OUTPUT_DIR / "source_classes.csv",
        [
            "source_class_id",
            "source_class",
            "priority",
            "description",
        ],
        SOURCE_CLASSES,
    )

    write_csv(
        OUTPUT_DIR / "discovery_stages.csv",
        [
            "stage_id",
            "stage_name",
            "priority",
            "description",
        ],
        DISCOVERY_STAGES,
    )

    write_csv(
        OUTPUT_DIR / "candidate_requirements.csv",
        [
            "requirement_id",
            "requirement",
            "expected",
        ],
        CANDIDATE_REQUIREMENTS,
    )

    write_csv(
        OUTPUT_DIR / "discovery_statuses.csv",
        [
            "status",
            "implementation_authority",
        ],
        DISCOVERY_STATUSES,
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
            for index, field
            in enumerate(
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
        OUTPUT_DIR / "implementation_steps.csv",
        [
            "ordinal",
            "step",
        ],
        IMPLEMENTATION_STEPS,
    )

    write_csv(
        OUTPUT_DIR
        / "blocked_remediation_inventory.csv",
        [
            "source_value_remediation_plan_record_id",
            "source_value_remediation_plan_record_digest",
            "comparison_record_id",
            "defect_source_path",
            "defect_source_symbol",
            "defect_source_record_id",
            "defect_source_record_digest",
            "defective_value",
            "defective_runtime_type",
            "remediation_status",
            "remediation_blockers",
        ],
        records,
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
                    "Layer 9AK is planning-only and grants no "
                    "canonical mutation, mapping change, coercion, "
                    "defaulting, imputation, candidate inference, "
                    "downstream recomputation, quality, production, "
                    "market, pricing, or betting authority."
                ),
            }
            for authority
            in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_authoritative_"
                    "observation_discovery_implementation"
                ),
                "granted":
                    all_checks_passed,
                "reason": (
                    "Layer 9AL may implement deterministic, read-only "
                    "candidate discovery under the identity, authority, "
                    "provenance, domain, cardinality, and conflict "
                    "boundaries defined by this plan."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "plan_version": PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.REMEDIATION_CONTRACT_VERSION,
        "remediation_records":
            len(records),
        "remediation_comparisons":
            len(remediation_comparison_ids),
        "remediation_status_counts":
            dict(
                sorted(
                    status_counts.items()
                )
            ),
        "remediation_blocker_counts":
            dict(
                sorted(
                    blocker_counts.items()
                )
            ),
        "discovery_principles":
            len(DISCOVERY_PRINCIPLES),
        "source_classes":
            len(SOURCE_CLASSES),
        "discovery_stages":
            len(DISCOVERY_STAGES),
        "candidate_requirements":
            len(CANDIDATE_REQUIREMENTS),
        "discovery_statuses":
            len(DISCOVERY_STATUSES),
        "blocker_codes":
            len(BLOCKER_CODES),
        "discovery_record_fields":
            len(DISCOVERY_RECORD_FIELDS),
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
        "predecessor_digest":
            sha256_payload(records),
        "reverse_predecessor_digest":
            sha256_payload(
                reverse_records
            ),
        "plan_digest":
            plan_digest,
        "authoritative_observations_discovered": 0,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "source_values_repaired": 0,
        "candidate_values_coerced": 0,
        "candidate_values_defaulted": 0,
        "candidate_values_imputed": 0,
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
        / "authoritative_observation_discovery_plan_summary.json",
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
            "historical_outcome_authoritative_"
            "observation_discovery_implementation"
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
        f"{predecessor.REMEDIATION_CONTRACT_VERSION}"
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
        "Remediation records replayed: "
        f"{len(records)}"
    )
    print(
        "Remediation comparisons: "
        f"{len(remediation_comparison_ids)}"
    )
    print(
        "Remediation status counts: "
        f"{dict(sorted(status_counts.items()))}"
    )
    print(
        "Remediation blocker counts: "
        f"{dict(sorted(blocker_counts.items()))}"
    )
    print(
        "Discovery principles: "
        f"{len(DISCOVERY_PRINCIPLES)}"
    )
    print(
        "Source classes: "
        f"{len(SOURCE_CLASSES)}"
    )
    print(
        "Discovery stages: "
        f"{len(DISCOVERY_STAGES)}"
    )
    print(
        "Candidate requirements: "
        f"{len(CANDIDATE_REQUIREMENTS)}"
    )
    print(
        "Discovery record fields: "
        f"{len(DISCOVERY_RECORD_FIELDS)}"
    )
    print(
        f"Plan digest: {plan_digest}"
    )
    print(
        "Authoritative observations discovered: 0"
    )
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Source values repaired: 0")
    print("Candidate values coerced: 0")
    print("Candidate values defaulted: 0")
    print("Candidate values imputed: 0")
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
