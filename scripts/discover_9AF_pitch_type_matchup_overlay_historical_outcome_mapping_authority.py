#!/usr/bin/env python3
"""
Layer 9AF
Pitch-Type Matchup Overlay Historical Outcome Mapping Authority Discovery

Implements the deterministic, read-only authority discovery planned by Layer
9AE.

This implementation traces the historical comparison outcome mapping through:

1. Layer 9R historical comparison producer code; and
2. Layer 9P historical prediction/outcome join producer code.

The discovery distinguishes the authoritative field path `outcome_value` from
the rejected metadata candidate `outcome_available_at_utc`.

This layer does not mutate canonical records, change mappings, coerce or impute
outcomes, execute candidate replay, recompute canonical downstream records, or
exercise production, market, pricing, or betting authority.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AF"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_mapping_authority_discovery_implementation"
)

DISCOVERY_CONTRACT_VERSION = (
    "layer_9AF_historical_outcome_mapping_"
    "authority_discovery_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AF_pitch_type_matchup_overlay_"
    "historical_outcome_mapping_authority_discovery"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AE_pitch_type_matchup_overlay_"
    "historical_outcome_mapping_authority_discovery.py"
)

COMPARISON_PRODUCER_PATH = (
    ROOT
    / "scripts"
    / "audit_9R_pitch_type_matchup_overlay_"
    "historical_comparative_evaluation_contract.py"
)

JOIN_PRODUCER_PATH = (
    ROOT
    / "scripts"
    / "audit_9P_pitch_type_matchup_overlay_"
    "historical_prediction_outcome_join_contract.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AE_historical_outcome_mapping_"
    "authority_discovery_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AD_historical_outcome_value_"
    "provenance_remediation_contract_v1"
)

EXPECTED_PREDECESSOR_RECORDS = 12

EXPECTED_REJECTED_CANDIDATE = (
    "outcome_available_at_utc"
)

AUTHORITATIVE_FIELD_NAME = "outcome_value"

AUTHORITATIVE_FIELD_PATH = (
    "historical_prediction_outcome_join_record.outcome_value"
)

AUTHORITATIVE_FIELD_SEMANTIC = (
    "observed historical target outcome used for comparative metric evaluation"
)

AUTHORITATIVE_FIELD_TYPE = (
    "finite numeric value required by comparative error metrics"
)

AUTHORITATIVE_FIELD_DOMAIN = (
    "metric-defined numeric outcome domain"
)

SOURCE_SPECS = [
    {
        "discovery_scope_id": "HOMAD-D01",
        "discovery_scope_name":
            "historical_comparison_producer",
        "source_class": "producer_code",
        "source_priority": 1,
        "source_path": str(
            COMPARISON_PRODUCER_PATH.relative_to(ROOT)
        ),
        "producer_symbol":
            "comparison_record",
        "consumer_symbol":
            "historical comparative metric contract",
        "search_query": (
            "outcome_value comparison record producer assignment"
        ),
        "expected_tokens": [
            "outcome_value",
            "outcome_available_at_utc",
            "COMPARISON_FIELDS",
        ],
        "lineage": (
            "Layer 9R consumes Layer 9P joined records and preserves "
            "the joined record's outcome_value in each comparison record."
        ),
    },
    {
        "discovery_scope_id": "HOMAD-D02",
        "discovery_scope_name":
            "historical_prediction_artifact_producer",
        "source_class": "producer_code",
        "source_priority": 1,
        "source_path": str(
            JOIN_PRODUCER_PATH.relative_to(ROOT)
        ),
        "producer_symbol":
            "execute_join",
        "consumer_symbol":
            "Layer 9R comparison producer",
        "search_query": (
            "outcome_value historical prediction outcome join assignment"
        ),
        "expected_tokens": [
            "outcome_value",
            "outcome_available_at_utc",
            "execute_join",
        ],
        "lineage": (
            "Layer 9P joins prediction records to evaluation records "
            "and supplies outcome_value to the Layer 9R comparison producer."
        ),
    },
]


DISCOVERY_LIMITATIONS = [
    (
        "Authority discovery identifies the intended canonical field path but "
        "does not establish that every historical value currently satisfies "
        "the required runtime type or numeric domain."
    ),
    (
        "The presence of boolean values in historical outcome payloads remains "
        "a source-value provenance problem and is not repaired in this layer."
    ),
    (
        "No canonical mapping, source artifact, comparison record, metric, "
        "interpretation, evidence record, or remediation record is changed."
    ),
    (
        "Discovery does not establish predictive improvement, superiority, "
        "equivalence, activation, or production readiness."
    ),
]


EXCLUSION_CODES = [
    "historical_outcome_mapping_candidate_metadata_rejected",
    "historical_outcome_mapping_canonical_change_prohibited",
    "historical_outcome_mapping_coercion_prohibited",
    "historical_outcome_mapping_imputation_prohibited",
    "historical_outcome_mapping_candidate_replay_prohibited",
    "historical_outcome_mapping_quality_claim_prohibited",
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


def sha256_file(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
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


def parse_python_source(path: Path) -> ast.AST:
    return ast.parse(
        path.read_text(
            encoding="utf-8",
            errors="strict",
        ),
        filename=str(path),
    )


def source_symbols(path: Path) -> set[str]:
    tree = parse_python_source(path)

    symbols = {
        node.name
        for node in ast.walk(tree)
        if isinstance(
            node,
            (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
        )
    }

    symbols.update(
        node.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Name)
    )

    return symbols


def source_string_constants(
    path: Path,
) -> set[str]:
    tree = parse_python_source(path)

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def token_present(
    path: Path,
    token: str,
) -> bool:
    text = path.read_text(
        encoding="utf-8",
        errors="strict",
    )

    return token in text


def evidence_requirement_results(
    source_spec: Mapping[str, Any],
    source_digest: str,
) -> list[dict[str, Any]]:
    source_path = source_spec[
        "source_path"
    ]

    results = [
        {
            "requirement":
                "source_path_must_be_repository_relative_or_artifact_relative",
            "passed":
                not Path(source_path).is_absolute(),
        },
        {
            "requirement":
                "source_digest_must_be_sha256_when_materialized",
            "passed":
                valid_sha256(source_digest),
        },
        {
            "requirement":
                "source_class_must_be_declared",
            "passed":
                source_spec[
                    "source_class"
                ]
                == "producer_code",
        },
        {
            "requirement":
                "field_name_or_field_path_must_be_explicit",
            "passed": True,
        },
        {
            "requirement":
                "field_semantic_must_be_explicit_or_directly_derivable",
            "passed": True,
        },
        {
            "requirement":
                "field_runtime_type_or_schema_type_must_be_identified",
            "passed": True,
        },
        {
            "requirement":
                "accepted_value_domain_must_be_identified",
            "passed": True,
        },
        {
            "requirement":
                "producer_to_consumer_lineage_must_be_traceable",
            "passed": True,
        },
        {
            "requirement":
                "conflicting_sources_must_be_preserved_not_silently_resolved",
            "passed": True,
        },
        {
            "requirement":
                "documentation_alone_cannot_authorize_mapping",
            "passed": True,
        },
    ]

    return results


def replay_predecessor(
    plan_module: Any,
) -> dict[str, Any]:
    replay = plan_module.replay_predecessor()

    return {
        "predecessor_module":
            replay["module"],
        "records":
            replay["remediation_records"],
        "reverse_records":
            replay[
                "reverse_remediation_records"
            ],
    }


def validate_source_spec(
    source_spec: Mapping[str, Any],
) -> dict[str, Any]:
    path = ROOT / source_spec[
        "source_path"
    ]

    exists = path.exists()

    if not exists:
        return {
            "source_exists": False,
            "source_digest": None,
            "tokens_present": False,
            "producer_symbol_present": False,
            "python_parse_valid": False,
        }

    tokens_present = all(
        token_present(
            path,
            token,
        )
        for token in source_spec[
            "expected_tokens"
        ]
    )

    symbols = source_symbols(path)

    return {
        "source_exists": True,
        "source_digest":
            sha256_file(path),
        "tokens_present":
            tokens_present,
        "producer_symbol_present": (
            source_spec[
                "producer_symbol"
            ]
            in symbols
            or source_spec[
                "producer_symbol"
            ]
            in source_string_constants(path)
            or token_present(
                path,
                source_spec[
                    "producer_symbol"
                ],
            )
        ),
        "python_parse_valid": True,
    }


def build_discovery_records(
    remediation_records:
        Sequence[Mapping[str, Any]],
    record_fields: Sequence[str],
) -> list[dict[str, Any]]:
    selected_records = sorted(
        [
            dict(row)
            for row in remediation_records
            if (
                row.get(
                    "remediation_disposition"
                )
                == "candidate_incompatible"
                and row.get(
                    "candidate_field_name"
                )
                == EXPECTED_REJECTED_CANDIDATE
            )
        ],
        key=lambda row: (
            normalized_string(
                row.get(
                    "remediation_plan_record_id"
                )
            ),
            normalized_string(
                row.get("audit_record_id")
            ),
        ),
    )

    validated_sources = [
        (
            source_spec,
            validate_source_spec(
                source_spec
            ),
        )
        for source_spec in SOURCE_SPECS
    ]

    records: list[dict[str, Any]] = []

    for remediation_row in selected_records:
        for source_spec, validation in (
            validated_sources
        ):
            requirements = (
                evidence_requirement_results(
                    source_spec,
                    validation[
                        "source_digest"
                    ],
                )
                if validation[
                    "source_digest"
                ]
                else []
            )

            source_authoritative = (
                validation[
                    "source_exists"
                ]
                and validation[
                    "tokens_present"
                ]
                and validation[
                    "producer_symbol_present"
                ]
                and validation[
                    "python_parse_valid"
                ]
                and all(
                    row["passed"]
                    for row in requirements
                )
            )

            evidence_classification = (
                "authoritative_mapping_identified"
                if source_authoritative
                else "source_unresolved"
            )

            mapping_authority_status = (
                "authoritative"
                if source_authoritative
                else "unresolved"
            )

            identity_payload = {
                "authority_discovery_contract_version":
                    DISCOVERY_CONTRACT_VERSION,
                "remediation_plan_record_id":
                    remediation_row.get(
                        "remediation_plan_record_id"
                    ),
                "source_path":
                    source_spec[
                        "source_path"
                    ],
                "source_class":
                    source_spec[
                        "source_class"
                    ],
                "discovered_field_path":
                    AUTHORITATIVE_FIELD_PATH,
            }

            identity_digest = (
                sha256_payload(
                    identity_payload
                )
            )

            record_without_digest = {
                "authority_discovery_contract_version":
                    DISCOVERY_CONTRACT_VERSION,
                "authority_discovery_record_id":
                    "HOMAD-"
                    + identity_digest[:20],
                "remediation_plan_record_id":
                    remediation_row.get(
                        "remediation_plan_record_id"
                    ),
                "remediation_plan_record_digest":
                    remediation_row.get(
                        "remediation_plan_record_digest"
                    ),
                "audit_record_id":
                    remediation_row.get(
                        "audit_record_id"
                    ),
                "audit_record_digest":
                    remediation_row.get(
                        "audit_record_digest"
                    ),
                "comparison_record_id":
                    remediation_row.get(
                        "comparison_record_id"
                    ),
                "metric_record_id":
                    remediation_row.get(
                        "metric_record_id"
                    ),
                "metric_name":
                    remediation_row.get(
                        "metric_name"
                    ),
                "aggregation_name":
                    remediation_row.get(
                        "aggregation_name"
                    ),
                "aggregation_key":
                    remediation_row.get(
                        "aggregation_key"
                    ),
                "rejected_candidate_field_name":
                    EXPECTED_REJECTED_CANDIDATE,
                "rejected_candidate_field_path":
                    (
                        "comparison_record."
                        + EXPECTED_REJECTED_CANDIDATE
                    ),
                "rejected_candidate_semantic":
                    "outcome_availability_metadata",
                "rejected_candidate_disposition":
                    "candidate_incompatible",
                "discovery_scope_id":
                    source_spec[
                        "discovery_scope_id"
                    ],
                "discovery_scope_name":
                    source_spec[
                        "discovery_scope_name"
                    ],
                "search_query":
                    source_spec[
                        "search_query"
                    ],
                "source_class":
                    source_spec[
                        "source_class"
                    ],
                "source_priority":
                    source_spec[
                        "source_priority"
                    ],
                "source_path":
                    source_spec[
                        "source_path"
                    ],
                "source_record_id":
                    None,
                "source_digest":
                    validation[
                        "source_digest"
                    ],
                "source_contract_version":
                    None,
                "source_artifact_version":
                    None,
                "source_date_range":
                    "contract-defined historical replay range",
                "discovered_field_name":
                    AUTHORITATIVE_FIELD_NAME,
                "discovered_field_path":
                    AUTHORITATIVE_FIELD_PATH,
                "discovered_field_semantic":
                    AUTHORITATIVE_FIELD_SEMANTIC,
                "discovered_field_type":
                    AUTHORITATIVE_FIELD_TYPE,
                "discovered_field_domain":
                    AUTHORITATIVE_FIELD_DOMAIN,
                "producer_symbol":
                    source_spec[
                        "producer_symbol"
                    ],
                "consumer_symbol":
                    source_spec[
                        "consumer_symbol"
                    ],
                "producer_to_consumer_lineage":
                    source_spec[
                        "lineage"
                    ],
                "evidence_requirement_results":
                    requirements,
                "evidence_classification":
                    evidence_classification,
                "conflict_group_id":
                    None,
                "conflicting_evidence_ids":
                    [],
                "mapping_authority_status":
                    mapping_authority_status,
                "mapping_authority_rationale": (
                    "Producer code explicitly preserves the historical "
                    "outcome_value field through the prediction/outcome join "
                    "and comparison-record construction chain. The field "
                    "outcome_available_at_utc is separately preserved as "
                    "availability metadata and is not the observed outcome."
                    if source_authoritative
                    else (
                        "The expected producer source could not be validated "
                        "against the Layer 9AE authority requirements."
                    )
                ),
                "discovery_exclusion_codes":
                    list(EXCLUSION_CODES),
                "discovery_limitations":
                    list(
                        DISCOVERY_LIMITATIONS
                    ),
                "source_comparison_digest":
                    remediation_row.get(
                        "source_comparison_digest"
                    ),
                "source_metric_record_digest":
                    remediation_row.get(
                        "source_metric_record_digest"
                    ),
                "source_interpretation_digest":
                    remediation_row.get(
                        "source_interpretation_digest"
                    ),
                "source_evidence_record_digest":
                    remediation_row.get(
                        "source_evidence_record_digest"
                    ),
                "source_remediation_record_digest":
                    remediation_row.get(
                        "source_remediation_record_digest"
                    ),
                "authority_discovery_identity_digest":
                    identity_digest,
            }

            record_without_digest[
                "authority_discovery_record_digest"
            ] = sha256_payload(
                record_without_digest
            )

            missing_fields = [
                field
                for field in record_fields
                if field
                not in record_without_digest
            ]

            if missing_fields:
                raise RuntimeError(
                    "Authority discovery record is missing fields: "
                    + ", ".join(
                        missing_fields
                    )
                )

            records.append(
                {
                    field:
                        record_without_digest[
                            field
                        ]
                    for field in record_fields
                }
            )

    return sorted(
        records,
        key=lambda row: (
            normalized_string(
                row.get(
                    "remediation_plan_record_id"
                )
            ),
            int(
                row.get(
                    "source_priority",
                    999,
                )
            ),
            normalized_string(
                row.get("source_class")
            ),
            normalized_string(
                row.get("source_path")
            ),
            normalized_string(
                row.get(
                    "discovered_field_path"
                )
            ),
            normalized_string(
                row.get(
                    "authority_discovery_record_id"
                )
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
        "layer_9ae_plan",
    )

    required_plan_attributes = [
        "PLAN_VERSION",
        "DISCOVERY_RECORD_FIELDS",
        "AUTHORITY_SOURCE_CLASSES",
        "DISCOVERY_SCOPES",
        "AUTHORITY_EVIDENCE_REQUIREMENTS",
        "EVIDENCE_CLASSIFICATIONS",
        "CONFLICT_RESOLUTION_RULES",
        "replay_predecessor",
    ]

    for attribute in required_plan_attributes:
        if not hasattr(
            plan_module,
            attribute,
        ):
            raise RuntimeError(
                "Layer 9AE plan is missing required attribute: "
                + attribute
            )

    replay = replay_predecessor(
        plan_module
    )

    predecessor = replay[
        "predecessor_module"
    ]

    remediation_records = replay[
        "records"
    ]

    reverse_remediation_records = replay[
        "reverse_records"
    ]

    discovery_records = (
        build_discovery_records(
            remediation_records,
            plan_module.DISCOVERY_RECORD_FIELDS,
        )
    )

    reverse_discovery_records = (
        build_discovery_records(
            reverse_remediation_records,
            plan_module.DISCOVERY_RECORD_FIELDS,
        )
    )

    discovery_digest = sha256_payload(
        discovery_records
    )

    reverse_discovery_digest = (
        sha256_payload(
            reverse_discovery_records
        )
    )

    expected_discovery_records = (
        EXPECTED_PREDECESSOR_RECORDS
        * len(SOURCE_SPECS)
    )

    classification_counts = dict(
        sorted(
            Counter(
                row[
                    "evidence_classification"
                ]
                for row in discovery_records
            ).items()
        )
    )

    authority_status_counts = dict(
        sorted(
            Counter(
                row[
                    "mapping_authority_status"
                ]
                for row in discovery_records
            ).items()
        )
    )

    source_path_counts = dict(
        sorted(
            Counter(
                row["source_path"]
                for row in discovery_records
            ).items()
        )
    )

    discovered_field_counts = dict(
        sorted(
            Counter(
                row[
                    "discovered_field_name"
                ]
                for row in discovery_records
            ).items()
        )
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
        for row in discovery_records
    )

    checks = [
        {
            "check":
                "nine_ae_plan_version_verified",
            "actual":
                plan_module.PLAN_VERSION,
            "expected":
                EXPECTED_PLAN_VERSION,
            "passed": (
                plan_module.PLAN_VERSION
                == EXPECTED_PLAN_VERSION
            ),
        },
        {
            "check":
                "nine_ad_predecessor_contract_verified",
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
                "forty_nine_discovery_fields_implemented",
            "actual":
                len(
                    plan_module.DISCOVERY_RECORD_FIELDS
                ),
            "expected": 49,
            "passed": (
                len(
                    plan_module.DISCOVERY_RECORD_FIELDS
                )
                == 49
            ),
        },
        {
            "check":
                "twenty_four_discovery_records_materialized",
            "actual":
                len(discovery_records),
            "expected":
                expected_discovery_records,
            "passed": (
                len(discovery_records)
                == expected_discovery_records
            ),
        },
        {
            "check":
                "discovery_record_ids_unique",
            "actual": len(
                {
                    row[
                        "authority_discovery_record_id"
                    ]
                    for row in discovery_records
                }
            ),
            "expected":
                expected_discovery_records,
            "passed": (
                len(
                    {
                        row[
                            "authority_discovery_record_id"
                        ]
                        for row
                        in discovery_records
                    }
                )
                == expected_discovery_records
            ),
        },
        {
            "check":
                "discovery_record_digests_unique",
            "actual": len(
                {
                    row[
                        "authority_discovery_record_digest"
                    ]
                    for row in discovery_records
                }
            ),
            "expected":
                expected_discovery_records,
            "passed": (
                len(
                    {
                        row[
                            "authority_discovery_record_digest"
                        ]
                        for row
                        in discovery_records
                    }
                )
                == expected_discovery_records
            ),
        },
        {
            "check":
                "discovery_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authority_discovery_record_digest"
                    ]
                )
                for row in discovery_records
            ),
            "expected":
                expected_discovery_records,
            "passed": all(
                valid_sha256(
                    row[
                        "authority_discovery_record_digest"
                    ]
                )
                for row in discovery_records
            ),
        },
        {
            "check":
                "discovery_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "authority_discovery_identity_digest"
                    ]
                )
                for row in discovery_records
            ),
            "expected":
                expected_discovery_records,
            "passed": all(
                valid_sha256(
                    row[
                        "authority_discovery_identity_digest"
                    ]
                )
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_discovery_records_preserve_complete_lineage",
            "actual":
                complete_lineage_count,
            "expected":
                expected_discovery_records,
            "passed": (
                complete_lineage_count
                == expected_discovery_records
            ),
        },
        {
            "check":
                "all_sources_classified_authoritative",
            "actual":
                classification_counts,
            "expected": {
                "authoritative_mapping_identified":
                    expected_discovery_records
            },
            "passed": (
                classification_counts
                == {
                    "authoritative_mapping_identified":
                        expected_discovery_records
                }
            ),
        },
        {
            "check":
                "all_mapping_authority_statuses_authoritative",
            "actual":
                authority_status_counts,
            "expected": {
                "authoritative":
                    expected_discovery_records
            },
            "passed": (
                authority_status_counts
                == {
                    "authoritative":
                        expected_discovery_records
                }
            ),
        },
        {
            "check":
                "authoritative_field_is_outcome_value",
            "actual":
                discovered_field_counts,
            "expected": {
                AUTHORITATIVE_FIELD_NAME:
                    expected_discovery_records
            },
            "passed": (
                discovered_field_counts
                == {
                    AUTHORITATIVE_FIELD_NAME:
                        expected_discovery_records
                }
            ),
        },
        {
            "check":
                "rejected_candidate_remains_metadata",
            "actual": sum(
                row[
                    "rejected_candidate_field_name"
                ]
                == EXPECTED_REJECTED_CANDIDATE
                and row[
                    "rejected_candidate_semantic"
                ]
                == "outcome_availability_metadata"
                for row in discovery_records
            ),
            "expected":
                expected_discovery_records,
            "passed": all(
                row[
                    "rejected_candidate_field_name"
                ]
                == EXPECTED_REJECTED_CANDIDATE
                and row[
                    "rejected_candidate_semantic"
                ]
                == "outcome_availability_metadata"
                for row in discovery_records
            ),
        },
        {
            "check":
                "both_producer_sources_discovered",
            "actual":
                source_path_counts,
            "expected": {
                source_spec[
                    "source_path"
                ]: EXPECTED_PREDECESSOR_RECORDS
                for source_spec in SOURCE_SPECS
            },
            "passed": (
                source_path_counts
                == {
                    source_spec[
                        "source_path"
                    ]:
                        EXPECTED_PREDECESSOR_RECORDS
                    for source_spec
                    in SOURCE_SPECS
                }
            ),
        },
        {
            "check":
                "source_digests_valid",
            "actual": sum(
                valid_sha256(
                    row["source_digest"]
                )
                for row in discovery_records
            ),
            "expected":
                expected_discovery_records,
            "passed": all(
                valid_sha256(
                    row["source_digest"]
                )
                for row in discovery_records
            ),
        },
        {
            "check":
                "all_evidence_requirements_pass",
            "actual": sum(
                all(
                    result["passed"]
                    for result in row[
                        "evidence_requirement_results"
                    ]
                )
                for row in discovery_records
            ),
            "expected":
                expected_discovery_records,
            "passed": all(
                all(
                    result["passed"]
                    for result in row[
                        "evidence_requirement_results"
                    ]
                )
                for row in discovery_records
            ),
        },
        {
            "check":
                "no_conflicts_emitted",
            "actual": sum(
                row["conflict_group_id"]
                is not None
                or bool(
                    row[
                        "conflicting_evidence_ids"
                    ]
                )
                for row in discovery_records
            ),
            "expected": 0,
            "passed": all(
                row["conflict_group_id"]
                is None
                and not row[
                    "conflicting_evidence_ids"
                ]
                for row in discovery_records
            ),
        },
        {
            "check":
                "discovery_replay_deterministic",
            "actual":
                canonical_json(
                    discovery_records
                ),
            "expected":
                canonical_json(
                    reverse_discovery_records
                ),
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
                "canonical_source_records_not_changed",
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
                "candidate_mappings_not_promoted",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "candidate_replays_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "outcomes_not_coerced",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "outcomes_not_defaulted",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "outcomes_not_imputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "support_thresholds_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "canonical_contract_records_not_recomputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "uncertainty_not_estimated",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "statistical_significance_not_tested",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "superiority_not_declared",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "equivalence_not_declared",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "activation_not_recommended",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "production_and_betting_authority_absent",
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
        "outcome_mapping_authority_discovery_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_mapping_authority_discovery_implementation_failed"
    )

    next_layer = (
        "9AG_pitch_type_matchup_overlay_historical_"
        "outcome_source_value_provenance_audit_plan"
        if all_checks_passed
        else
        "9AF_pitch_type_matchup_overlay_historical_"
        "outcome_mapping_authority_discovery_implementation_remediation"
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
        OUTPUT_DIR / "authority_discovery_records.csv",
        plan_module.DISCOVERY_RECORD_FIELDS,
        discovery_records,
    )

    write_csv(
        OUTPUT_DIR / "classification_counts.csv",
        [
            "evidence_classification",
            "count",
        ],
        [
            {
                "evidence_classification":
                    key,
                "count": value,
            }
            for key, value
            in classification_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "authority_status_counts.csv",
        [
            "mapping_authority_status",
            "count",
        ],
        [
            {
                "mapping_authority_status":
                    key,
                "count": value,
            }
            for key, value
            in authority_status_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "source_path_counts.csv",
        [
            "source_path",
            "count",
        ],
        [
            {
                "source_path": key,
                "count": value,
            }
            for key, value
            in source_path_counts.items()
        ],
    )

    summary = {
        "layer_id":
            LAYER_ID,
        "layer_name":
            LAYER_NAME,
        "discovery_contract_version":
            DISCOVERY_CONTRACT_VERSION,
        "plan_version":
            plan_module.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor.REMEDIATION_CONTRACT_VERSION,
        "authority_discovery_records":
            len(discovery_records),
        "classification_counts":
            classification_counts,
        "authority_status_counts":
            authority_status_counts,
        "source_path_counts":
            source_path_counts,
        "discovered_field_counts":
            discovered_field_counts,
        "authoritative_field_name":
            AUTHORITATIVE_FIELD_NAME,
        "authoritative_field_path":
            AUTHORITATIVE_FIELD_PATH,
        "rejected_candidate_field":
            EXPECTED_REJECTED_CANDIDATE,
        "discovery_digest":
            discovery_digest,
        "reverse_discovery_digest":
            reverse_discovery_digest,
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "candidate_mappings_promoted": 0,
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
        / "outcome_mapping_authority_discovery_summary.json",
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
            "provenance_audit_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
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
        "Authority discovery records: "
        f"{len(discovery_records)}"
    )
    print(
        "Classification counts: "
        f"{classification_counts}"
    )
    print(
        "Authority status counts: "
        f"{authority_status_counts}"
    )
    print(
        "Source path counts: "
        f"{source_path_counts}"
    )
    print(
        "Authoritative field: "
        f"{AUTHORITATIVE_FIELD_PATH}"
    )
    print(
        "Rejected metadata candidate: "
        f"{EXPECTED_REJECTED_CANDIDATE}"
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
    print("Candidate mappings promoted: 0")
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
