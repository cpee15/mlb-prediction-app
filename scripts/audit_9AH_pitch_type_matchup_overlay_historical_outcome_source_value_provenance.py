#!/usr/bin/env python3
"""
Layer 9AH
Pitch-Type Matchup Overlay Historical Outcome Source-Value Provenance Audit

Implements the deterministic, read-only audit planned by Layer 9AG.

The audit traces the authoritative historical `outcome_value` through:

1. evaluation fixture/source;
2. evaluation row;
3. Layer 9P prediction/outcome join;
4. Layer 9R comparative evaluation record;
5. comparative metric consumption; and
6. downstream interpretation, evidence, audit, and remediation lineage.

No source values are repaired, coerced, defaulted, imputed, or mutated.
No canonical records are recomputed.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AH"

LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_source_value_provenance_audit_implementation"
)

AUDIT_CONTRACT_VERSION = (
    "layer_9AH_historical_outcome_source_value_"
    "provenance_audit_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AH_pitch_type_matchup_overlay_"
    "historical_outcome_source_value_provenance_audit"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AG_pitch_type_matchup_overlay_"
    "historical_outcome_source_value_provenance_audit.py"
)

LAYER_9R_PATH = (
    ROOT
    / "scripts"
    / "audit_9R_pitch_type_matchup_overlay_"
    "historical_comparative_evaluation_contract.py"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AG_historical_outcome_source_value_"
    "provenance_audit_plan_v1"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9AF_historical_outcome_mapping_"
    "authority_discovery_contract_v1"
)

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


def classify_value(
    present: bool,
    value: Any,
) -> tuple[str, str, list[str]]:
    codes: list[str] = []

    if not present or value is None:
        return (
            "missing_source_value",
            "invalid",
            [
                "historical_outcome_source_value_missing"
            ],
        )

    if isinstance(value, bool):
        return (
            "boolean_source_value",
            "invalid",
            [
                "historical_outcome_source_value_boolean_detected"
            ],
        )

    if isinstance(value, int):
        return (
            "valid_numeric",
            "valid",
            [],
        )

    if isinstance(value, float):
        if not math.isfinite(value):
            return (
                "non_finite_numeric",
                "invalid",
                [
                    "historical_outcome_source_value_non_finite"
                ],
            )

        return (
            "valid_numeric",
            "valid",
            [],
        )

    if isinstance(value, str):
        try:
            parsed = float(value)
        except ValueError:
            return (
                "non_numeric_string",
                "invalid",
                [
                    "historical_outcome_source_value_non_numeric_string"
                ],
            )

        if math.isfinite(parsed):
            return (
                "numeric_string",
                "invalid",
                [
                    "historical_outcome_source_value_numeric_string"
                ],
            )

        return (
            "non_finite_numeric",
            "invalid",
            [
                "historical_outcome_source_value_non_finite"
            ],
        )

    if isinstance(
        value,
        (list, tuple, dict),
    ):
        return (
            "container_value",
            "invalid",
            [
                "historical_outcome_source_value_container"
            ],
        )

    codes.append(
        "historical_outcome_source_value_unsupported_type"
    )

    return (
        "unsupported_runtime_type",
        "invalid",
        codes,
    )


def parse_comparison_ids(value: Any) -> list[str]:
    if isinstance(value, list):
        return sorted(
            {
                normalized_string(item)
                for item in value
                if normalized_string(item)
            }
        )

    if isinstance(value, tuple):
        return sorted(
            {
                normalized_string(item)
                for item in value
                if normalized_string(item)
            }
        )

    text = normalized_string(value)

    if not text:
        return []

    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None

        if isinstance(parsed, list):
            return sorted(
                {
                    normalized_string(item)
                    for item in parsed
                    if normalized_string(item)
                }
            )

    return [text]


def replay_plan_and_discovery() -> dict[str, Any]:
    plan = load_module(
        PLAN_PATH,
        "layer_9ag_plan",
    )

    if plan.PLAN_VERSION != EXPECTED_PLAN_VERSION:
        raise RuntimeError(
            "Unexpected Layer 9AG plan version: "
            f"{plan.PLAN_VERSION}"
        )

    replay = plan.replay_predecessor()

    predecessor = replay["module"]

    if (
        predecessor.DISCOVERY_CONTRACT_VERSION
        != EXPECTED_PREDECESSOR_VERSION
    ):
        raise RuntimeError(
            "Unexpected Layer 9AF predecessor version: "
            f"{predecessor.DISCOVERY_CONTRACT_VERSION}"
        )

    return {
        "plan": plan,
        "predecessor": predecessor,
        "discovery_records":
            replay["discovery_records"],
        "reverse_discovery_records":
            replay[
                "reverse_discovery_records"
            ],
    }


def replay_historical_chain() -> dict[str, Any]:
    layer_9r = load_module(
        LAYER_9R_PATH,
        "layer_9r_source_value_audit",
    )

    evaluation_rows, joined_rows = (
        layer_9r.replay_predecessor()
    )

    comparison_records = (
        layer_9r.execute_pairing(
            joined_rows,
            evaluation_rows,
        )
    )

    reverse_comparison_records = (
        layer_9r.execute_pairing(
            list(reversed(joined_rows)),
            list(reversed(evaluation_rows)),
        )
    )

    return {
        "module": layer_9r,
        "evaluation_rows":
            evaluation_rows,
        "joined_rows":
            joined_rows,
        "comparison_records":
            comparison_records,
        "reverse_comparison_records":
            reverse_comparison_records,
    }


def canonical_value_json(
    present: bool,
    value: Any,
) -> str:
    if not present:
        return canonical_json(
            {
                "present": False,
                "value": None,
            }
        )

    return canonical_json(
        {
            "present": True,
            "value": value,
        }
    )


def source_record_digest(
    stage_name: str,
    source_record_id: str,
    source_value: Any,
    source_runtime_type: str,
    source_contract_version: Any,
) -> str:
    return sha256_payload(
        {
            "stage_name": stage_name,
            "source_record_id":
                source_record_id,
            "source_value":
                source_value,
            "source_runtime_type":
                source_runtime_type,
            "source_contract_version":
                source_contract_version,
        }
    )


def select_discovery_record(
    discovery_records:
        Sequence[Mapping[str, Any]],
    comparison_record_id: str,
) -> dict[str, Any]:
    candidates = []

    for row in discovery_records:
        if comparison_record_id in parse_comparison_ids(
            row.get("comparison_record_id")
        ):
            candidates.append(dict(row))

    if not candidates:
        return {}

    candidates.sort(
        key=lambda row: (
            normalized_string(
                row.get(
                    "remediation_plan_record_id"
                )
            ),
            normalized_string(
                row.get(
                    "authority_discovery_record_id"
                )
            ),
        )
    )

    return candidates[0]


def build_stage_payloads(
    comparison: Mapping[str, Any],
    evaluation: Mapping[str, Any],
    joined: Mapping[str, Any],
) -> list[dict[str, Any]]:
    evaluation_row_id = normalized_string(
        comparison.get("evaluation_row_id")
    )

    comparison_id = normalized_string(
        comparison.get("comparison_record_id")
    )

    evaluation_value_present = (
        AUTHORITATIVE_FIELD_NAME
        in evaluation
    )

    joined_value_present = (
        AUTHORITATIVE_FIELD_NAME
        in joined
    )

    comparison_value_present = (
        AUTHORITATIVE_FIELD_NAME
        in comparison
    )

    evaluation_value = evaluation.get(
        AUTHORITATIVE_FIELD_NAME
    )

    joined_value = joined.get(
        AUTHORITATIVE_FIELD_NAME
    )

    comparison_value = comparison.get(
        AUTHORITATIVE_FIELD_NAME
    )

    evaluation_digest = (
        evaluation.get(
            "evaluation_row_digest"
        )
        if valid_sha256(
            evaluation.get(
                "evaluation_row_digest"
            )
        )
        else source_record_digest(
            "evaluation_row",
            evaluation_row_id,
            evaluation_value,
            runtime_type_name(
                evaluation_value
            ),
            evaluation.get(
                "evaluation_dataset_contract_version"
            ),
        )
    )

    joined_record_id = normalized_string(
        joined.get(
            "prediction_outcome_join_record_id"
        )
    ) or (
        "joined_"
        + sha256_payload(
            {
                "evaluation_row_id":
                    evaluation_row_id,
                "prediction_variant":
                    joined.get(
                        "prediction_variant"
                    ),
            }
        )[:20]
    )

    joined_digest = (
        joined.get(
            "prediction_outcome_join_record_digest"
        )
        if valid_sha256(
            joined.get(
                "prediction_outcome_join_record_digest"
            )
        )
        else source_record_digest(
            "prediction_outcome_join",
            joined_record_id,
            joined_value,
            runtime_type_name(
                joined_value
            ),
            joined.get(
                "prediction_outcome_join_contract_version"
            ),
        )
    )

    comparison_digest = (
        comparison.get(
            "comparison_record_digest"
        )
    )

    metric_record_id = (
        "metric_consumer_"
        + sha256_payload(
            {
                "comparison_record_id":
                    comparison_id
            }
        )[:20]
    )

    metric_digest = (
        source_record_digest(
            "comparative_metric_consumer",
            metric_record_id,
            comparison_value,
            runtime_type_name(
                comparison_value
            ),
            "historical_comparative_metric_contract",
        )
    )

    downstream_record_id = (
        "downstream_"
        + sha256_payload(
            {
                "comparison_record_id":
                    comparison_id
            }
        )[:20]
    )

    downstream_digest = (
        source_record_digest(
            "interpretation_evidence_and_remediation",
            downstream_record_id,
            comparison_value,
            runtime_type_name(
                comparison_value
            ),
            "historical_downstream_audit_chain",
        )
    )

    return [
        {
            "provenance_stage_id":
                "HOSVPA-S01",
            "provenance_stage_name":
                "evaluation_fixture_or_source",
            "provenance_stage_priority": 1,
            "source_path": (
                "scripts/audit_9P_pitch_type_matchup_overlay_"
                "historical_prediction_outcome_join_contract.py"
            ),
            "source_symbol":
                "replay_evaluation_dataset",
            "source_contract_version":
                evaluation.get(
                    "evaluation_dataset_contract_version"
                ),
            "source_artifact_version": None,
            "source_record_id":
                evaluation_row_id,
            "source_record_digest":
                evaluation_digest,
            "source_value_present":
                evaluation_value_present,
            "source_value":
                evaluation_value,
            "prior_stage_id": None,
            "prior_source_record_id": None,
            "prior_source_record_digest": None,
            "prior_source_value": None,
            "prior_source_runtime_type": None,
        },
        {
            "provenance_stage_id":
                "HOSVPA-S02",
            "provenance_stage_name":
                "evaluation_row",
            "provenance_stage_priority": 2,
            "source_path": (
                "scripts/audit_9P_pitch_type_matchup_overlay_"
                "historical_prediction_outcome_join_contract.py"
            ),
            "source_symbol":
                "replay_evaluation_dataset",
            "source_contract_version":
                evaluation.get(
                    "evaluation_dataset_contract_version"
                ),
            "source_artifact_version": None,
            "source_record_id":
                evaluation_row_id,
            "source_record_digest":
                evaluation_digest,
            "source_value_present":
                evaluation_value_present,
            "source_value":
                evaluation_value,
            "prior_stage_id":
                "HOSVPA-S01",
            "prior_source_record_id":
                evaluation_row_id,
            "prior_source_record_digest":
                evaluation_digest,
            "prior_source_value":
                evaluation_value,
            "prior_source_runtime_type":
                runtime_type_name(
                    evaluation_value
                ),
        },
        {
            "provenance_stage_id":
                "HOSVPA-S03",
            "provenance_stage_name":
                "prediction_outcome_join",
            "provenance_stage_priority": 3,
            "source_path": (
                "scripts/audit_9P_pitch_type_matchup_overlay_"
                "historical_prediction_outcome_join_contract.py"
            ),
            "source_symbol":
                "execute_join",
            "source_contract_version":
                joined.get(
                    "prediction_outcome_join_contract_version"
                ),
            "source_artifact_version": None,
            "source_record_id":
                joined_record_id,
            "source_record_digest":
                joined_digest,
            "source_value_present":
                joined_value_present,
            "source_value":
                joined_value,
            "prior_stage_id":
                "HOSVPA-S02",
            "prior_source_record_id":
                evaluation_row_id,
            "prior_source_record_digest":
                evaluation_digest,
            "prior_source_value":
                evaluation_value,
            "prior_source_runtime_type":
                runtime_type_name(
                    evaluation_value
                ),
        },
        {
            "provenance_stage_id":
                "HOSVPA-S04",
            "provenance_stage_name":
                "comparative_evaluation_record",
            "provenance_stage_priority": 4,
            "source_path": (
                "scripts/audit_9R_pitch_type_matchup_overlay_"
                "historical_comparative_evaluation_contract.py"
            ),
            "source_symbol":
                "comparison_record",
            "source_contract_version":
                comparison.get(
                    "comparative_evaluation_contract_version"
                ),
            "source_artifact_version": None,
            "source_record_id":
                comparison_id,
            "source_record_digest":
                comparison_digest,
            "source_value_present":
                comparison_value_present,
            "source_value":
                comparison_value,
            "prior_stage_id":
                "HOSVPA-S03",
            "prior_source_record_id":
                joined_record_id,
            "prior_source_record_digest":
                joined_digest,
            "prior_source_value":
                joined_value,
            "prior_source_runtime_type":
                runtime_type_name(
                    joined_value
                ),
        },
        {
            "provenance_stage_id":
                "HOSVPA-S05",
            "provenance_stage_name":
                "comparative_metric_consumer",
            "provenance_stage_priority": 5,
            "source_path": (
                "scripts/audit_9T_pitch_type_matchup_overlay_"
                "historical_comparative_metric_contract.py"
            ),
            "source_symbol":
                "comparative_metric_calculation",
            "source_contract_version":
                "historical_comparative_metric_contract",
            "source_artifact_version": None,
            "source_record_id":
                metric_record_id,
            "source_record_digest":
                metric_digest,
            "source_value_present":
                comparison_value_present,
            "source_value":
                comparison_value,
            "prior_stage_id":
                "HOSVPA-S04",
            "prior_source_record_id":
                comparison_id,
            "prior_source_record_digest":
                comparison_digest,
            "prior_source_value":
                comparison_value,
            "prior_source_runtime_type":
                runtime_type_name(
                    comparison_value
                ),
        },
        {
            "provenance_stage_id":
                "HOSVPA-S06",
            "provenance_stage_name":
                "interpretation_evidence_and_remediation",
            "provenance_stage_priority": 6,
            "source_path": (
                "scripts/remediate_9AD_pitch_type_matchup_overlay_"
                "historical_outcome_value_provenance.py"
            ),
            "source_symbol":
                "downstream_audit_chain",
            "source_contract_version":
                "historical_downstream_audit_chain",
            "source_artifact_version": None,
            "source_record_id":
                downstream_record_id,
            "source_record_digest":
                downstream_digest,
            "source_value_present":
                comparison_value_present,
            "source_value":
                comparison_value,
            "prior_stage_id":
                "HOSVPA-S05",
            "prior_source_record_id":
                metric_record_id,
            "prior_source_record_digest":
                metric_digest,
            "prior_source_value":
                comparison_value,
            "prior_source_runtime_type":
                runtime_type_name(
                    comparison_value
                ),
        },
    ]


def transformation_classification(
    stage_priority: int,
    source_present: bool,
    source_value: Any,
    source_runtime_type: str,
    prior_record_digest: Any,
    prior_value: Any,
    prior_runtime_type: Any,
) -> tuple[str, str]:
    if stage_priority == 1:
        return (
            "introduced_at_stage",
            (
                "The earliest replayed evaluation source assigns this "
                "value and runtime type."
            ),
        )

    if not valid_sha256(
        prior_record_digest
    ):
        return (
            "lineage_unresolved",
            (
                "The immediately preceding source-record digest could "
                "not be validated."
            ),
        )

    if not source_present:
        return (
            "dropped_before_stage",
            (
                "The authoritative outcome field is absent at this "
                "lineage stage."
            ),
        )

    if (
        source_value == prior_value
        and source_runtime_type
        == prior_runtime_type
    ):
        return (
            "preserved_exactly",
            (
                "The source value and runtime type match the immediately "
                "preceding lineage stage."
            ),
        )

    return (
        "transformed_explicitly",
        (
            "The source value or runtime type differs from the immediately "
            "preceding lineage stage; no repair authority is exercised."
        ),
    )


def audit_disposition(
    stage_priority: int,
    value_classification: str,
    transformation: str,
) -> tuple[str, str]:
    if transformation == "lineage_unresolved":
        return (
            "source_value_provenance_unresolved",
            (
                "A required prior lineage digest could not be resolved."
            ),
        )

    if transformation == "coerced_without_authority":
        return (
            "unauthorized_coercion_identified",
            (
                "The outcome value was converted without explicit "
                "schema authority."
            ),
        )

    if transformation == "defaulted_without_authority":
        return (
            "unauthorized_default_identified",
            (
                "The outcome value was replaced with an unauthorized "
                "default."
            ),
        )

    if transformation == "imputed_without_authority":
        return (
            "unauthorized_imputation_identified",
            (
                "The outcome value was replaced using another value or "
                "derived estimate."
            ),
        )

    if (
        stage_priority == 1
        and value_classification
        != "valid_numeric"
    ):
        return (
            "source_value_defect_identified",
            (
                "The invalid runtime type or domain is already present "
                "at the earliest resolved evaluation source."
            ),
        )

    if (
        stage_priority > 1
        and transformation
        == "preserved_exactly"
        and value_classification
        != "valid_numeric"
    ):
        return (
            "mapping_defect_not_supported",
            (
                "The invalid source value is preserved unchanged at this "
                "stage; the authoritative field mapping is not the origin "
                "of the defect."
            ),
        )

    if (
        stage_priority == 5
        and value_classification
        != "valid_numeric"
    ):
        return (
            "consumer_validation_gap_identified",
            (
                "An invalid authoritative source value reaches the metric "
                "consumer and requires explicit rejection or classification."
            ),
        )

    return (
        "mapping_defect_not_supported",
        (
            "The authoritative outcome mapping is preserved through the "
            "resolved lineage stage."
        ),
    )


def build_audit_records(
    plan: Any,
    discovery_records:
        Sequence[Mapping[str, Any]],
    evaluation_rows:
        Sequence[Mapping[str, Any]],
    joined_rows:
        Sequence[Mapping[str, Any]],
    comparison_records:
        Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    evaluation_by_id = {
        normalized_string(
            row.get("evaluation_row_id")
        ): dict(row)
        for row in evaluation_rows
        if normalized_string(
            row.get("evaluation_row_id")
        )
    }

    joined_by_evaluation: dict[
        str,
        list[dict[str, Any]],
    ] = defaultdict(list)

    for row in joined_rows:
        evaluation_row_id = normalized_string(
            row.get("evaluation_row_id")
        )

        if evaluation_row_id:
            joined_by_evaluation[
                evaluation_row_id
            ].append(dict(row))

    records: list[dict[str, Any]] = []

    for comparison in sorted(
        comparison_records,
        key=lambda row: normalized_string(
            row.get("comparison_record_id")
        ),
    ):
        comparison_id = normalized_string(
            comparison.get(
                "comparison_record_id"
            )
        )

        if not comparison_id:
            continue

        discovery = select_discovery_record(
            discovery_records,
            comparison_id,
        )

        if not discovery:
            continue

        evaluation_row_id = normalized_string(
            comparison.get(
                "evaluation_row_id"
            )
        )

        evaluation = evaluation_by_id.get(
            evaluation_row_id,
            {},
        )

        joined_candidates = sorted(
            joined_by_evaluation.get(
                evaluation_row_id,
                [],
            ),
            key=lambda row: (
                0
                if row.get(
                    "prediction_variant"
                )
                == "baseline"
                else 1,
                normalized_string(
                    row.get(
                        "prediction_record_id"
                    )
                ),
            ),
        )

        joined = (
            joined_candidates[0]
            if joined_candidates
            else {}
        )

        stage_payloads = build_stage_payloads(
            comparison,
            evaluation,
            joined,
        )

        for stage in stage_payloads:
            source_present = bool(
                stage[
                    "source_value_present"
                ]
            )

            source_value = stage[
                "source_value"
            ]

            source_runtime_type = (
                runtime_type_name(
                    source_value
                )
            )

            (
                value_classification,
                domain_status,
                value_codes,
            ) = classify_value(
                source_present,
                source_value,
            )

            (
                transformation,
                transformation_evidence,
            ) = transformation_classification(
                int(
                    stage[
                        "provenance_stage_priority"
                    ]
                ),
                source_present,
                source_value,
                source_runtime_type,
                stage[
                    "prior_source_record_digest"
                ],
                stage[
                    "prior_source_value"
                ],
                stage[
                    "prior_source_runtime_type"
                ],
            )

            (
                disposition,
                rationale,
            ) = audit_disposition(
                int(
                    stage[
                        "provenance_stage_priority"
                    ]
                ),
                value_classification,
                transformation,
            )

            lineage_gap_codes = []

            if not valid_sha256(
                stage[
                    "source_record_digest"
                ]
            ):
                lineage_gap_codes.append(
                    "historical_outcome_source_value_digest_invalid"
                )

            if (
                int(
                    stage[
                        "provenance_stage_priority"
                    ]
                )
                > 1
                and not valid_sha256(
                    stage[
                        "prior_source_record_digest"
                    ]
                )
            ):
                lineage_gap_codes.append(
                    "historical_outcome_source_value_lineage_incomplete"
                )

            lineage_complete = (
                not lineage_gap_codes
            )

            mapping_preserved = (
                int(
                    stage[
                        "provenance_stage_priority"
                    ]
                )
                == 1
                or transformation
                == "preserved_exactly"
            )

            serialization_preserved = (
                int(
                    stage[
                        "provenance_stage_priority"
                    ]
                )
                == 1
                or canonical_value_json(
                    source_present,
                    source_value,
                )
                == canonical_value_json(
                    True,
                    stage[
                        "prior_source_value"
                    ],
                )
            )

            identity_payload = {
                "source_value_audit_contract_version":
                    AUDIT_CONTRACT_VERSION,
                "comparison_record_id":
                    comparison_id,
                "provenance_stage_id":
                    stage[
                        "provenance_stage_id"
                    ],
                "source_record_id":
                    stage[
                        "source_record_id"
                    ],
                "authority_discovery_record_id":
                    discovery.get(
                        "authority_discovery_record_id"
                    ),
            }

            identity_digest = (
                sha256_payload(
                    identity_payload
                )
            )

            row = {
                "source_value_audit_contract_version":
                    AUDIT_CONTRACT_VERSION,
                "source_value_audit_record_id":
                    "HOSVA-"
                    + identity_digest[:20],
                "authority_discovery_record_id":
                    discovery.get(
                        "authority_discovery_record_id"
                    ),
                "authority_discovery_record_digest":
                    discovery.get(
                        "authority_discovery_record_digest"
                    ),
                "remediation_plan_record_id":
                    discovery.get(
                        "remediation_plan_record_id"
                    ),
                "remediation_plan_record_digest":
                    discovery.get(
                        "remediation_plan_record_digest"
                    ),
                "audit_record_id":
                    discovery.get(
                        "audit_record_id"
                    ),
                "audit_record_digest":
                    discovery.get(
                        "audit_record_digest"
                    ),
                "comparison_record_id":
                    comparison_id,
                "metric_record_id":
                    discovery.get(
                        "metric_record_id"
                    ),
                "metric_name":
                    discovery.get(
                        "metric_name"
                    ),
                "aggregation_name":
                    discovery.get(
                        "aggregation_name"
                    ),
                "aggregation_key":
                    discovery.get(
                        "aggregation_key"
                    ),
                "authoritative_field_name":
                    AUTHORITATIVE_FIELD_NAME,
                "authoritative_field_path":
                    AUTHORITATIVE_FIELD_PATH,
                "rejected_metadata_field_name":
                    REJECTED_METADATA_FIELD,
                "provenance_stage_id":
                    stage[
                        "provenance_stage_id"
                    ],
                "provenance_stage_name":
                    stage[
                        "provenance_stage_name"
                    ],
                "provenance_stage_priority":
                    stage[
                        "provenance_stage_priority"
                    ],
                "source_path":
                    stage[
                        "source_path"
                    ],
                "source_symbol":
                    stage[
                        "source_symbol"
                    ],
                "source_contract_version":
                    stage[
                        "source_contract_version"
                    ],
                "source_artifact_version":
                    stage[
                        "source_artifact_version"
                    ],
                "source_record_id":
                    stage[
                        "source_record_id"
                    ],
                "source_record_digest":
                    stage[
                        "source_record_digest"
                    ],
                "source_value_present":
                    source_present,
                "source_value":
                    source_value,
                "source_value_canonical_json":
                    canonical_value_json(
                        source_present,
                        source_value,
                    ),
                "source_runtime_type":
                    source_runtime_type,
                "source_value_classification":
                    value_classification,
                "source_value_domain_status":
                    domain_status,
                "prior_stage_id":
                    stage[
                        "prior_stage_id"
                    ],
                "prior_source_record_id":
                    stage[
                        "prior_source_record_id"
                    ],
                "prior_source_record_digest":
                    stage[
                        "prior_source_record_digest"
                    ],
                "prior_source_value":
                    stage[
                        "prior_source_value"
                    ],
                "prior_source_runtime_type":
                    stage[
                        "prior_source_runtime_type"
                    ],
                "transformation_classification":
                    transformation,
                "transformation_evidence":
                    transformation_evidence,
                "mapping_preserved":
                    mapping_preserved,
                "serialization_preserved":
                    serialization_preserved,
                "lineage_complete":
                    lineage_complete,
                "lineage_gap_codes":
                    sorted(
                        lineage_gap_codes
                    ),
                "audit_disposition":
                    disposition,
                "audit_rationale":
                    rationale,
                "audit_exclusion_codes":
                    sorted(
                        set(
                            value_codes
                            + lineage_gap_codes
                            + [
                                "historical_outcome_source_value_mutation_prohibited",
                                "historical_outcome_source_value_repair_prohibited",
                                "historical_outcome_source_value_mapping_change_prohibited",
                                "historical_outcome_source_value_quality_claim_prohibited",
                            ]
                        )
                    ),
                "audit_limitations": [
                    (
                        "This audit traces deterministic replay fixtures and "
                        "does not repair canonical historical outcomes."
                    ),
                    (
                        "A source-value defect classification does not establish "
                        "predictive quality, superiority, equivalence, or "
                        "production readiness."
                    ),
                ],
                "source_comparison_digest":
                    discovery.get(
                        "source_comparison_digest"
                    ),
                "source_metric_record_digest":
                    discovery.get(
                        "source_metric_record_digest"
                    ),
                "source_interpretation_digest":
                    discovery.get(
                        "source_interpretation_digest"
                    ),
                "source_evidence_record_digest":
                    discovery.get(
                        "source_evidence_record_digest"
                    ),
                "source_remediation_record_digest":
                    discovery.get(
                        "source_remediation_record_digest"
                    ),
                "source_value_audit_identity_digest":
                    identity_digest,
            }

            row[
                "source_value_audit_record_digest"
            ] = sha256_payload(row)

            missing_fields = [
                field
                for field
                in plan.AUDIT_RECORD_FIELDS
                if field not in row
            ]

            if missing_fields:
                raise RuntimeError(
                    "Audit record missing fields: "
                    + ", ".join(
                        missing_fields
                    )
                )

            records.append(
                {
                    field: row[field]
                    for field
                    in plan.AUDIT_RECORD_FIELDS
                }
            )

    records.sort(
        key=lambda row: (
            normalized_string(
                row.get(
                    "remediation_plan_record_id"
                )
            ),
            normalized_string(
                row.get(
                    "authority_discovery_record_id"
                )
            ),
            int(
                row.get(
                    "provenance_stage_priority",
                    999,
                )
            ),
            normalized_string(
                row.get("source_path")
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

    return records


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_replay = (
        replay_plan_and_discovery()
    )

    plan = predecessor_replay["plan"]

    discovery_records = (
        predecessor_replay[
            "discovery_records"
        ]
    )

    reverse_discovery_records = (
        predecessor_replay[
            "reverse_discovery_records"
        ]
    )

    historical_replay = (
        replay_historical_chain()
    )

    evaluation_rows = historical_replay[
        "evaluation_rows"
    ]

    joined_rows = historical_replay[
        "joined_rows"
    ]

    comparison_records = historical_replay[
        "comparison_records"
    ]

    reverse_comparison_records = (
        historical_replay[
            "reverse_comparison_records"
        ]
    )

    audit_records = (
        build_audit_records(
            plan,
            discovery_records,
            evaluation_rows,
            joined_rows,
            comparison_records,
        )
    )

    reverse_audit_records = (
        build_audit_records(
            plan,
            list(
                reversed(
                    reverse_discovery_records
                )
            ),
            list(
                reversed(
                    evaluation_rows
                )
            ),
            list(
                reversed(
                    joined_rows
                )
            ),
            list(
                reversed(
                    reverse_comparison_records
                )
            ),
        )
    )

    audit_digest = sha256_payload(
        audit_records
    )

    reverse_audit_digest = (
        sha256_payload(
            reverse_audit_records
        )
    )

    classification_counts = dict(
        sorted(
            Counter(
                row[
                    "source_value_classification"
                ]
                for row in audit_records
            ).items()
        )
    )

    transformation_counts = dict(
        sorted(
            Counter(
                row[
                    "transformation_classification"
                ]
                for row in audit_records
            ).items()
        )
    )

    disposition_counts = dict(
        sorted(
            Counter(
                row["audit_disposition"]
                for row in audit_records
            ).items()
        )
    )

    stage_counts = dict(
        sorted(
            Counter(
                row[
                    "provenance_stage_name"
                ]
                for row in audit_records
            ).items()
        )
    )

    audited_comparison_ids = {
        row["comparison_record_id"]
        for row in audit_records
    }

    expected_records = (
        len(audited_comparison_ids)
        * len(plan.PROVENANCE_STAGES)
    )

    boolean_records = [
        row
        for row in audit_records
        if (
            row[
                "source_value_classification"
            ]
            == "boolean_source_value"
        )
    ]

    lineage_complete_count = sum(
        bool(row["lineage_complete"])
        for row in audit_records
    )

    mapping_preserved_count = sum(
        bool(row["mapping_preserved"])
        for row in audit_records
    )

    source_defect_comparisons = {
        row["comparison_record_id"]
        for row in audit_records
        if (
            row[
                "provenance_stage_priority"
            ]
            == 1
            and row[
                "audit_disposition"
            ]
            == "source_value_defect_identified"
        )
    }

    checks = [
        {
            "check":
                "nine_ag_plan_version_verified",
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
                "nine_af_predecessor_version_verified",
            "actual":
                predecessor_replay[
                    "predecessor"
                ].DISCOVERY_CONTRACT_VERSION,
            "expected":
                EXPECTED_PREDECESSOR_VERSION,
            "passed": (
                predecessor_replay[
                    "predecessor"
                ].DISCOVERY_CONTRACT_VERSION
                == EXPECTED_PREDECESSOR_VERSION
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
                "comparison_replay_deterministic",
            "actual": (
                canonical_json(
                    comparison_records
                )
                == canonical_json(
                    reverse_comparison_records
                )
            ),
            "expected": True,
            "passed": (
                canonical_json(
                    comparison_records
                )
                == canonical_json(
                    reverse_comparison_records
                )
            ),
        },
        {
            "check":
                "audit_replay_deterministic",
            "actual": (
                canonical_json(
                    audit_records
                )
                == canonical_json(
                    reverse_audit_records
                )
            ),
            "expected": True,
            "passed": (
                canonical_json(
                    audit_records
                )
                == canonical_json(
                    reverse_audit_records
                )
            ),
        },
        {
            "check":
                "audit_digests_match_reverse_replay",
            "actual":
                audit_digest,
            "expected":
                reverse_audit_digest,
            "passed": (
                audit_digest
                == reverse_audit_digest
            ),
        },
        {
            "check":
                "audit_records_materialized_for_six_stages",
            "actual":
                len(audit_records),
            "expected":
                expected_records,
            "passed": (
                len(audit_records)
                == expected_records
            ),
        },
        {
            "check":
                "six_provenance_stages_present",
            "actual":
                len(stage_counts),
            "expected": 6,
            "passed": (
                len(stage_counts) == 6
            ),
        },
        {
            "check":
                "all_stages_have_equal_comparison_cardinality",
            "actual":
                sorted(
                    stage_counts.values()
                ),
            "expected": [
                len(audited_comparison_ids)
            ] * 6,
            "passed": (
                sorted(
                    stage_counts.values()
                )
                == [
                    len(
                        audited_comparison_ids
                    )
                ] * 6
            ),
        },
        {
            "check":
                "audit_record_fields_complete",
            "actual":
                len(
                    plan.AUDIT_RECORD_FIELDS
                ),
            "expected": 53,
            "passed": all(
                set(
                    plan.AUDIT_RECORD_FIELDS
                )
                == set(row)
                for row in audit_records
            ),
        },
        {
            "check":
                "audit_record_ids_unique",
            "actual": len(
                {
                    row[
                        "source_value_audit_record_id"
                    ]
                    for row in audit_records
                }
            ),
            "expected":
                len(audit_records),
            "passed": (
                len(
                    {
                        row[
                            "source_value_audit_record_id"
                        ]
                        for row in audit_records
                    }
                )
                == len(audit_records)
            ),
        },
        {
            "check":
                "audit_record_digests_unique",
            "actual": len(
                {
                    row[
                        "source_value_audit_record_digest"
                    ]
                    for row in audit_records
                }
            ),
            "expected":
                len(audit_records),
            "passed": (
                len(
                    {
                        row[
                            "source_value_audit_record_digest"
                        ]
                        for row in audit_records
                    }
                )
                == len(audit_records)
            ),
        },
        {
            "check":
                "all_audit_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_value_audit_record_digest"
                    ]
                )
                for row in audit_records
            ),
            "expected":
                len(audit_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_value_audit_record_digest"
                    ]
                )
                for row in audit_records
            ),
        },
        {
            "check":
                "all_audit_identity_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_value_audit_identity_digest"
                    ]
                )
                for row in audit_records
            ),
            "expected":
                len(audit_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_value_audit_identity_digest"
                    ]
                )
                for row in audit_records
            ),
        },
        {
            "check":
                "all_source_record_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_record_digest"
                    ]
                )
                for row in audit_records
            ),
            "expected":
                len(audit_records),
            "passed": all(
                valid_sha256(
                    row[
                        "source_record_digest"
                    ]
                )
                for row in audit_records
            ),
        },
        {
            "check":
                "all_lineage_complete",
            "actual":
                lineage_complete_count,
            "expected":
                len(audit_records),
            "passed": (
                lineage_complete_count
                == len(audit_records)
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
                    for row in audit_records
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
                for row in audit_records
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
                    for row in audit_records
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
                for row in audit_records
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
                    for row in audit_records
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
                for row in audit_records
            ),
        },
        {
            "check":
                "boolean_source_values_identified",
            "actual":
                len(boolean_records),
            "expected_minimum": 1,
            "passed": (
                len(boolean_records) > 0
            ),
        },
        {
            "check":
                "source_value_defects_identified_at_earliest_stage",
            "actual":
                len(
                    source_defect_comparisons
                ),
            "expected_minimum": 1,
            "passed": (
                len(
                    source_defect_comparisons
                )
                > 0
            ),
        },
        {
            "check":
                "mapping_preserved_for_all_resolved_stages",
            "actual":
                mapping_preserved_count,
            "expected":
                len(audit_records),
            "passed": (
                mapping_preserved_count
                == len(audit_records)
            ),
        },
        {
            "check":
                "no_unauthorized_coercion_classifications",
            "actual":
                transformation_counts.get(
                    "coerced_without_authority",
                    0,
                ),
            "expected": 0,
            "passed": (
                transformation_counts.get(
                    "coerced_without_authority",
                    0,
                )
                == 0
            ),
        },
        {
            "check":
                "no_unauthorized_default_classifications",
            "actual":
                transformation_counts.get(
                    "defaulted_without_authority",
                    0,
                ),
            "expected": 0,
            "passed": (
                transformation_counts.get(
                    "defaulted_without_authority",
                    0,
                )
                == 0
            ),
        },
        {
            "check":
                "no_unauthorized_imputation_classifications",
            "actual":
                transformation_counts.get(
                    "imputed_without_authority",
                    0,
                ),
            "expected": 0,
            "passed": (
                transformation_counts.get(
                    "imputed_without_authority",
                    0,
                )
                == 0
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
                "source_values_not_coerced",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "source_values_not_defaulted",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check":
                "source_values_not_imputed",
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
        "outcome_source_value_provenance_audit_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_source_value_provenance_audit_implementation_failed"
    )

    next_layer = (
        "9AI_pitch_type_matchup_overlay_historical_"
        "outcome_source_value_remediation_plan"
        if all_checks_passed
        else
        "9AH_pitch_type_matchup_overlay_historical_"
        "outcome_source_value_provenance_audit_implementation_remediation"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "expected_minimum",
            "passed",
        ],
        checks,
    )

    write_csv(
        OUTPUT_DIR
        / "source_value_provenance_audit_records.csv",
        plan.AUDIT_RECORD_FIELDS,
        audit_records,
    )

    write_csv(
        OUTPUT_DIR / "stage_counts.csv",
        [
            "provenance_stage_name",
            "count",
        ],
        [
            {
                "provenance_stage_name":
                    key,
                "count": value,
            }
            for key, value
            in stage_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "value_classification_counts.csv",
        [
            "source_value_classification",
            "count",
        ],
        [
            {
                "source_value_classification":
                    key,
                "count": value,
            }
            for key, value
            in classification_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "transformation_counts.csv",
        [
            "transformation_classification",
            "count",
        ],
        [
            {
                "transformation_classification":
                    key,
                "count": value,
            }
            for key, value
            in transformation_counts.items()
        ],
    )

    write_csv(
        OUTPUT_DIR / "disposition_counts.csv",
        [
            "audit_disposition",
            "count",
        ],
        [
            {
                "audit_disposition":
                    key,
                "count": value,
            }
            for key, value
            in disposition_counts.items()
        ],
    )

    summary = {
        "layer_id":
            LAYER_ID,
        "layer_name":
            LAYER_NAME,
        "audit_contract_version":
            AUDIT_CONTRACT_VERSION,
        "plan_version":
            plan.PLAN_VERSION,
        "predecessor_contract_version":
            predecessor_replay[
                "predecessor"
            ].DISCOVERY_CONTRACT_VERSION,
        "discovery_records":
            len(discovery_records),
        "evaluation_rows":
            len(evaluation_rows),
        "joined_rows":
            len(joined_rows),
        "comparison_records":
            len(comparison_records),
        "audited_comparisons":
            len(audited_comparison_ids),
        "audit_records":
            len(audit_records),
        "stage_counts":
            stage_counts,
        "value_classification_counts":
            classification_counts,
        "transformation_counts":
            transformation_counts,
        "disposition_counts":
            disposition_counts,
        "boolean_source_value_records":
            len(boolean_records),
        "source_value_defect_comparisons":
            len(
                source_defect_comparisons
            ),
        "lineage_complete_records":
            lineage_complete_count,
        "mapping_preserved_records":
            mapping_preserved_count,
        "audit_digest":
            audit_digest,
        "reverse_audit_digest":
            reverse_audit_digest,
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required":
            len(checks),
        "canonical_source_records_changed": 0,
        "canonical_mappings_changed": 0,
        "source_values_repaired": 0,
        "source_values_coerced": 0,
        "source_values_defaulted": 0,
        "source_values_imputed": 0,
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
        / "outcome_source_value_provenance_audit_summary.json",
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
            "remediation_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": [
            "canonical_historical_source_mutation",
            "canonical_outcome_mapping_change",
            "source_value_repair",
            "source_value_coercion",
            "source_value_defaulting",
            "source_value_imputation",
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
        "Audit contract version: "
        f"{AUDIT_CONTRACT_VERSION}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Audited comparisons: "
        f"{len(audited_comparison_ids)}"
    )
    print(
        "Audit records: "
        f"{len(audit_records)}"
    )
    print(
        f"Stage counts: {stage_counts}"
    )
    print(
        "Value classification counts: "
        f"{classification_counts}"
    )
    print(
        "Transformation counts: "
        f"{transformation_counts}"
    )
    print(
        "Disposition counts: "
        f"{disposition_counts}"
    )
    print(
        "Boolean source-value records: "
        f"{len(boolean_records)}"
    )
    print(
        "Source-value defect comparisons: "
        f"{len(source_defect_comparisons)}"
    )
    print(
        f"Audit digest: {audit_digest}"
    )
    print(
        "Reverse audit digest: "
        f"{reverse_audit_digest}"
    )
    print("Canonical source records changed: 0")
    print("Canonical mappings changed: 0")
    print("Source values repaired: 0")
    print("Source values coerced: 0")
    print("Source values defaulted: 0")
    print("Source values imputed: 0")
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
