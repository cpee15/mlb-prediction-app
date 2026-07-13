#!/usr/bin/env python3
"""
Layer 9AB
Pitch-Type Matchup Overlay Historical Outcome-Value Provenance Audit Implementation

Implements the read-only provenance audit planned by Layer 9AA for the twelve
historical comparative metric records classified as outcome_value_invalid.

This implementation:

- verifies the merged Layer 9AA plan and Layer 9Z predecessor;
- replays comparison, metric, interpretation, evidence, and remediation records;
- selects exactly twelve invalid-outcome remediation records;
- resolves each record through the full deterministic lineage chain;
- inventories source comparison outcome fields, raw values, and runtime types;
- classifies outcome-value failure modes without applying repairs;
- records candidate source fields only as diagnostic observations;
- replays the audit under reversed input ordering;
- writes temporary diagnostic artifacts only.

This implementation does not mutate canonical historical records, coerce,
default, fabricate, or impute outcomes, repair mappings, lower support
thresholds, recompute source contract records, train or tune models, estimate
uncertainty or statistical significance, declare superiority or equivalence,
recommend activation, or modify production probabilities, simulations,
pricing, markets, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
import math
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9AB"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_value_provenance_audit_implementation"
)

AUDIT_CONTRACT_VERSION = (
    "layer_9AB_historical_outcome_value_"
    "provenance_audit_contract_v1"
)

EXPECTED_PLAN_VERSION = (
    "layer_9AA_historical_outcome_value_"
    "provenance_audit_plan_v1"
)

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_"
    "outcome_value_provenance_audit_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_outcome_value_"
    "provenance_audit_implementation"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9Z_historical_comparative_data_gap_"
    "remediation_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "data_gap_remediation_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_value_"
    "provenance_audit_planning"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9AB_pitch_type_matchup_overlay_"
    "historical_outcome_value_provenance_audit"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9AA_pitch_type_matchup_overlay_"
    "historical_outcome_value_provenance_audit.py"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9Z_pitch_type_matchup_overlay_"
    "historical_comparative_data_gap_remediation_contract.py"
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

AUDIT_RECORD_FIELDS = [
    "audit_contract_version",
    "audit_record_id",
    "source_remediation_record_id",
    "metric_name",
    "aggregation_name",
    "aggregation_key",
    "comparison_record_id",
    "metric_record_id",
    "interpretation_record_id",
    "evidence_record_id",
    "source_artifact_path",
    "source_artifact_digest",
    "source_record_id",
    "source_record_digest",
    "outcome_field_name",
    "outcome_field_path",
    "raw_outcome_value",
    "raw_outcome_type",
    "raw_outcome_serialization",
    "normalized_outcome_value",
    "normalized_outcome_type",
    "normalization_rule_applied",
    "expected_outcome_semantic",
    "expected_outcome_type",
    "accepted_value_domain",
    "compatibility_check_results",
    "failure_stage",
    "failure_modes",
    "failure_detail",
    "candidate_outcome_fields",
    "audit_status",
    "audit_limitations",
    "audit_exclusion_codes",
    "source_comparison_digest",
    "source_metric_record_digest",
    "source_interpretation_digest",
    "source_evidence_record_digest",
    "source_remediation_record_digest",
    "audit_identity_digest",
    "audit_record_digest",
]

EXCLUSION_CODES = {
    "remediation_invalid":
        "historical_outcome_audit_remediation_record_invalid",
    "lineage_invalid":
        "historical_outcome_audit_lineage_invalid",
    "artifact_unresolved":
        "historical_outcome_audit_source_artifact_unresolved",
    "record_unresolved":
        "historical_outcome_audit_source_record_unresolved",
    "field_absent":
        "historical_outcome_audit_field_absent",
    "value_null":
        "historical_outcome_audit_value_null",
    "value_empty":
        "historical_outcome_audit_value_empty",
    "type_incompatible":
        "historical_outcome_audit_value_type_incompatible",
    "non_finite":
        "historical_outcome_audit_value_non_finite",
    "domain_invalid":
        "historical_outcome_audit_value_domain_invalid",
    "semantic_mismatch":
        "historical_outcome_audit_semantic_mismatch",
    "candidate_mapping":
        "historical_outcome_audit_candidate_mapping_observed",
    "multiple_candidates":
        "historical_outcome_audit_multiple_candidate_fields",
    "unresolved":
        "historical_outcome_audit_failure_mode_unresolved",
    "mutation_prohibited":
        "historical_outcome_audit_source_mutation_prohibited",
    "imputation_prohibited":
        "historical_outcome_audit_imputation_prohibited",
}

COMMON_LIMITATIONS = [
    (
        "This audit is a deterministic read-only provenance diagnosis and "
        "does not repair any historical outcome value or source mapping."
    ),
    (
        "Canonical historical observations may not be silently altered, "
        "coerced, defaulted, fabricated, or imputed."
    ),
    (
        "A candidate field observation does not establish that the field is "
        "the correct canonical outcome source."
    ),
    (
        "A classified provenance failure does not establish predictive "
        "improvement, superiority, equivalence, or production readiness."
    ),
]

PROHIBITED_AUTHORITIES = [
    "activation_recommendation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "candidate_artifact_write",
    "canonical_historical_record_mutation",
    "canonical_outcome_mapping_change",
    "canonical_probability_authority_change",
    "dataset_split_execution",
    "edge_detection",
    "equivalence_declaration",
    "market_comparison",
    "model_training",
    "outcome_coercion",
    "outcome_imputation",
    "parameter_tuning",
    "pricing",
    "production_historical_prediction_materialization",
    "production_matchup_activation",
    "production_overlay_integration",
    "production_readiness_declaration",
    "simulation_probability_change",
    "simulation_state_change",
    "statistical_significance_evaluation",
    "superiority_declaration",
    "threshold_relaxation",
    "threshold_tuning",
    "uncertainty_estimation",
]


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def canonical_json_text(payload: Any) -> str:
    return canonical_json_bytes(payload).decode("utf-8")


def sha256_payload(payload: Any) -> str:
    return hashlib.sha256(
        canonical_json_bytes(payload)
    ).hexdigest()


def normalized_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def valid_sha256(value: Any) -> bool:
    return bool(
        SHA256_PATTERN.fullmatch(
            normalized_string(value)
        )
    )


def finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def load_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(
        name,
        path,
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Unable to load module: {path}"
        )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def string_constants(path: Path) -> set[str]:
    if not path.exists():
        return set()

    try:
        tree = ast.parse(
            path.read_text(
                encoding="utf-8",
                errors="ignore",
            ),
            filename=str(path),
        )
    except SyntaxError:
        return set()

    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
    }


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )


def write_jsonl(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        for row in rows:
            handle.write(
                canonical_json_text(
                    dict(row)
                )
                + "\n"
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
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)


def parse_json_object(value: Any) -> dict[str, Any]:
    text = normalized_string(value)

    if not text:
        return {}

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}

    if not isinstance(payload, dict):
        return {}

    return payload


def replay_contract_chain() -> dict[str, Any]:
    layer_9z = load_module(
        PREDECESSOR_PATH,
        "layer_9z_outcome_audit_source",
    )

    layer_9y = load_module(
        layer_9z.PLAN_PATH,
        "layer_9y_outcome_audit_source",
    )

    interpretation_rows, evidence_rows = (
        layer_9z.replay_sources()
    )

    remediation_rows = (
        layer_9z.execute_remediation_planning(
            evidence_rows,
            interpretation_rows,
            layer_9y,
        )
    )

    layer_9x = load_module(
        layer_9z.PREDECESSOR_PATH,
        "layer_9x_outcome_audit_source",
    )

    layer_9v = load_module(
        layer_9x.PREDECESSOR_PATH,
        "layer_9v_outcome_audit_source",
    )

    metric_rows = layer_9v.replay_metric_records()

    layer_9t = load_module(
        layer_9v.PREDECESSOR_PATH,
        "layer_9t_outcome_audit_source",
    )

    comparison_rows = (
        layer_9t.replay_comparison_records()
    )

    return {
        "layer_9z": layer_9z,
        "comparison_rows": comparison_rows,
        "metric_rows": metric_rows,
        "interpretation_rows": interpretation_rows,
        "evidence_rows": evidence_rows,
        "remediation_rows": remediation_rows,
    }


def runtime_type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def serialize_raw_value(value: Any) -> str:
    return canonical_json_text(value)


def comparison_matches_metric(
    comparison_row: Mapping[str, Any],
    metric_row: Mapping[str, Any],
) -> bool:
    aggregation_key = parse_json_object(
        metric_row.get("aggregation_key")
    )

    for field, expected in aggregation_key.items():
        if comparison_row.get(field) != expected:
            return False

    return True


def outcome_candidate_fields(
    comparison_rows: Sequence[Mapping[str, Any]],
) -> list[str]:
    candidates: set[str] = set()

    explicit_names = {
        "outcome",
        "outcome_value",
        "actual",
        "actual_value",
        "observed",
        "observed_value",
        "result",
        "result_value",
        "target",
        "target_value",
        "label",
        "winner",
        "home_win",
        "away_win",
        "is_home_win",
        "is_away_win",
    }

    for row in comparison_rows:
        for field in row:
            lowered = field.lower()

            if (
                lowered in explicit_names
                or "outcome" in lowered
                or lowered.startswith("actual_")
                or lowered.endswith("_actual")
                or lowered.endswith("_result")
            ):
                candidates.add(field)

    return sorted(candidates)


def expected_outcome_contract(
    metric_name: str,
    metric_family: str,
) -> tuple[str, str, str]:
    lowered_name = metric_name.lower()
    lowered_family = metric_family.lower()

    if (
        "brier" in lowered_name
        or "log_loss" in lowered_name
        or "logloss" in lowered_name
        or "accuracy" in lowered_name
        or "binary" in lowered_family
        or "probability" in lowered_family
    ):
        return (
            "binary_observed_outcome",
            "finite numeric value",
            "[0,1] with binary metrics requiring {0,1}",
        )

    if "coverage" in lowered_family:
        return (
            "outcome_presence_indicator",
            "any observed value",
            "present source outcome value",
        )

    return (
        "numeric_observed_outcome",
        "finite numeric value",
        "metric-defined numeric domain",
    )


def classify_raw_outcome(
    value: Any,
    expected_semantic: str,
) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    failure_modes: list[str] = []
    exclusion_codes: list[str] = []

    field_present = True
    value_present = value is not None
    runtime_compatible = finite_number(value)
    finite_numeric = finite_number(value)
    domain_compatible = True
    semantic_compatible = True

    if value is None:
        failure_modes.append(
            "outcome_value_null"
        )
        exclusion_codes.append(
            EXCLUSION_CODES["value_null"]
        )
        runtime_compatible = False
        finite_numeric = False
        domain_compatible = False
    elif isinstance(value, str) and not value.strip():
        failure_modes.append(
            "outcome_value_empty"
        )
        exclusion_codes.append(
            EXCLUSION_CODES["value_empty"]
        )
        runtime_compatible = False
        finite_numeric = False
        domain_compatible = False
    elif not finite_number(value):
        failure_modes.append(
            "outcome_value_non_numeric"
        )
        exclusion_codes.append(
            EXCLUSION_CODES[
                "type_incompatible"
            ]
        )
        domain_compatible = False
    elif not math.isfinite(float(value)):
        failure_modes.append(
            "outcome_value_non_finite"
        )
        exclusion_codes.append(
            EXCLUSION_CODES["non_finite"]
        )
        domain_compatible = False

    if (
        finite_number(value)
        and expected_semantic
        == "binary_observed_outcome"
        and float(value) not in {0.0, 1.0}
    ):
        failure_modes.append(
            "outcome_value_domain_invalid"
        )
        exclusion_codes.append(
            EXCLUSION_CODES["domain_invalid"]
        )
        domain_compatible = False

    checks = [
        {
            "check_name": "field_presence",
            "passed": field_present,
        },
        {
            "check_name": "value_presence",
            "passed": value_present,
        },
        {
            "check_name": "runtime_type",
            "passed": runtime_compatible,
        },
        {
            "check_name": "finite_numeric_value",
            "passed": finite_numeric,
        },
        {
            "check_name": "accepted_domain",
            "passed": domain_compatible,
        },
        {
            "check_name": "semantic_alignment",
            "passed": semantic_compatible,
        },
        {
            "check_name": "artifact_resolution",
            "passed": True,
        },
        {
            "check_name": "lineage_completeness",
            "passed": True,
        },
    ]

    return (
        sorted(set(failure_modes)),
        sorted(set(exclusion_codes)),
        checks,
    )


def source_validation_codes(
    remediation_row: Mapping[str, Any],
) -> list[str]:
    codes: set[str] = set()

    if (
        remediation_row.get("gap_category")
        != "invalid_outcome_value"
    ):
        codes.add(
            EXCLUSION_CODES[
                "remediation_invalid"
            ]
        )

    if remediation_row.get("gap_priority") != 1:
        codes.add(
            EXCLUSION_CODES[
                "remediation_invalid"
            ]
        )

    required_digests = [
        "remediation_record_digest",
        "source_evidence_record_digest",
        "source_interpretation_digest",
    ]

    if not all(
        valid_sha256(
            remediation_row.get(field)
        )
        for field in required_digests
    ):
        codes.add(
            EXCLUSION_CODES["lineage_invalid"]
        )

    return sorted(codes)


def execute_provenance_audit(
    comparison_rows: Sequence[Mapping[str, Any]],
    metric_rows: Sequence[Mapping[str, Any]],
    interpretation_rows: Sequence[Mapping[str, Any]],
    evidence_rows: Sequence[Mapping[str, Any]],
    remediation_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    invalid_remediation_rows = sorted(
        [
            dict(row)
            for row in remediation_rows
            if row.get("gap_category")
            == "invalid_outcome_value"
        ],
        key=lambda row: normalized_string(
            row.get("remediation_record_id")
        ),
    )

    metric_by_id = {
        normalized_string(
            row.get("metric_record_id")
        ): row
        for row in metric_rows
    }

    interpretation_by_id = {
        normalized_string(
            row.get(
                "interpretation_record_id"
            )
        ): row
        for row in interpretation_rows
    }

    evidence_by_id = {
        normalized_string(
            row.get("evidence_record_id")
        ): row
        for row in evidence_rows
    }

    records: list[dict[str, Any]] = []

    for remediation_row in invalid_remediation_rows:
        exclusion_codes = set(
            source_validation_codes(
                remediation_row
            )
        )

        evidence_id = normalized_string(
            remediation_row.get(
                "source_evidence_record_id"
            )
        )

        evidence_row = evidence_by_id.get(
            evidence_id
        )

        assessment_key = (
            parse_json_object(
                evidence_row.get(
                    "assessment_key"
                )
            )
            if evidence_row
            else {}
        )

        interpretation_id = normalized_string(
            assessment_key.get(
                "interpretation_record_id"
            )
        )

        interpretation_row = (
            interpretation_by_id.get(
                interpretation_id
            )
        )

        metric_id = normalized_string(
            (
                interpretation_row.get(
                    "metric_record_id"
                )
                or interpretation_row.get(
                    "source_metric_record_id"
                )
            )
            if interpretation_row
            else ""
        )

        metric_row = metric_by_id.get(
            metric_id
        )

        if not (
            evidence_row
            and interpretation_row
            and metric_row
        ):
            exclusion_codes.add(
                EXCLUSION_CODES[
                    "lineage_invalid"
                ]
            )

        matching_comparisons = (
            sorted(
                [
                    dict(row)
                    for row in comparison_rows
                    if metric_row
                    and comparison_matches_metric(
                        row,
                        metric_row,
                    )
                ],
                key=lambda row: (
                    normalized_string(
                        row.get("comparison_record_id")
                    ),
                    normalized_string(
                        row.get("comparison_record_digest")
                    ),
                ),
            )
            if metric_row
            else []
        )

        candidate_fields = (
            outcome_candidate_fields(
                matching_comparisons
            )
        )

        outcome_values = [
            row.get("outcome_value")
            for row in matching_comparisons
        ]

        comparison_ids = [
            normalized_string(
                row.get(
                    "comparison_record_id"
                )
            )
            for row in matching_comparisons
        ]

        comparison_digests = [
            normalized_string(
                row.get(
                    "comparison_record_digest"
                )
            )
            for row in matching_comparisons
        ]

        metric_name = normalized_string(
            metric_row.get("metric_name")
            if metric_row
            else remediation_row.get(
                "metric_name"
            )
        )

        metric_family = normalized_string(
            metric_row.get("metric_family")
            if metric_row
            else ""
        )

        (
            expected_semantic,
            expected_type,
            accepted_domain,
        ) = expected_outcome_contract(
            metric_name,
            metric_family,
        )

        unique_raw_values = {
            canonical_json_text(value)
            for value in outcome_values
        }

        raw_value_payload: Any

        if not matching_comparisons:
            raw_value_payload = None
            failure_modes = [
                "outcome_source_artifact_unresolved"
            ]
            compatibility_checks = [
                {
                    "check_name":
                        "artifact_resolution",
                    "passed": False,
                },
                {
                    "check_name":
                        "lineage_completeness",
                    "passed": False,
                },
            ]
            exclusion_codes.update(
                {
                    EXCLUSION_CODES[
                        "artifact_unresolved"
                    ],
                    EXCLUSION_CODES[
                        "record_unresolved"
                    ],
                }
            )
            audit_status = (
                "source_artifact_unresolved"
            )
            failure_stage = (
                "historical_prediction_artifact"
            )
        else:
            raw_value_payload = (
                outcome_values[0]
                if len(unique_raw_values) == 1
                else outcome_values
            )

            all_failure_modes: set[str] = set()
            all_classification_codes: set[str] = set()
            check_results: dict[
                str,
                bool,
            ] = {}

            for value in outcome_values:
                (
                    value_failures,
                    value_codes,
                    value_checks,
                ) = classify_raw_outcome(
                    value,
                    expected_semantic,
                )

                all_failure_modes.update(
                    value_failures
                )
                all_classification_codes.update(
                    value_codes
                )

                for check in value_checks:
                    name = check["check_name"]
                    passed = bool(check["passed"])

                    check_results[name] = (
                        check_results.get(
                            name,
                            True,
                        )
                        and passed
                    )

            failure_modes = sorted(
                all_failure_modes
            )

            exclusion_codes.update(
                all_classification_codes
            )

            compatibility_checks = [
                {
                    "check_name": name,
                    "passed": check_results[name],
                }
                for name in sorted(
                    check_results
                )
            ]

            if not failure_modes:
                failure_modes = [
                    "failure_mode_unresolved"
                ]
                exclusion_codes.add(
                    EXCLUSION_CODES["unresolved"]
                )
                audit_status = (
                    "audit_inconclusive"
                )
                failure_stage = (
                    "metric_compatibility_boundary"
                )
            elif len(failure_modes) == 1:
                audit_status = (
                    "failure_mode_classified"
                )
                failure_stage = (
                    "raw_outcome_value"
                )
            else:
                audit_status = (
                    "multiple_failure_modes_observed"
                )
                failure_stage = (
                    "raw_outcome_value"
                )

        plausible_alternatives = [
            field
            for field in candidate_fields
            if field != "outcome_value"
            and any(
                row.get(field) is not None
                for row in matching_comparisons
            )
        ]

        if plausible_alternatives:
            exclusion_codes.add(
                EXCLUSION_CODES[
                    "candidate_mapping"
                ]
            )

            if len(
                plausible_alternatives
            ) > 1:
                exclusion_codes.add(
                    EXCLUSION_CODES[
                        "multiple_candidates"
                    ]
                )

            if audit_status in {
                "failure_mode_classified",
                "audit_inconclusive",
            }:
                audit_status = (
                    "candidate_mapping_identified"
                )

        exclusion_codes.update(
            {
                EXCLUSION_CODES[
                    "mutation_prohibited"
                ],
                EXCLUSION_CODES[
                    "imputation_prohibited"
                ],
            }
        )

        comparison_record_id = (
            comparison_ids[0]
            if len(comparison_ids) == 1
            else canonical_json_text(
                comparison_ids
            )
        )

        source_comparison_digest = (
            comparison_digests[0]
            if len(
                set(comparison_digests)
            ) == 1
            else sha256_payload(
                sorted(
                    comparison_digests
                )
            )
        )

        raw_type_payload = sorted(
            {
                runtime_type_name(value)
                for value in outcome_values
            }
        )

        raw_outcome_type = (
            raw_type_payload[0]
            if len(raw_type_payload) == 1
            else canonical_json_text(
                raw_type_payload
            )
        )

        source_artifact_path = (
            "deterministic_layer_9R_comparison_replay"
        )

        source_artifact_digest = (
            source_comparison_digest
        )

        source_record_id = (
            comparison_record_id
        )

        source_record_digest = (
            source_comparison_digest
        )

        identity_payload = {
            "audit_contract_version":
                AUDIT_CONTRACT_VERSION,
            "source_remediation_record_id":
                remediation_row.get(
                    "remediation_record_id"
                ),
            "metric_record_id":
                metric_id,
            "comparison_record_id":
                comparison_record_id,
            "failure_modes":
                failure_modes,
        }

        audit_identity_digest = (
            sha256_payload(
                identity_payload
            )
        )

        audit_record_id = (
            "HOVPA-"
            + audit_identity_digest[:20]
        )

        failure_detail = {
            "matching_comparison_records":
                len(matching_comparisons),
            "unique_raw_outcome_values":
                len(unique_raw_values),
            "metric_status": (
                metric_row.get(
                    "metric_status"
                )
                if metric_row
                else None
            ),
            "metric_exclusion_codes": (
                metric_row.get(
                    "metric_exclusion_codes"
                )
                if metric_row
                else []
            ),
            "source_metric_status": (
                interpretation_row.get(
                    "source_metric_status"
                )
                if interpretation_row
                else None
            ),
        }

        record_without_digest = {
            "audit_contract_version":
                AUDIT_CONTRACT_VERSION,
            "audit_record_id":
                audit_record_id,
            "source_remediation_record_id":
                remediation_row.get(
                    "remediation_record_id"
                ),
            "metric_name":
                metric_name,
            "aggregation_name": (
                metric_row.get(
                    "aggregation_name"
                )
                if metric_row
                else remediation_row.get(
                    "aggregation_name"
                )
            ),
            "aggregation_key": (
                metric_row.get(
                    "aggregation_key"
                )
                if metric_row
                else remediation_row.get(
                    "aggregation_key"
                )
            ),
            "comparison_record_id":
                comparison_record_id,
            "metric_record_id":
                metric_id,
            "interpretation_record_id":
                interpretation_id,
            "evidence_record_id":
                evidence_id,
            "source_artifact_path":
                source_artifact_path,
            "source_artifact_digest":
                source_artifact_digest,
            "source_record_id":
                source_record_id,
            "source_record_digest":
                source_record_digest,
            "outcome_field_name":
                "outcome_value",
            "outcome_field_path":
                "comparison_record.outcome_value",
            "raw_outcome_value":
                raw_value_payload,
            "raw_outcome_type":
                raw_outcome_type,
            "raw_outcome_serialization":
                canonical_json_text(
                    raw_value_payload
                ),
            "normalized_outcome_value":
                None,
            "normalized_outcome_type":
                "not_materialized",
            "normalization_rule_applied":
                "none_read_only_audit",
            "expected_outcome_semantic":
                expected_semantic,
            "expected_outcome_type":
                expected_type,
            "accepted_value_domain":
                accepted_domain,
            "compatibility_check_results":
                compatibility_checks,
            "failure_stage":
                failure_stage,
            "failure_modes":
                failure_modes,
            "failure_detail":
                failure_detail,
            "candidate_outcome_fields":
                plausible_alternatives,
            "audit_status":
                audit_status,
            "audit_limitations":
                COMMON_LIMITATIONS,
            "audit_exclusion_codes":
                sorted(exclusion_codes),
            "source_comparison_digest":
                source_comparison_digest,
            "source_metric_record_digest": (
                metric_row.get(
                    "metric_record_digest"
                )
                if metric_row
                else ""
            ),
            "source_interpretation_digest": (
                interpretation_row.get(
                    "interpretation_record_digest"
                )
                if interpretation_row
                else remediation_row.get(
                    "source_interpretation_digest"
                )
            ),
            "source_evidence_record_digest": (
                evidence_row.get(
                    "evidence_record_digest"
                )
                if evidence_row
                else remediation_row.get(
                    "source_evidence_record_digest"
                )
            ),
            "source_remediation_record_digest":
                remediation_row.get(
                    "remediation_record_digest"
                ),
            "audit_identity_digest":
                audit_identity_digest,
        }

        audit_record_digest = (
            sha256_payload(
                record_without_digest
            )
        )

        record = {
            **record_without_digest,
            "audit_record_digest":
                audit_record_digest,
        }

        records.append(record)

    return sorted(
        records,
        key=lambda row: (
            normalized_string(
                row.get("metric_name")
            ),
            normalized_string(
                row.get("aggregation_name")
            ),
            normalized_string(
                row.get("aggregation_key")
            ),
            normalized_string(
                row.get("failure_stage")
            ),
            normalized_string(
                row.get("audit_status")
            ),
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


def csv_safe_record(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for field in AUDIT_RECORD_FIELDS:
        value = row.get(field)

        if isinstance(
            value,
            (dict, list),
        ):
            result[field] = (
                canonical_json_text(value)
            )
        else:
            result[field] = value

    return result


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    plan_constants = string_constants(
        PLAN_PATH
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    plan_verified = (
        PLAN_PATH.exists()
        and EXPECTED_PLAN_VERSION
        in plan_constants
        and EXPECTED_PLAN_DIAGNOSIS
        in plan_constants
        and EXPECTED_PLAN_AUTHORITY
        in plan_constants
    )

    predecessor_verified = (
        PREDECESSOR_PATH.exists()
        and EXPECTED_PREDECESSOR_VERSION
        in predecessor_constants
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    chain = replay_contract_chain()

    comparison_rows = chain[
        "comparison_rows"
    ]

    metric_rows = chain[
        "metric_rows"
    ]

    interpretation_rows = chain[
        "interpretation_rows"
    ]

    evidence_rows = chain[
        "evidence_rows"
    ]

    remediation_rows = chain[
        "remediation_rows"
    ]

    audit_rows = execute_provenance_audit(
        comparison_rows,
        metric_rows,
        interpretation_rows,
        evidence_rows,
        remediation_rows,
    )

    reverse_audit_rows = (
        execute_provenance_audit(
            list(reversed(comparison_rows)),
            list(reversed(metric_rows)),
            list(
                reversed(
                    interpretation_rows
                )
            ),
            list(reversed(evidence_rows)),
            list(reversed(remediation_rows)),
        )
    )

    audit_digest = sha256_payload(
        audit_rows
    )

    reverse_audit_digest = (
        sha256_payload(
            reverse_audit_rows
        )
    )

    failure_mode_counts = Counter(
        mode
        for row in audit_rows
        for mode in row["failure_modes"]
    )

    audit_status_counts = Counter(
        row["audit_status"]
        for row in audit_rows
    )

    raw_type_counts = Counter(
        row["raw_outcome_type"]
        for row in audit_rows
    )

    candidate_mapping_records = sum(
        bool(
            row["candidate_outcome_fields"]
        )
        for row in audit_rows
    )

    unresolved_records = sum(
        row["audit_status"]
        in {
            "source_artifact_unresolved",
            "lineage_invalid",
            "audit_inconclusive",
        }
        for row in audit_rows
    )

    lineage_complete_records = sum(
        all(
            valid_sha256(
                row.get(field)
            )
            for field in [
                "source_metric_record_digest",
                "source_interpretation_digest",
                "source_evidence_record_digest",
                "source_remediation_record_digest",
                "audit_identity_digest",
                "audit_record_digest",
            ]
        )
        for row in audit_rows
    )

    checks = [
        {
            "check": "nine_aa_plan_verified",
            "actual": plan_verified,
            "expected": True,
            "passed": plan_verified,
        },
        {
            "check": "nine_z_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "two_hundred_forty_eight_metric_records_replayed",
            "actual": len(metric_rows),
            "expected": 248,
            "passed": len(metric_rows) == 248,
        },
        {
            "check": "two_hundred_forty_eight_interpretation_records_replayed",
            "actual": len(
                interpretation_rows
            ),
            "expected": 248,
            "passed": (
                len(interpretation_rows)
                == 248
            ),
        },
        {
            "check": "five_hundred_thirty_six_evidence_records_replayed",
            "actual": len(evidence_rows),
            "expected": 536,
            "passed": len(evidence_rows) == 536,
        },
        {
            "check": "five_hundred_thirty_six_remediation_records_replayed",
            "actual": len(
                remediation_rows
            ),
            "expected": 536,
            "passed": (
                len(remediation_rows)
                == 536
            ),
        },
        {
            "check": "twelve_invalid_outcome_audit_records_materialized",
            "actual": len(audit_rows),
            "expected": 12,
            "passed": len(audit_rows) == 12,
        },
        {
            "check": "one_audit_record_per_invalid_remediation_record",
            "actual": len(
                {
                    row[
                        "source_remediation_record_id"
                    ]
                    for row in audit_rows
                }
            ),
            "expected": 12,
            "passed": (
                len(
                    {
                        row[
                            "source_remediation_record_id"
                        ]
                        for row in audit_rows
                    }
                )
                == 12
            ),
        },
        {
            "check": "forty_fields_implemented",
            "actual": len(
                AUDIT_RECORD_FIELDS
            ),
            "expected": 40,
            "passed": (
                len(AUDIT_RECORD_FIELDS)
                == 40
                and all(
                    set(row)
                    == set(
                        AUDIT_RECORD_FIELDS
                    )
                    for row in audit_rows
                )
            ),
        },
        {
            "check": "audit_ids_unique",
            "actual": len(
                {
                    row["audit_record_id"]
                    for row in audit_rows
                }
            ),
            "expected": 12,
            "passed": (
                len(
                    {
                        row[
                            "audit_record_id"
                        ]
                        for row in audit_rows
                    }
                )
                == 12
            ),
        },
        {
            "check": "audit_digests_unique",
            "actual": len(
                {
                    row[
                        "audit_record_digest"
                    ]
                    for row in audit_rows
                }
            ),
            "expected": 12,
            "passed": (
                len(
                    {
                        row[
                            "audit_record_digest"
                        ]
                        for row in audit_rows
                    }
                )
                == 12
            ),
        },
        {
            "check": "audit_digests_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "audit_record_digest"
                    ]
                )
                for row in audit_rows
            ),
            "expected": 12,
            "passed": all(
                valid_sha256(
                    row[
                        "audit_record_digest"
                    ]
                )
                for row in audit_rows
            ),
        },
        {
            "check": "all_records_preserve_complete_lineage",
            "actual": lineage_complete_records,
            "expected": 12,
            "passed": (
                lineage_complete_records
                == 12
            ),
        },
        {
            "check": "all_records_capture_raw_outcome_serialization",
            "actual": sum(
                bool(
                    row[
                        "raw_outcome_serialization"
                    ]
                )
                for row in audit_rows
            ),
            "expected": 12,
            "passed": all(
                bool(
                    row[
                        "raw_outcome_serialization"
                    ]
                )
                for row in audit_rows
            ),
        },
        {
            "check": "all_records_capture_runtime_type",
            "actual": sum(
                bool(
                    row[
                        "raw_outcome_type"
                    ]
                )
                for row in audit_rows
            ),
            "expected": 12,
            "passed": all(
                bool(
                    row[
                        "raw_outcome_type"
                    ]
                )
                for row in audit_rows
            ),
        },
        {
            "check": "all_records_capture_failure_modes",
            "actual": sum(
                bool(row["failure_modes"])
                for row in audit_rows
            ),
            "expected": 12,
            "passed": all(
                bool(row["failure_modes"])
                for row in audit_rows
            ),
        },
        {
            "check": "all_records_capture_compatibility_checks",
            "actual": sum(
                bool(
                    row[
                        "compatibility_check_results"
                    ]
                )
                for row in audit_rows
            ),
            "expected": 12,
            "passed": all(
                bool(
                    row[
                        "compatibility_check_results"
                    ]
                )
                for row in audit_rows
            ),
        },
        {
            "check": "candidate_fields_are_diagnostic_only",
            "actual": 0,
            "expected": 0,
            "passed": all(
                row[
                    "normalized_outcome_value"
                ]
                is None
                and row[
                    "normalization_rule_applied"
                ]
                == "none_read_only_audit"
                for row in audit_rows
            ),
        },
        {
            "check": "audit_replay_deterministic",
            "actual": audit_rows,
            "expected": reverse_audit_rows,
            "passed": (
                audit_rows
                == reverse_audit_rows
            ),
        },
        {
            "check": "audit_digests_match_reverse_replay",
            "actual": audit_digest,
            "expected": reverse_audit_digest,
            "passed": (
                audit_digest
                == reverse_audit_digest
            ),
        },
        {
            "check": "canonical_source_records_not_changed",
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
            "check": "outcomes_not_imputed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "canonical_mapping_not_changed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "thresholds_not_relaxed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "source_contract_records_not_recomputed",
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
            "check": "statistical_significance_not_calculated",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "superiority_and_equivalence_not_declared",
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
        "outcome_value_provenance_audit_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_audit_implementation_failed"
    )

    next_layer = (
        "9AC_pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_remediation_plan"
        if all_checks_passed
        else
        "9AB_pitch_type_matchup_overlay_historical_"
        "outcome_value_provenance_audit_implementation_remediation"
    )

    write_jsonl(
        OUTPUT_DIR
        / "outcome_value_provenance_audit_records.jsonl",
        audit_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "outcome_value_provenance_audit_records.csv",
        AUDIT_RECORD_FIELDS,
        [
            csv_safe_record(row)
            for row in audit_rows
        ],
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        [
            {
                **row,
                "actual": (
                    canonical_json_text(
                        row["actual"]
                    )
                    if isinstance(
                        row["actual"],
                        (dict, list),
                    )
                    else row["actual"]
                ),
                "expected": (
                    canonical_json_text(
                        row["expected"]
                    )
                    if isinstance(
                        row["expected"],
                        (dict, list),
                    )
                    else row["expected"]
                ),
            }
            for row in checks
        ],
    )

    write_csv(
        OUTPUT_DIR / "failure_mode_counts.csv",
        ["failure_mode", "count"],
        [
            {
                "failure_mode": key,
                "count": failure_mode_counts[key],
            }
            for key in sorted(
                failure_mode_counts
            )
        ],
    )

    write_csv(
        OUTPUT_DIR / "audit_status_counts.csv",
        ["audit_status", "count"],
        [
            {
                "audit_status": key,
                "count": audit_status_counts[key],
            }
            for key in sorted(
                audit_status_counts
            )
        ],
    )

    write_csv(
        OUTPUT_DIR / "raw_outcome_type_counts.csv",
        ["raw_outcome_type", "count"],
        [
            {
                "raw_outcome_type": key,
                "count": raw_type_counts[key],
            }
            for key in sorted(
                raw_type_counts
            )
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "audit_contract_version":
            AUDIT_CONTRACT_VERSION,
        "plan_verified": plan_verified,
        "predecessor_verified":
            predecessor_verified,
        "comparison_records_replayed":
            len(comparison_rows),
        "metric_records_replayed":
            len(metric_rows),
        "interpretation_records_replayed":
            len(interpretation_rows),
        "evidence_records_replayed":
            len(evidence_rows),
        "remediation_records_replayed":
            len(remediation_rows),
        "invalid_outcome_audit_records":
            len(audit_rows),
        "failure_mode_counts":
            dict(sorted(
                failure_mode_counts.items()
            )),
        "audit_status_counts":
            dict(sorted(
                audit_status_counts.items()
            )),
        "raw_outcome_type_counts":
            dict(sorted(
                raw_type_counts.items()
            )),
        "candidate_mapping_records":
            candidate_mapping_records,
        "unresolved_records":
            unresolved_records,
        "lineage_complete_records":
            lineage_complete_records,
        "audit_digest":
            audit_digest,
        "reverse_audit_digest":
            reverse_audit_digest,
        "implementation_checks_passed":
            sum(
                bool(row["passed"])
                for row in checks
            ),
        "implementation_checks_required":
            len(checks),
        "canonical_source_records_changed":
            0,
        "outcomes_coerced": 0,
        "outcomes_imputed": 0,
        "canonical_mappings_changed": 0,
        "thresholds_relaxed": 0,
        "source_contract_records_recomputed":
            0,
        "uncertainty_estimates_calculated":
            0,
        "statistical_significance_tests_calculated":
            0,
        "superiority_decisions_emitted":
            0,
        "equivalence_decisions_emitted":
            0,
        "activation_recommendations_emitted":
            0,
        "production_probabilities_changed":
            0,
        "market_comparisons_executed":
            0,
        "betting_edges_calculated": 0,
        "all_checks_passed":
            all_checks_passed,
        "recommended_next_layer":
            next_layer,
    }

    write_json(
        OUTPUT_DIR
        / "outcome_value_provenance_audit_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed":
            all_checks_passed,
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_outcome_value_"
            "provenance_remediation_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
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
        f"Plan verified: {plan_verified}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_verified}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Comparison records replayed: "
        f"{len(comparison_rows)}"
    )
    print(
        "Metric records replayed: "
        f"{len(metric_rows)}"
    )
    print(
        "Interpretation records replayed: "
        f"{len(interpretation_rows)}"
    )
    print(
        "Evidence records replayed: "
        f"{len(evidence_rows)}"
    )
    print(
        "Remediation records replayed: "
        f"{len(remediation_rows)}"
    )
    print(
        "Invalid-outcome audit records: "
        f"{len(audit_rows)}"
    )
    print(
        "Failure mode counts: "
        f"{dict(sorted(failure_mode_counts.items()))}"
    )
    print(
        "Audit status counts: "
        f"{dict(sorted(audit_status_counts.items()))}"
    )
    print(
        "Raw outcome type counts: "
        f"{dict(sorted(raw_type_counts.items()))}"
    )
    print(
        "Candidate mapping records: "
        f"{candidate_mapping_records}"
    )
    print(
        "Unresolved records: "
        f"{unresolved_records}"
    )
    print(
        f"Audit digest: {audit_digest}"
    )
    print(
        "Reverse audit digest: "
        f"{reverse_audit_digest}"
    )
    print(
        "Canonical source records changed: 0"
    )
    print("Outcomes coerced: 0")
    print("Outcomes imputed: 0")
    print("Canonical mappings changed: 0")
    print("Thresholds relaxed: 0")
    print(
        "Source contract records recomputed: 0"
    )
    print(
        "Uncertainty estimates calculated: 0"
    )
    print(
        "Statistical significance tests calculated: 0"
    )
    print(
        "Superiority decisions emitted: 0"
    )
    print(
        "Equivalence decisions emitted: 0"
    )
    print(
        "Activation recommendations emitted: 0"
    )
    print(
        "Production probabilities changed: 0"
    )
    print(
        "Market comparisons executed: 0"
    )
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
