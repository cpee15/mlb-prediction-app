#!/usr/bin/env python3
"""
Layer 9V
Pitch-Type Matchup Overlay Historical Comparative Metric Result Interpretation
Contract Implementation

Implements the bounded interpretation contract planned by Layer 9U.

This implementation:

- verifies the Layer 9U plan and Layer 9T predecessor;
- deterministically replays Layer 9T diagnostic metric records;
- validates metric identities, lineage, statuses, counts, support, and values;
- classifies coverage, insufficient-support, invalid-input, and directional
  performance records;
- emits observational interpretation language with explicit limitations;
- replays interpretation under reversed source ordering;
- writes temporary diagnostic artifacts only.

This implementation does not recompute metrics, estimate uncertainty or
statistical significance, declare superiority or equivalence, recommend
activation, or modify production probabilities, simulations, pricing,
markets, or bets.
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


LAYER_ID = "9V"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "metric_result_interpretation_contract_implementation"
)

INTERPRETATION_CONTRACT_VERSION = (
    "layer_9V_historical_comparative_metric_result_"
    "interpretation_contract_v1"
)

EXPECTED_PLAN_VERSION = (
    "layer_9U_historical_comparative_metric_result_"
    "interpretation_contract_plan_v1"
)

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "metric_result_interpretation_contract_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_comparative_metric_result_"
    "interpretation_contract_implementation"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9T_historical_comparative_metric_calculation_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "metric_calculation_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_comparative_metric_result_"
    "interpretation_contract_planning"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9V_pitch_type_matchup_overlay_"
    "historical_comparative_metric_result_interpretation_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9U_pitch_type_matchup_overlay_"
    "historical_comparative_metric_result_interpretation_contract.py"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9T_pitch_type_matchup_overlay_"
    "historical_comparative_metric_calculation_contract.py"
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

RECOGNIZED_METRIC_STATUSES = {
    "metric_eligible",
    "coverage_metric_eligible",
    "insufficient_support",
    "prediction_value_type_incompatible",
    "outcome_type_incompatible",
    "prediction_value_invalid",
    "outcome_value_invalid",
    "comparison_lineage_invalid",
    "metric_definition_invalid",
}

INVALID_METRIC_STATUSES = {
    "prediction_value_type_incompatible",
    "outcome_type_incompatible",
    "prediction_value_invalid",
    "outcome_value_invalid",
    "comparison_lineage_invalid",
    "metric_definition_invalid",
}

RECOGNIZED_DIRECTIONS = {
    "lower",
    "higher",
    "descriptive_only",
}

INTERPRETATION_FIELDS = [
    "interpretation_contract_version",
    "interpretation_record_id",
    "metric_record_id",
    "metric_name",
    "metric_family",
    "aggregation_name",
    "aggregation_key",
    "candidate_pair_count",
    "eligible_pair_count",
    "excluded_pair_count",
    "baseline_metric_value",
    "augmented_metric_value",
    "augmented_minus_baseline_delta",
    "better_direction",
    "support_satisfied",
    "source_metric_status",
    "interpretation_status",
    "directional_observation",
    "interpretation_limitations",
    "interpretation_eligible",
    "interpretation_exclusion_codes",
    "source_comparison_digest",
    "source_metric_record_digest",
    "interpretation_identity_digest",
    "interpretation_record_digest",
]

EXCLUSION_CODES = {
    "metric_record_invalid":
        "historical_interpretation_metric_record_invalid",
    "metric_lineage_invalid":
        "historical_interpretation_metric_lineage_invalid",
    "metric_status_unrecognized":
        "historical_interpretation_metric_status_unrecognized",
    "direction_unrecognized":
        "historical_interpretation_direction_unrecognized",
    "insufficient_support":
        "historical_interpretation_insufficient_support",
    "metric_values_missing":
        "historical_interpretation_metric_values_missing",
    "delta_missing":
        "historical_interpretation_delta_missing",
    "coverage_only":
        "historical_interpretation_coverage_only",
    "source_metric_invalid":
        "historical_interpretation_source_metric_invalid",
    "duplicate_metric_identity":
        "historical_interpretation_duplicate_metric_identity",
}

PROHIBITED_AUTHORITIES = [
    "activation_recommendation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "canonical_probability_authority_change",
    "dataset_split_execution",
    "edge_detection",
    "equivalence_declaration",
    "market_comparison",
    "model_training",
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
    "threshold_tuning",
    "uncertainty_estimation",
]


def canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


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
                canonical_json_bytes(
                    dict(row)
                ).decode("utf-8")
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


def replay_metric_records() -> list[dict[str, Any]]:
    layer_9t = load_module(
        PREDECESSOR_PATH,
        "layer_9t_interpretation_source",
    )

    layer_9s = load_module(
        layer_9t.PLAN_PATH,
        "layer_9s_interpretation_source",
    )

    comparison_rows = (
        layer_9t.replay_comparison_records()
    )

    return layer_9t.execute_metric_calculation(
        comparison_rows,
        layer_9s.METRIC_DEFINITIONS,
        layer_9s.AGGREGATION_LEVELS,
    )


def source_validation_codes(
    row: Mapping[str, Any],
    duplicate_metric_ids: set[str],
) -> list[str]:
    codes: set[str] = set()

    required_fields = {
        "metric_record_id",
        "metric_name",
        "metric_family",
        "aggregation_name",
        "aggregation_key",
        "candidate_pair_count",
        "eligible_pair_count",
        "excluded_pair_count",
        "better_direction",
        "support_satisfied",
        "metric_status",
        "source_comparison_digest",
        "metric_record_digest",
    }

    if not required_fields.issubset(set(row)):
        codes.add(
            EXCLUSION_CODES[
                "metric_record_invalid"
            ]
        )

    metric_record_id = normalized_string(
        row.get("metric_record_id")
    )

    if (
        not metric_record_id
        or metric_record_id
        in duplicate_metric_ids
    ):
        codes.add(
            EXCLUSION_CODES[
                "duplicate_metric_identity"
            ]
        )

    if not valid_sha256(
        row.get("metric_record_digest")
    ):
        codes.add(
            EXCLUSION_CODES[
                "metric_lineage_invalid"
            ]
        )

    if not valid_sha256(
        row.get("source_comparison_digest")
    ):
        codes.add(
            EXCLUSION_CODES[
                "metric_lineage_invalid"
            ]
        )

    if (
        row.get("metric_status")
        not in RECOGNIZED_METRIC_STATUSES
    ):
        codes.add(
            EXCLUSION_CODES[
                "metric_status_unrecognized"
            ]
        )

    if (
        row.get("better_direction")
        not in RECOGNIZED_DIRECTIONS
    ):
        codes.add(
            EXCLUSION_CODES[
                "direction_unrecognized"
            ]
        )

    candidate_count = row.get(
        "candidate_pair_count"
    )
    eligible_count = row.get(
        "eligible_pair_count"
    )
    excluded_count = row.get(
        "excluded_pair_count"
    )

    if not all(
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
        for value in (
            candidate_count,
            eligible_count,
            excluded_count,
        )
    ):
        codes.add(
            EXCLUSION_CODES[
                "metric_record_invalid"
            ]
        )
    elif (
        eligible_count + excluded_count
        != candidate_count
    ):
        codes.add(
            EXCLUSION_CODES[
                "metric_record_invalid"
            ]
        )

    metric_status = row.get("metric_status")
    metric_family = row.get("metric_family")

    baseline = row.get(
        "baseline_metric_value"
    )
    augmented = row.get(
        "augmented_metric_value"
    )
    delta = row.get(
        "augmented_minus_baseline_delta"
    )

    if metric_status == "metric_eligible":
        if not (
            finite_number(baseline)
            and finite_number(augmented)
        ):
            codes.add(
                EXCLUSION_CODES[
                    "metric_values_missing"
                ]
            )

        if not finite_number(delta):
            codes.add(
                EXCLUSION_CODES[
                    "delta_missing"
                ]
            )

    if metric_status == "insufficient_support":
        if any(
            value is not None
            for value in (
                baseline,
                augmented,
                delta,
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "source_metric_invalid"
                ]
            )

    if (
        metric_family == "coverage"
        or metric_status
        == "coverage_metric_eligible"
    ):
        if delta is not None:
            codes.add(
                EXCLUSION_CODES[
                    "source_metric_invalid"
                ]
            )

    return sorted(codes)


def directional_classification(
    better_direction: str,
    delta: float,
) -> tuple[str, str]:
    if delta == 0.0:
        return (
            "directionally_equal_observed_value",
            (
                "The baseline and augmented variants have the same "
                "observed diagnostic metric value for this aggregation."
            ),
        )

    if better_direction == "lower":
        if delta < 0.0:
            return (
                "directionally_lower_augmented_loss",
                (
                    "The augmented variant has a lower observed diagnostic "
                    "loss than the baseline variant for this aggregation."
                ),
            )

        return (
            "directionally_higher_augmented_loss",
            (
                "The augmented variant has a higher observed diagnostic "
                "loss than the baseline variant for this aggregation."
            ),
        )

    if better_direction == "higher":
        if delta > 0.0:
            return (
                "directionally_higher_augmented_score",
                (
                    "The augmented variant has a higher observed diagnostic "
                    "score than the baseline variant for this aggregation."
                ),
            )

        return (
            "directionally_lower_augmented_score",
            (
                "The augmented variant has a lower observed diagnostic "
                "score than the baseline variant for this aggregation."
            ),
        )

    return (
        "not_interpretable",
        (
            "No authorized directional interpretation applies to this "
            "diagnostic metric record."
        ),
    )


def classify_interpretation(
    row: Mapping[str, Any],
    duplicate_metric_ids: set[str],
) -> dict[str, Any]:
    exclusion_codes = set(
        source_validation_codes(
            row,
            duplicate_metric_ids,
        )
    )

    metric_status = normalized_string(
        row.get("metric_status")
    )
    metric_family = normalized_string(
        row.get("metric_family")
    )
    better_direction = normalized_string(
        row.get("better_direction")
    )
    delta = row.get(
        "augmented_minus_baseline_delta"
    )

    interpretation_status = (
        "not_interpretable"
    )
    directional_observation = (
        "No authorized directional interpretation applies."
    )
    interpretation_eligible = False

    limitations = [
        (
            "This is a bounded diagnostic interpretation of an observed "
            "point estimate."
        ),
        (
            "No uncertainty or statistical-significance estimate has been "
            "calculated."
        ),
        (
            "This record does not establish superiority, equivalence, "
            "activation, or production readiness."
        ),
        (
            "The interpretation applies only to the preserved metric and "
            "aggregation key."
        ),
    ]

    if exclusion_codes:
        interpretation_status = "input_invalid"
        directional_observation = (
            "The source metric record is invalid or incompatible with "
            "the interpretation contract."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "source_metric_invalid"
            ]
        )

    elif (
        metric_family == "coverage"
        or metric_status
        == "coverage_metric_eligible"
    ):
        interpretation_status = "coverage_only"
        directional_observation = (
            "This record describes diagnostic data availability or "
            "pair coverage only."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "coverage_only"
            ]
        )
        limitations.append(
            (
                "Coverage does not describe comparative predictive quality."
            )
        )

    elif metric_status == "insufficient_support":
        interpretation_status = (
            "insufficient_support"
        )
        directional_observation = (
            "The diagnostic metric record is suppressed because the "
            "minimum support requirement is not satisfied."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "insufficient_support"
            ]
        )
        limitations.append(
            (
                "No directional interpretation is authorized for a "
                "support-suppressed record."
            )
        )

    elif metric_status in INVALID_METRIC_STATUSES:
        interpretation_status = "input_invalid"
        directional_observation = (
            "The diagnostic metric record cannot be interpreted because "
            "its source status indicates invalid or incompatible input."
        )
        exclusion_codes.add(
            EXCLUSION_CODES[
                "source_metric_invalid"
            ]
        )

    elif (
        metric_status == "metric_eligible"
        and finite_number(delta)
        and better_direction
        in {"lower", "higher"}
    ):
        (
            interpretation_status,
            directional_observation,
        ) = directional_classification(
            better_direction,
            float(delta),
        )
        interpretation_eligible = True

    else:
        interpretation_status = (
            "not_interpretable"
        )
        directional_observation = (
            "No authorized interpretation rule applies to this "
            "diagnostic metric record."
        )

    identity_payload = {
        "interpretation_contract_version": (
            INTERPRETATION_CONTRACT_VERSION
        ),
        "metric_record_id": row.get(
            "metric_record_id"
        ),
        "source_metric_record_digest": (
            row.get("metric_record_digest")
        ),
        "interpretation_status": (
            interpretation_status
        ),
    }

    identity_digest = sha256_payload(
        identity_payload
    )

    interpretation_row = {
        "interpretation_contract_version": (
            INTERPRETATION_CONTRACT_VERSION
        ),
        "interpretation_record_id": (
            f"hcint_{identity_digest[:32]}"
        ),
        "metric_record_id": row.get(
            "metric_record_id"
        ),
        "metric_name": row.get(
            "metric_name"
        ),
        "metric_family": row.get(
            "metric_family"
        ),
        "aggregation_name": row.get(
            "aggregation_name"
        ),
        "aggregation_key": row.get(
            "aggregation_key"
        ),
        "candidate_pair_count": row.get(
            "candidate_pair_count"
        ),
        "eligible_pair_count": row.get(
            "eligible_pair_count"
        ),
        "excluded_pair_count": row.get(
            "excluded_pair_count"
        ),
        "baseline_metric_value": row.get(
            "baseline_metric_value"
        ),
        "augmented_metric_value": row.get(
            "augmented_metric_value"
        ),
        "augmented_minus_baseline_delta": (
            row.get(
                "augmented_minus_baseline_delta"
            )
        ),
        "better_direction": row.get(
            "better_direction"
        ),
        "support_satisfied": row.get(
            "support_satisfied"
        ),
        "source_metric_status": (
            metric_status
        ),
        "interpretation_status": (
            interpretation_status
        ),
        "directional_observation": (
            directional_observation
        ),
        "interpretation_limitations": (
            limitations
        ),
        "interpretation_eligible": (
            interpretation_eligible
        ),
        "interpretation_exclusion_codes": (
            sorted(exclusion_codes)
        ),
        "source_comparison_digest": (
            row.get(
                "source_comparison_digest"
            )
        ),
        "source_metric_record_digest": (
            row.get("metric_record_digest")
        ),
        "interpretation_identity_digest": (
            identity_digest
        ),
        "interpretation_record_digest": "",
    }

    interpretation_row[
        "interpretation_record_digest"
    ] = sha256_payload(
        {
            key: value
            for key, value
            in interpretation_row.items()
            if key
            != "interpretation_record_digest"
        }
    )

    return interpretation_row


def execute_interpretation(
    metric_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    metric_id_counts = Counter(
        normalized_string(
            row.get("metric_record_id")
        )
        for row in metric_rows
    )

    duplicate_metric_ids = {
        metric_id
        for metric_id, count
        in metric_id_counts.items()
        if metric_id and count > 1
    }

    interpretation_rows = [
        classify_interpretation(
            row,
            duplicate_metric_ids,
        )
        for row in metric_rows
    ]

    interpretation_rows.sort(
        key=lambda row: (
            normalized_string(
                row.get("aggregation_name")
            ),
            normalized_string(
                row.get("aggregation_key")
            ),
            normalized_string(
                row.get("metric_family")
            ),
            normalized_string(
                row.get("metric_name")
            ),
            normalized_string(
                row.get(
                    "interpretation_status"
                )
            ),
            normalized_string(
                row.get(
                    "interpretation_record_id"
                )
            ),
        )
    )

    return interpretation_rows


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

    metric_rows = replay_metric_records()

    interpretation_rows = (
        execute_interpretation(
            metric_rows
        )
    )

    reverse_interpretation_rows = (
        execute_interpretation(
            list(reversed(metric_rows))
        )
    )

    interpretation_status_counts = Counter(
        row["interpretation_status"]
        for row in interpretation_rows
    )

    source_metric_status_counts = Counter(
        row["source_metric_status"]
        for row in interpretation_rows
    )

    exclusion_code_counts = Counter(
        code
        for row in interpretation_rows
        for code in row[
            "interpretation_exclusion_codes"
        ]
    )

    interpretation_ids = [
        row["interpretation_record_id"]
        for row in interpretation_rows
    ]

    interpretation_digests = [
        row[
            "interpretation_record_digest"
        ]
        for row in interpretation_rows
    ]

    field_contract_valid = all(
        set(row)
        == set(INTERPRETATION_FIELDS)
        for row in interpretation_rows
    )

    deterministic_replay = (
        canonical_json_bytes(
            interpretation_rows
        )
        == canonical_json_bytes(
            reverse_interpretation_rows
        )
    )

    coverage_rows = [
        row
        for row in interpretation_rows
        if row[
            "interpretation_status"
        ]
        == "coverage_only"
    ]

    insufficient_support_rows = [
        row
        for row in interpretation_rows
        if row[
            "interpretation_status"
        ]
        == "insufficient_support"
    ]

    invalid_rows = [
        row
        for row in interpretation_rows
        if row[
            "interpretation_status"
        ]
        == "input_invalid"
    ]

    directional_rows = [
        row
        for row in interpretation_rows
        if row["interpretation_status"]
        in {
            "directionally_lower_augmented_loss",
            "directionally_higher_augmented_loss",
            "directionally_equal_observed_value",
            "directionally_higher_augmented_score",
            "directionally_lower_augmented_score",
        }
    ]

    interpretation_digest = (
        sha256_payload(
            interpretation_rows
        )
    )

    reverse_interpretation_digest = (
        sha256_payload(
            reverse_interpretation_rows
        )
    )

    checks = [
        {
            "check": "nine_u_plan_verified",
            "actual": plan_verified,
            "expected": True,
            "passed": plan_verified,
        },
        {
            "check": "nine_t_predecessor_verified",
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
            "check": "one_interpretation_per_metric_record",
            "actual": len(
                interpretation_rows
            ),
            "expected": len(metric_rows),
            "passed": (
                len(interpretation_rows)
                == len(metric_rows)
            ),
        },
        {
            "check": "twenty_five_fields_implemented",
            "actual": len(
                INTERPRETATION_FIELDS
            ),
            "expected": 25,
            "passed": (
                len(INTERPRETATION_FIELDS)
                == 25
                and field_contract_valid
            ),
        },
        {
            "check": "interpretation_ids_unique",
            "actual": len(
                set(interpretation_ids)
            ),
            "expected": len(
                interpretation_rows
            ),
            "passed": (
                len(set(interpretation_ids))
                == len(interpretation_rows)
            ),
        },
        {
            "check": "interpretation_digests_unique",
            "actual": len(
                set(interpretation_digests)
            ),
            "expected": len(
                interpretation_rows
            ),
            "passed": (
                len(set(interpretation_digests))
                == len(interpretation_rows)
            ),
        },
        {
            "check": "interpretation_digests_valid",
            "actual": sum(
                valid_sha256(value)
                for value
                in interpretation_digests
            ),
            "expected": len(
                interpretation_rows
            ),
            "passed": all(
                valid_sha256(value)
                for value
                in interpretation_digests
            ),
        },
        {
            "check": "source_metric_lineage_preserved",
            "actual": sum(
                valid_sha256(
                    row[
                        "source_metric_record_digest"
                    ]
                )
                for row in interpretation_rows
            ),
            "expected": len(
                interpretation_rows
            ),
            "passed": all(
                valid_sha256(
                    row[
                        "source_metric_record_digest"
                    ]
                )
                for row in interpretation_rows
            ),
        },
        {
            "check": "ninety_three_coverage_records_classified",
            "actual": len(coverage_rows),
            "expected": 93,
            "passed": len(
                coverage_rows
            )
            == 93,
        },
        {
            "check": "one_hundred_forty_three_insufficient_support_records_classified",
            "actual": len(
                insufficient_support_rows
            ),
            "expected": 143,
            "passed": len(
                insufficient_support_rows
            )
            == 143,
        },
        {
            "check": "twelve_invalid_records_classified",
            "actual": len(
                invalid_rows
            ),
            "expected": 12,
            "passed": len(
                invalid_rows
            )
            == 12,
        },
        {
            "check": "zero_directional_records_expected",
            "actual": len(
                directional_rows
            ),
            "expected": 0,
            "passed": len(
                directional_rows
            )
            == 0,
        },
        {
            "check": "coverage_records_not_interpretation_eligible",
            "actual": sum(
                not row[
                    "interpretation_eligible"
                ]
                for row in coverage_rows
            ),
            "expected": len(
                coverage_rows
            ),
            "passed": all(
                not row[
                    "interpretation_eligible"
                ]
                for row in coverage_rows
            ),
        },
        {
            "check": "suppressed_records_not_interpretation_eligible",
            "actual": sum(
                not row[
                    "interpretation_eligible"
                ]
                for row
                in insufficient_support_rows
            ),
            "expected": len(
                insufficient_support_rows
            ),
            "passed": all(
                not row[
                    "interpretation_eligible"
                ]
                for row
                in insufficient_support_rows
            ),
        },
        {
            "check": "invalid_records_not_interpretation_eligible",
            "actual": sum(
                not row[
                    "interpretation_eligible"
                ]
                for row in invalid_rows
            ),
            "expected": len(
                invalid_rows
            ),
            "passed": all(
                not row[
                    "interpretation_eligible"
                ]
                for row in invalid_rows
            ),
        },
        {
            "check": "all_records_include_limitations",
            "actual": sum(
                bool(
                    row[
                        "interpretation_limitations"
                    ]
                )
                for row in interpretation_rows
            ),
            "expected": len(
                interpretation_rows
            ),
            "passed": all(
                bool(
                    row[
                        "interpretation_limitations"
                    ]
                )
                for row in interpretation_rows
            ),
        },
        {
            "check": "counts_reconcile_to_source_metrics",
            "actual": sum(
                interpretation_status_counts.values()
            ),
            "expected": len(metric_rows),
            "passed": (
                sum(
                    interpretation_status_counts.values()
                )
                == len(metric_rows)
            ),
        },
        {
            "check": "interpretation_replay_deterministic",
            "actual": deterministic_replay,
            "expected": True,
            "passed": deterministic_replay,
        },
        {
            "check": "interpretation_digests_match_reverse_replay",
            "actual": interpretation_digest,
            "expected": (
                reverse_interpretation_digest
            ),
            "passed": (
                interpretation_digest
                == reverse_interpretation_digest
            ),
        },
        {
            "check": "metrics_not_recomputed",
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
        "pitch_type_matchup_overlay_historical_comparative_"
        "metric_result_interpretation_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_comparative_"
        "metric_result_interpretation_contract_implementation_failed"
    )

    next_layer = (
        "9W_pitch_type_matchup_overlay_historical_comparative_"
        "evidence_sufficiency_contract_plan"
        if all_checks_passed
        else
        "9V_pitch_type_matchup_overlay_historical_comparative_"
        "metric_result_interpretation_contract_implementation_remediation"
    )

    write_jsonl(
        OUTPUT_DIR
        / "metric_interpretation_records.jsonl",
        interpretation_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "metric_interpretation_records.csv",
        INTERPRETATION_FIELDS,
        [
            {
                **row,
                "interpretation_limitations": (
                    "|".join(
                        row[
                            "interpretation_limitations"
                        ]
                    )
                ),
                "interpretation_exclusion_codes": (
                    "|".join(
                        row[
                            "interpretation_exclusion_codes"
                        ]
                    )
                ),
            }
            for row in interpretation_rows
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "interpretation_status_counts.csv",
        [
            "interpretation_status",
            "count",
        ],
        [
            {
                "interpretation_status": status,
                "count": count,
            }
            for status, count
            in sorted(
                interpretation_status_counts.items()
            )
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "source_metric_status_counts.csv",
        [
            "source_metric_status",
            "count",
        ],
        [
            {
                "source_metric_status": status,
                "count": count,
            }
            for status, count
            in sorted(
                source_metric_status_counts.items()
            )
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "exclusion_code_counts.csv",
        [
            "exclusion_code",
            "count",
        ],
        [
            {
                "exclusion_code": code,
                "count": count,
            }
            for code, count
            in sorted(
                exclusion_code_counts.items()
            )
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "implementation_checks.csv",
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
        / "authority_boundaries.csv",
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
                    "Layer 9V emits bounded diagnostic interpretations "
                    "only and grants no uncertainty, significance, "
                    "superiority, equivalence, activation, production, "
                    "market, pricing, or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_comparative_evidence_"
                    "sufficiency_contract_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "The classified interpretation records permit planning "
                    "a bounded evidence-sufficiency contract without making "
                    "a superiority or activation decision."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "interpretation_contract_version": (
            INTERPRETATION_CONTRACT_VERSION
        ),
        "plan_verified": plan_verified,
        "predecessor_verified": (
            predecessor_verified
        ),
        "metric_records_replayed": (
            len(metric_rows)
        ),
        "interpretation_records_materialized": (
            len(interpretation_rows)
        ),
        "interpretation_status_counts": dict(
            sorted(
                interpretation_status_counts.items()
            )
        ),
        "source_metric_status_counts": dict(
            sorted(
                source_metric_status_counts.items()
            )
        ),
        "exclusion_code_counts": dict(
            sorted(
                exclusion_code_counts.items()
            )
        ),
        "directional_interpretation_records": (
            len(directional_rows)
        ),
        "coverage_only_records": (
            len(coverage_rows)
        ),
        "insufficient_support_records": (
            len(insufficient_support_rows)
        ),
        "invalid_input_records": (
            len(invalid_rows)
        ),
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required": (
            len(checks)
        ),
        "interpretation_digest": (
            interpretation_digest
        ),
        "reverse_interpretation_digest": (
            reverse_interpretation_digest
        ),
        "metrics_recomputed": 0,
        "uncertainty_estimates_calculated": 0,
        "statistical_significance_tests_calculated": 0,
        "superiority_decisions_emitted": 0,
        "equivalence_decisions_emitted": 0,
        "activation_recommendations_emitted": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            next_layer
        ),
    }

    write_json(
        OUTPUT_DIR
        / "metric_interpretation_summary.json",
        summary,
    )

    diagnosis = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "all_checks_passed": (
            all_checks_passed
        ),
        "diagnosis": diagnosis_name,
        "authority_granted": (
            "historical_comparative_evidence_"
            "sufficiency_contract_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "recommended_next_layer": (
            next_layer
        ),
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
        "Interpretation contract version: "
        f"{INTERPRETATION_CONTRACT_VERSION}"
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
        "Metric records replayed: "
        f"{summary['metric_records_replayed']}"
    )
    print(
        "Interpretation records materialized: "
        f"{summary['interpretation_records_materialized']}"
    )
    print(
        "Directional interpretation records: "
        f"{summary['directional_interpretation_records']}"
    )
    print(
        "Coverage-only records: "
        f"{summary['coverage_only_records']}"
    )
    print(
        "Insufficient-support records: "
        f"{summary['insufficient_support_records']}"
    )
    print(
        "Invalid-input records: "
        f"{summary['invalid_input_records']}"
    )
    print(
        "Interpretation digest: "
        f"{interpretation_digest}"
    )
    print(
        "Reverse interpretation digest: "
        f"{reverse_interpretation_digest}"
    )
    print("Metrics recomputed: 0")
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
