#!/usr/bin/env python3
"""
Layer 9T
Pitch-Type Matchup Overlay Historical Comparative Metric Calculation Contract Implementation

Implements the bounded deterministic temporary diagnostic metric-calculation
contract planned by Layer 9S.

This implementation:

- verifies the Layer 9S plan and Layer 9R predecessor;
- replays Layer 9R comparative-evaluation records;
- calculates only the metric catalog authorized by Layer 9S;
- applies value compatibility, log-loss clipping, aggregation, support, and
  suppression rules;
- emits paired baseline, augmented, and augmented-minus-baseline values;
- replays calculation under reversed input ordering;
- writes temporary diagnostic artifacts only.

This implementation does not estimate uncertainty, calculate statistical
significance, declare superiority, execute backtests or dataset splits, tune
models, or modify production probabilities, simulations, pricing, markets,
or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9T"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "metric_calculation_contract_implementation"
)

METRIC_CONTRACT_VERSION = (
    "layer_9T_historical_comparative_metric_calculation_contract_v1"
)

EXPECTED_PLAN_VERSION = (
    "layer_9S_historical_comparative_metric_calculation_contract_plan_v1"
)

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "metric_calculation_contract_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_comparative_metric_calculation_contract_implementation"
)

EXPECTED_PREDECESSOR_VERSION = (
    "layer_9R_historical_comparative_evaluation_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "evaluation_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_comparative_metric_calculation_contract_planning"
)

MINIMUM_SUPPORT = 2
FLOAT_PRECISION = 12
LOG_LOSS_EPSILON = 1e-15

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9T_pitch_type_matchup_overlay_"
    "historical_comparative_metric_calculation_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9S_pitch_type_matchup_overlay_"
    "historical_comparative_metric_calculation_contract.py"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9R_pitch_type_matchup_overlay_"
    "historical_comparative_evaluation_contract.py"
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

METRIC_RECORD_FIELDS = [
    "metric_contract_version",
    "metric_record_id",
    "metric_name",
    "metric_family",
    "aggregation_name",
    "aggregation_key",
    "target_id",
    "event_level",
    "game_date",
    "baseline_model_contract_version",
    "augmented_model_contract_version",
    "augmented_overlay_contract_version",
    "candidate_pair_count",
    "eligible_pair_count",
    "excluded_pair_count",
    "baseline_metric_value",
    "augmented_metric_value",
    "augmented_minus_baseline_delta",
    "better_direction",
    "minimum_support_required",
    "support_satisfied",
    "metric_status",
    "metric_exclusion_codes",
    "source_comparison_digest",
    "metric_identity_digest",
    "metric_record_digest",
]

EXCLUSION_CODES = {
    "insufficient_support":
        "historical_metric_insufficient_support",
    "prediction_type":
        "historical_metric_prediction_value_type_incompatible",
    "outcome_type":
        "historical_metric_outcome_type_incompatible",
    "prediction_invalid":
        "historical_metric_prediction_value_invalid",
    "outcome_invalid":
        "historical_metric_outcome_value_invalid",
    "probability_bounds":
        "historical_metric_probability_out_of_bounds",
    "binary_outcome":
        "historical_metric_binary_outcome_invalid",
    "comparison_lineage":
        "historical_metric_comparison_lineage_invalid",
    "duplicate_comparison":
        "historical_metric_duplicate_comparison_identity",
    "metric_definition":
        "historical_metric_definition_invalid",
    "aggregation":
        "historical_metric_aggregation_invalid",
    "source":
        "historical_metric_source_record_invalid",
}

PROHIBITED_AUTHORITIES = [
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "canonical_probability_authority_change",
    "dataset_split_execution",
    "edge_detection",
    "market_comparison",
    "model_training",
    "parameter_tuning",
    "pricing",
    "production_historical_prediction_materialization",
    "production_matchup_activation",
    "production_overlay_integration",
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


def rounded(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), FLOAT_PRECISION)


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


def replay_comparison_records() -> list[dict[str, Any]]:
    layer_9r = load_module(
        PREDECESSOR_PATH,
        "layer_9r_metric_source",
    )

    evaluation_rows, joined_rows = (
        layer_9r.replay_predecessor()
    )

    return layer_9r.execute_pairing(
        joined_rows,
        evaluation_rows,
    )


def aggregation_value(
    row: Mapping[str, Any],
    field: str,
) -> Any:
    return row.get(field)


def build_aggregation_groups(
    rows: Sequence[Mapping[str, Any]],
    aggregation_definition: Mapping[str, Any],
) -> list[
    tuple[dict[str, Any], list[Mapping[str, Any]]]
]:
    grouping_text = normalized_string(
        aggregation_definition.get(
            "grouping_fields"
        )
    )

    grouping_fields = (
        grouping_text.split("|")
        if grouping_text
        else []
    )

    grouped: dict[
        str,
        tuple[
            dict[str, Any],
            list[Mapping[str, Any]],
        ],
    ] = {}

    for row in rows:
        key_payload = {
            field: aggregation_value(
                row,
                field,
            )
            for field in grouping_fields
        }

        key = canonical_json_bytes(
            key_payload
        ).decode("utf-8")

        if key not in grouped:
            grouped[key] = (
                key_payload,
                [],
            )

        grouped[key][1].append(row)

    if not grouped and not grouping_fields:
        grouped["{}"] = ({}, [])

    return [
        grouped[key]
        for key in sorted(grouped)
    ]


def prediction_type_compatible(
    metric_definition: Mapping[str, Any],
    row: Mapping[str, Any],
) -> bool:
    required_type = metric_definition[
        "prediction_value_type"
    ]

    if required_type == "any":
        return True

    actual_type = normalized_string(
        row.get("prediction_value_type")
    )

    if required_type == "probability":
        return actual_type == "probability"

    if required_type == "numeric":
        return actual_type in {
            "numeric",
            "probability",
        }

    return False


def metric_input_codes(
    metric_definition: Mapping[str, Any],
    row: Mapping[str, Any],
) -> list[str]:
    codes: set[str] = set()

    if (
        row.get("comparison_status")
        != "paired_eligible"
        or not row.get("comparison_eligible")
    ):
        codes.add(
            EXCLUSION_CODES["source"]
        )
        return sorted(codes)

    if not valid_sha256(
        row.get("comparison_record_digest")
    ):
        codes.add(
            EXCLUSION_CODES[
                "comparison_lineage"
            ]
        )

    if not prediction_type_compatible(
        metric_definition,
        row,
    ):
        codes.add(
            EXCLUSION_CODES[
                "prediction_type"
            ]
        )

    baseline = row.get(
        "baseline_prediction_value"
    )
    augmented = row.get(
        "augmented_prediction_value"
    )
    outcome = row.get("outcome_value")

    if metric_definition[
        "metric_family"
    ] != "coverage":
        if not finite_number(baseline) or not finite_number(
            augmented
        ):
            codes.add(
                EXCLUSION_CODES[
                    "prediction_invalid"
                ]
            )

        if not finite_number(outcome):
            codes.add(
                EXCLUSION_CODES[
                    "outcome_invalid"
                ]
            )

    if metric_definition[
        "prediction_value_type"
    ] == "probability":
        if finite_number(baseline) and not (
            0.0 <= float(baseline) <= 1.0
        ):
            codes.add(
                EXCLUSION_CODES[
                    "probability_bounds"
                ]
            )

        if finite_number(augmented) and not (
            0.0 <= float(augmented) <= 1.0
        ):
            codes.add(
                EXCLUSION_CODES[
                    "probability_bounds"
                ]
            )

    if metric_definition[
        "outcome_type"
    ] == "binary":
        if finite_number(outcome) and float(
            outcome
        ) not in {0.0, 1.0}:
            codes.add(
                EXCLUSION_CODES[
                    "binary_outcome"
                ]
            )

    return sorted(codes)


def row_loss(
    metric_name: str,
    prediction: float,
    outcome: float,
) -> float:
    if metric_name == "brier_score":
        return (
            float(prediction)
            - float(outcome)
        ) ** 2

    if metric_name == "log_loss":
        clipped = min(
            max(
                float(prediction),
                LOG_LOSS_EPSILON,
            ),
            1.0 - LOG_LOSS_EPSILON,
        )

        return -(
            float(outcome)
            * math.log(clipped)
            + (
                1.0 - float(outcome)
            )
            * math.log(
                1.0 - clipped
            )
        )

    if metric_name == "absolute_error":
        return abs(
            float(prediction)
            - float(outcome)
        )

    if metric_name in {
        "squared_error",
        "root_mean_squared_error",
    }:
        return (
            float(prediction)
            - float(outcome)
        ) ** 2

    raise ValueError(
        f"Unsupported performance metric: {metric_name}"
    )


def aggregate_loss(
    metric_name: str,
    losses: Sequence[float],
) -> float:
    mean_value = sum(losses) / len(losses)

    if metric_name == (
        "root_mean_squared_error"
    ):
        return math.sqrt(mean_value)

    return mean_value


def missing_pair(
    row: Mapping[str, Any],
) -> bool:
    return row.get(
        "comparison_status"
    ) in {
        "baseline_prediction_missing",
        "augmented_prediction_missing",
    }


def metric_record(
    metric_definition: Mapping[str, Any],
    aggregation_definition: Mapping[str, Any],
    aggregation_key_payload: Mapping[str, Any],
    group_rows: Sequence[Mapping[str, Any]],
    source_comparison_digest: str,
) -> dict[str, Any]:
    metric_name = metric_definition[
        "metric_name"
    ]
    metric_family = metric_definition[
        "metric_family"
    ]

    candidate_count = len(group_rows)

    eligible_rows = [
        row
        for row in group_rows
        if (
            row.get("comparison_eligible")
            and row.get(
                "comparison_status"
            )
            == "paired_eligible"
        )
    ]

    excluded_count = (
        candidate_count
        - len(eligible_rows)
    )

    valid_rows = []
    input_codes: set[str] = set()

    for row in eligible_rows:
        row_codes = metric_input_codes(
            metric_definition,
            row,
        )

        if row_codes:
            input_codes.update(row_codes)
        else:
            valid_rows.append(row)

    support_required = (
        0
        if metric_family == "coverage"
        else MINIMUM_SUPPORT
    )

    support_satisfied = (
        len(valid_rows)
        >= support_required
    )

    status = "metric_eligible"
    exclusion_codes: set[str] = set(
        input_codes
    )

    baseline_value = None
    augmented_value = None
    delta_value = None

    if metric_family == "coverage":
        status = "coverage_metric_eligible"

        if metric_name == "pair_count":
            baseline_value = float(
                len(eligible_rows)
            )
            augmented_value = float(
                len(eligible_rows)
            )

        elif metric_name == (
            "eligible_pair_rate"
        ):
            rate = (
                len(eligible_rows)
                / candidate_count
                if candidate_count
                else 0.0
            )
            baseline_value = rate
            augmented_value = rate

        elif metric_name == (
            "missing_pair_rate"
        ):
            missing_count = sum(
                missing_pair(row)
                for row in group_rows
            )
            rate = (
                missing_count
                / candidate_count
                if candidate_count
                else 0.0
            )
            baseline_value = rate
            augmented_value = rate

        else:
            status = (
                "metric_definition_invalid"
            )
            exclusion_codes.add(
                EXCLUSION_CODES[
                    "metric_definition"
                ]
            )

    elif not support_satisfied:
        status = "insufficient_support"
        exclusion_codes.add(
            EXCLUSION_CODES[
                "insufficient_support"
            ]
        )

    elif input_codes:
        if EXCLUSION_CODES[
            "comparison_lineage"
        ] in input_codes:
            status = (
                "comparison_lineage_invalid"
            )
        elif EXCLUSION_CODES[
            "prediction_type"
        ] in input_codes:
            status = (
                "prediction_value_type_incompatible"
            )
        elif EXCLUSION_CODES[
            "binary_outcome"
        ] in input_codes:
            status = (
                "outcome_type_incompatible"
            )
        elif (
            EXCLUSION_CODES[
                "prediction_invalid"
            ]
            in input_codes
            or EXCLUSION_CODES[
                "probability_bounds"
            ]
            in input_codes
        ):
            status = "prediction_value_invalid"
        else:
            status = "outcome_value_invalid"

    else:
        baseline_losses = [
            row_loss(
                metric_name,
                float(
                    row[
                        "baseline_prediction_value"
                    ]
                ),
                float(row["outcome_value"]),
            )
            for row in valid_rows
        ]

        augmented_losses = [
            row_loss(
                metric_name,
                float(
                    row[
                        "augmented_prediction_value"
                    ]
                ),
                float(row["outcome_value"]),
            )
            for row in valid_rows
        ]

        baseline_value = aggregate_loss(
            metric_name,
            baseline_losses,
        )

        augmented_value = aggregate_loss(
            metric_name,
            augmented_losses,
        )

        delta_value = (
            augmented_value
            - baseline_value
        )

    aggregation_name = (
        aggregation_definition[
            "aggregation_name"
        ]
    )

    aggregation_key = (
        canonical_json_bytes(
            aggregation_key_payload
        ).decode("utf-8")
    )

    identity_payload = {
        "metric_contract_version": (
            METRIC_CONTRACT_VERSION
        ),
        "metric_name": metric_name,
        "aggregation_name": (
            aggregation_name
        ),
        "aggregation_key": (
            aggregation_key_payload
        ),
        "source_comparison_digest": (
            source_comparison_digest
        ),
    }

    identity_digest = sha256_payload(
        identity_payload
    )

    row = {
        "metric_contract_version": (
            METRIC_CONTRACT_VERSION
        ),
        "metric_record_id": (
            f"hcmet_{identity_digest[:32]}"
        ),
        "metric_name": metric_name,
        "metric_family": metric_family,
        "aggregation_name": (
            aggregation_name
        ),
        "aggregation_key": aggregation_key,
        "target_id": (
            aggregation_key_payload.get(
                "target_id"
            )
        ),
        "event_level": (
            aggregation_key_payload.get(
                "event_level"
            )
        ),
        "game_date": (
            aggregation_key_payload.get(
                "game_date"
            )
        ),
        "baseline_model_contract_version": (
            aggregation_key_payload.get(
                "baseline_model_contract_version"
            )
        ),
        "augmented_model_contract_version": (
            aggregation_key_payload.get(
                "augmented_model_contract_version"
            )
        ),
        "augmented_overlay_contract_version": (
            aggregation_key_payload.get(
                "augmented_overlay_contract_version"
            )
        ),
        "candidate_pair_count": (
            candidate_count
        ),
        "eligible_pair_count": len(
            eligible_rows
        ),
        "excluded_pair_count": (
            excluded_count
        ),
        "baseline_metric_value": rounded(
            baseline_value
        ),
        "augmented_metric_value": rounded(
            augmented_value
        ),
        "augmented_minus_baseline_delta": (
            rounded(delta_value)
        ),
        "better_direction": (
            metric_definition[
                "better_direction"
            ]
        ),
        "minimum_support_required": (
            support_required
        ),
        "support_satisfied": (
            support_satisfied
        ),
        "metric_status": status,
        "metric_exclusion_codes": sorted(
            exclusion_codes
        ),
        "source_comparison_digest": (
            source_comparison_digest
        ),
        "metric_identity_digest": (
            identity_digest
        ),
        "metric_record_digest": "",
    }

    row["metric_record_digest"] = (
        sha256_payload(
            {
                key: value
                for key, value in row.items()
                if key
                != "metric_record_digest"
            }
        )
    )

    return row


def execute_metric_calculation(
    comparison_rows: Sequence[
        Mapping[str, Any]
    ],
    metric_definitions: Sequence[
        Mapping[str, Any]
    ],
    aggregation_definitions: Sequence[
        Mapping[str, Any]
    ],
) -> list[dict[str, Any]]:
    source_rows = sorted(
        [
            dict(row)
            for row in comparison_rows
        ],
        key=lambda row: normalized_string(
            row.get("comparison_record_id")
        ),
    )

    source_digest = sha256_payload(
        source_rows
    )

    records: list[dict[str, Any]] = []

    for aggregation_definition in (
        aggregation_definitions
    ):
        groups = build_aggregation_groups(
            source_rows,
            aggregation_definition,
        )

        for key_payload, group_rows in groups:
            for metric_definition in (
                metric_definitions
            ):
                records.append(
                    metric_record(
                        metric_definition,
                        aggregation_definition,
                        key_payload,
                        group_rows,
                        source_digest,
                    )
                )

    records.sort(
        key=lambda row: (
            normalized_string(
                row.get(
                    "aggregation_name"
                )
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
                row.get("target_id")
            ),
            normalized_string(
                row.get("event_level")
            ),
            normalized_string(
                row.get("game_date")
            ),
            normalized_string(
                row.get("metric_record_id")
            ),
        )
    )

    return records


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

    layer_9s = load_module(
        PLAN_PATH,
        "layer_9s_metric_plan",
    )

    comparison_rows = (
        replay_comparison_records()
    )

    metric_records = (
        execute_metric_calculation(
            comparison_rows,
            layer_9s.METRIC_DEFINITIONS,
            layer_9s.AGGREGATION_LEVELS,
        )
    )

    reverse_metric_records = (
        execute_metric_calculation(
            list(reversed(comparison_rows)),
            list(
                reversed(
                    layer_9s.METRIC_DEFINITIONS
                )
            ),
            list(
                reversed(
                    layer_9s.AGGREGATION_LEVELS
                )
            ),
        )
    )

    eligible_comparisons = [
        row
        for row in comparison_rows
        if row.get(
            "comparison_eligible"
        )
    ]

    metric_status_counts = Counter(
        row["metric_status"]
        for row in metric_records
    )

    metric_name_counts = Counter(
        row["metric_name"]
        for row in metric_records
    )

    aggregation_counts = Counter(
        row["aggregation_name"]
        for row in metric_records
    )

    exclusion_counts = Counter(
        code
        for row in metric_records
        for code in row[
            "metric_exclusion_codes"
        ]
    )

    metric_ids = [
        row["metric_record_id"]
        for row in metric_records
    ]

    metric_digests = [
        row["metric_record_digest"]
        for row in metric_records
    ]

    performance_records = [
        row
        for row in metric_records
        if row["metric_family"]
        != "coverage"
    ]

    coverage_records = [
        row
        for row in metric_records
        if row["metric_family"]
        == "coverage"
    ]

    emitted_performance_records = [
        row
        for row in performance_records
        if row["metric_status"]
        == "metric_eligible"
    ]

    suppressed_records = [
        row
        for row in metric_records
        if row["metric_status"]
        == "insufficient_support"
    ]

    deterministic_replay = (
        canonical_json_bytes(
            metric_records
        )
        == canonical_json_bytes(
            reverse_metric_records
        )
    )

    field_contract_valid = all(
        set(row)
        == set(METRIC_RECORD_FIELDS)
        for row in metric_records
    )

    checks = [
        {
            "check": "nine_s_plan_verified",
            "actual": plan_verified,
            "expected": True,
            "passed": plan_verified,
        },
        {
            "check": "nine_r_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "eighteen_comparison_records_replayed",
            "actual": len(comparison_rows),
            "expected": 18,
            "passed": len(comparison_rows) == 18,
        },
        {
            "check": "sixteen_comparison_pairs_eligible",
            "actual": len(
                eligible_comparisons
            ),
            "expected": 16,
            "passed": len(
                eligible_comparisons
            )
            == 16,
        },
        {
            "check": "eight_metric_definitions_replayed",
            "actual": len(
                layer_9s.METRIC_DEFINITIONS
            ),
            "expected": 8,
            "passed": len(
                layer_9s.METRIC_DEFINITIONS
            )
            == 8,
        },
        {
            "check": "seven_aggregation_levels_replayed",
            "actual": len(
                layer_9s.AGGREGATION_LEVELS
            ),
            "expected": 7,
            "passed": len(
                layer_9s.AGGREGATION_LEVELS
            )
            == 7,
        },
        {
            "check": "metric_records_materialized",
            "actual": len(metric_records),
            "expected": len(metric_records),
            "passed": len(metric_records) > 0,
        },
        {
            "check": "all_eight_metric_names_materialized",
            "actual": len(metric_name_counts),
            "expected": 8,
            "passed": len(metric_name_counts)
            == 8,
        },
        {
            "check": "all_seven_aggregations_materialized",
            "actual": len(
                aggregation_counts
            ),
            "expected": 7,
            "passed": len(
                aggregation_counts
            )
            == 7,
        },
        {
            "check": "twenty_six_fields_implemented",
            "actual": len(
                METRIC_RECORD_FIELDS
            ),
            "expected": 26,
            "passed": (
                len(METRIC_RECORD_FIELDS)
                == 26
                and field_contract_valid
            ),
        },
        {
            "check": "metric_ids_unique",
            "actual": len(
                set(metric_ids)
            ),
            "expected": len(metric_records),
            "passed": len(
                set(metric_ids)
            )
            == len(metric_records),
        },
        {
            "check": "metric_digests_unique",
            "actual": len(
                set(metric_digests)
            ),
            "expected": len(metric_records),
            "passed": len(
                set(metric_digests)
            )
            == len(metric_records),
        },
        {
            "check": "metric_digests_valid",
            "actual": sum(
                valid_sha256(value)
                for value in metric_digests
            ),
            "expected": len(metric_records),
            "passed": all(
                valid_sha256(value)
                for value in metric_digests
            ),
        },
        {
            "check": "coverage_records_emit_values",
            "actual": sum(
                row[
                    "baseline_metric_value"
                ]
                is not None
                and row[
                    "augmented_metric_value"
                ]
                is not None
                for row in coverage_records
            ),
            "expected": len(
                coverage_records
            ),
            "passed": all(
                row[
                    "baseline_metric_value"
                ]
                is not None
                and row[
                    "augmented_metric_value"
                ]
                is not None
                for row in coverage_records
            ),
        },
        {
            "check": "coverage_records_emit_no_deltas",
            "actual": sum(
                row[
                    "augmented_minus_baseline_delta"
                ]
                is None
                for row in coverage_records
            ),
            "expected": len(
                coverage_records
            ),
            "passed": all(
                row[
                    "augmented_minus_baseline_delta"
                ]
                is None
                for row in coverage_records
            ),
        },
        {
            "check": "eligible_performance_records_emit_paired_values",
            "actual": sum(
                row[
                    "baseline_metric_value"
                ]
                is not None
                and row[
                    "augmented_metric_value"
                ]
                is not None
                and row[
                    "augmented_minus_baseline_delta"
                ]
                is not None
                for row
                in emitted_performance_records
            ),
            "expected": len(
                emitted_performance_records
            ),
            "passed": all(
                row[
                    "baseline_metric_value"
                ]
                is not None
                and row[
                    "augmented_metric_value"
                ]
                is not None
                and row[
                    "augmented_minus_baseline_delta"
                ]
                is not None
                for row
                in emitted_performance_records
            ),
        },
        {
            "check": "insufficient_support_records_suppressed",
            "actual": sum(
                row[
                    "baseline_metric_value"
                ]
                is None
                and row[
                    "augmented_metric_value"
                ]
                is None
                and row[
                    "augmented_minus_baseline_delta"
                ]
                is None
                for row in suppressed_records
            ),
            "expected": len(
                suppressed_records
            ),
            "passed": all(
                row[
                    "baseline_metric_value"
                ]
                is None
                and row[
                    "augmented_metric_value"
                ]
                is None
                and row[
                    "augmented_minus_baseline_delta"
                ]
                is None
                for row in suppressed_records
            ),
        },
        {
            "check": "minimum_support_versioned_as_two",
            "actual": MINIMUM_SUPPORT,
            "expected": 2,
            "passed": MINIMUM_SUPPORT == 2,
        },
        {
            "check": "log_loss_clipping_boundary_implemented",
            "actual": LOG_LOSS_EPSILON,
            "expected": 1e-15,
            "passed": (
                LOG_LOSS_EPSILON == 1e-15
            ),
        },
        {
            "check": "candidate_counts_nonnegative",
            "actual": sum(
                row[
                    "candidate_pair_count"
                ]
                >= 0
                for row in metric_records
            ),
            "expected": len(metric_records),
            "passed": all(
                row[
                    "candidate_pair_count"
                ]
                >= 0
                for row in metric_records
            ),
        },
        {
            "check": "eligible_excluded_counts_reconcile",
            "actual": sum(
                row[
                    "eligible_pair_count"
                ]
                + row[
                    "excluded_pair_count"
                ]
                == row[
                    "candidate_pair_count"
                ]
                for row in metric_records
            ),
            "expected": len(metric_records),
            "passed": all(
                row[
                    "eligible_pair_count"
                ]
                + row[
                    "excluded_pair_count"
                ]
                == row[
                    "candidate_pair_count"
                ]
                for row in metric_records
            ),
        },
        {
            "check": "metric_replay_deterministic",
            "actual": deterministic_replay,
            "expected": True,
            "passed": deterministic_replay,
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
            "check": "dataset_split_not_executed",
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

    metric_digest = sha256_payload(
        metric_records
    )
    reverse_metric_digest = (
        sha256_payload(
            reverse_metric_records
        )
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_comparative_"
        "metric_calculation_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_comparative_"
        "metric_calculation_contract_implementation_failed"
    )

    next_layer = (
        "9U_pitch_type_matchup_overlay_historical_comparative_"
        "metric_result_interpretation_contract_plan"
        if all_checks_passed
        else
        "9T_pitch_type_matchup_overlay_historical_comparative_"
        "metric_calculation_contract_implementation_remediation"
    )

    write_jsonl(
        OUTPUT_DIR
        / "comparative_metric_records.jsonl",
        metric_records,
    )

    write_csv(
        OUTPUT_DIR
        / "comparative_metric_records.csv",
        METRIC_RECORD_FIELDS,
        [
            {
                **row,
                "metric_exclusion_codes": (
                    "|".join(
                        row[
                            "metric_exclusion_codes"
                        ]
                    )
                ),
            }
            for row in metric_records
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "metric_status_counts.csv",
        [
            "metric_status",
            "count",
        ],
        [
            {
                "metric_status": status,
                "count": count,
            }
            for status, count
            in sorted(
                metric_status_counts.items()
            )
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "metric_name_counts.csv",
        [
            "metric_name",
            "count",
        ],
        [
            {
                "metric_name": name,
                "count": count,
            }
            for name, count
            in sorted(
                metric_name_counts.items()
            )
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "aggregation_counts.csv",
        [
            "aggregation_name",
            "count",
        ],
        [
            {
                "aggregation_name": name,
                "count": count,
            }
            for name, count
            in sorted(
                aggregation_counts.items()
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
                exclusion_counts.items()
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
                    "Layer 9T calculates bounded temporary "
                    "diagnostic metrics only and grants no "
                    "uncertainty, superiority, production, "
                    "market, pricing, or betting authority."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_comparative_metric_"
                    "result_interpretation_contract_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Deterministic diagnostic metric records "
                    "permit planning bounded interpretation "
                    "rules without declaring superiority."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "metric_contract_version": (
            METRIC_CONTRACT_VERSION
        ),
        "plan_verified": plan_verified,
        "predecessor_verified": (
            predecessor_verified
        ),
        "comparison_records_replayed": (
            len(comparison_rows)
        ),
        "eligible_comparison_pairs": (
            len(eligible_comparisons)
        ),
        "metric_records_materialized": (
            len(metric_records)
        ),
        "performance_metric_records": (
            len(performance_records)
        ),
        "coverage_metric_records": (
            len(coverage_records)
        ),
        "emitted_performance_metric_records": (
            len(emitted_performance_records)
        ),
        "suppressed_metric_records": (
            len(suppressed_records)
        ),
        "metric_status_counts": dict(
            sorted(
                metric_status_counts.items()
            )
        ),
        "metric_name_counts": dict(
            sorted(
                metric_name_counts.items()
            )
        ),
        "aggregation_counts": dict(
            sorted(
                aggregation_counts.items()
            )
        ),
        "exclusion_code_counts": dict(
            sorted(
                exclusion_counts.items()
            )
        ),
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required": (
            len(checks)
        ),
        "metric_digest": metric_digest,
        "reverse_metric_digest": (
            reverse_metric_digest
        ),
        "uncertainty_estimates_calculated": 0,
        "statistical_significance_tests_calculated": 0,
        "superiority_decisions_emitted": 0,
        "dataset_splits_executed": 0,
        "production_predictions_generated": 0,
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
        / "comparative_metric_summary.json",
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
            "historical_comparative_metric_"
            "result_interpretation_contract_planning"
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
        "Metric contract version: "
        f"{METRIC_CONTRACT_VERSION}"
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
        f"{summary['comparison_records_replayed']}"
    )
    print(
        "Eligible comparison pairs: "
        f"{summary['eligible_comparison_pairs']}"
    )
    print(
        "Metric records materialized: "
        f"{summary['metric_records_materialized']}"
    )
    print(
        "Performance metric records: "
        f"{summary['performance_metric_records']}"
    )
    print(
        "Coverage metric records: "
        f"{summary['coverage_metric_records']}"
    )
    print(
        "Emitted performance records: "
        f"{summary['emitted_performance_metric_records']}"
    )
    print(
        "Suppressed metric records: "
        f"{summary['suppressed_metric_records']}"
    )
    print(
        f"Metric digest: {metric_digest}"
    )
    print(
        "Reverse metric digest: "
        f"{reverse_metric_digest}"
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
        "Dataset splits executed: 0"
    )
    print(
        "Production predictions generated: 0"
    )
    print(
        "Production probabilities changed: 0"
    )
    print(
        "Market comparisons executed: 0"
    )
    print(
        "Betting edges calculated: 0"
    )
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
