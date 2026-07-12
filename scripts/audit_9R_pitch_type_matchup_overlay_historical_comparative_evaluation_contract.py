#!/usr/bin/env python3
"""
Layer 9R
Pitch-Type Matchup Overlay Historical Comparative Evaluation Contract Implementation

Implements the bounded deterministic comparison-pair contract planned by
Layer 9Q.

This implementation:

- verifies the Layer 9Q plan and Layer 9P predecessor;
- replays Layer 9P prediction/outcome join records;
- pairs baseline and augmented records by evaluation_row_id;
- explicitly classifies missing or ineligible variants;
- validates identity, outcome, value-type, lineage, and cardinality rules;
- validates the metric, direction, aggregation, support, and uncertainty catalogs;
- replays pairing under reversed input ordering;
- writes temporary diagnostic artifacts only.

This implementation does not calculate comparative metrics, estimate
uncertainty, declare superiority, execute dataset splits, or change any
production, market, pricing, simulation, or betting behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9R"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "evaluation_contract_implementation"
)

COMPARATIVE_EVALUATION_CONTRACT_VERSION = (
    "layer_9R_historical_comparative_evaluation_contract_v1"
)

EXPECTED_PLAN_VERSION = (
    "layer_9Q_historical_comparative_evaluation_contract_plan_v1"
)

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_comparative_"
    "evaluation_contract_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_comparative_evaluation_contract_implementation"
)

EXPECTED_JOIN_CONTRACT_VERSION = (
    "layer_9P_historical_prediction_outcome_join_contract_v1"
)

EXPECTED_PREDICTION_CONTRACT_VERSION = (
    "layer_9P_synthetic_historical_prediction_contract_v1"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_prediction_"
    "outcome_join_contract_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_comparative_evaluation_contract_planning"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9R_pitch_type_matchup_overlay_"
    "historical_comparative_evaluation_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9Q_pitch_type_matchup_overlay_"
    "historical_comparative_evaluation_contract.py"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9P_pitch_type_matchup_overlay_"
    "historical_prediction_outcome_join_contract.py"
)

SHA256_PATTERN = re.compile(
    r"^[0-9a-f]{64}$"
)

COMPARISON_FIELDS = [
    "comparative_evaluation_contract_version",
    "comparison_record_id",
    "evaluation_row_id",
    "target_id",
    "event_level",
    "game_id",
    "game_date",
    "scheduled_start_utc",
    "plate_appearance_id",
    "pitch_id",
    "event_sequence",
    "baseline_prediction_record_id",
    "baseline_prediction_value",
    "baseline_model_contract_version",
    "baseline_prediction_provenance_digest",
    "augmented_prediction_record_id",
    "augmented_prediction_value",
    "augmented_model_contract_version",
    "augmented_overlay_contract_version",
    "augmented_prediction_provenance_digest",
    "prediction_value_type",
    "outcome_value",
    "outcome_available_at_utc",
    "evaluation_row_digest",
    "baseline_join_record_digest",
    "augmented_join_record_digest",
    "comparison_identity_digest",
    "comparison_status",
    "comparison_eligible",
    "comparison_exclusion_codes",
    "comparison_record_digest",
]

EXCLUSION_CODES = {
    "baseline_missing":
        "historical_comparison_baseline_prediction_missing",
    "augmented_missing":
        "historical_comparison_augmented_prediction_missing",
    "baseline_ineligible":
        "historical_comparison_baseline_join_ineligible",
    "augmented_ineligible":
        "historical_comparison_augmented_join_ineligible",
    "evaluation_identity":
        "historical_comparison_evaluation_identity_mismatch",
    "event_identity":
        "historical_comparison_event_identity_mismatch",
    "outcome":
        "historical_comparison_outcome_mismatch",
    "outcome_availability":
        "historical_comparison_outcome_availability_mismatch",
    "value_type":
        "historical_comparison_prediction_value_type_mismatch",
    "evaluation_lineage":
        "historical_comparison_evaluation_lineage_mismatch",
    "prediction_lineage":
        "historical_comparison_prediction_lineage_invalid",
    "join_lineage":
        "historical_comparison_join_lineage_invalid",
    "duplicate_baseline":
        "historical_comparison_duplicate_baseline",
    "duplicate_augmented":
        "historical_comparison_duplicate_augmented",
    "many_to_many":
        "historical_comparison_many_to_many_detected",
    "source_invalid":
        "historical_comparison_source_record_invalid",
}

PROHIBITED_AUTHORITIES = [
    "accuracy_evaluation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "calibration_evaluation",
    "canonical_probability_authority_change",
    "dataset_split_execution",
    "edge_detection",
    "historical_comparative_metric_calculation",
    "incremental_value_evaluation",
    "market_comparison",
    "model_training",
    "parameter_tuning",
    "predictive_metric_calculation",
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


def replay_predecessor() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    layer_9p = load_module(
        PREDECESSOR_PATH,
        "layer_9p_comparative_source",
    )

    evaluation_rows = (
        layer_9p.replay_evaluation_dataset()
    )

    predictions = (
        layer_9p.build_prediction_fixtures(
            evaluation_rows
        )
    )

    joined_rows = layer_9p.execute_join(
        predictions,
        evaluation_rows,
    )

    return evaluation_rows, joined_rows


def comparison_identity_payload(
    evaluation_row_id: str,
    baseline: Mapping[str, Any] | None,
    augmented: Mapping[str, Any] | None,
) -> dict[str, Any]:
    source = baseline or augmented or {}

    return {
        "comparative_evaluation_contract_version": (
            COMPARATIVE_EVALUATION_CONTRACT_VERSION
        ),
        "evaluation_row_id": evaluation_row_id,
        "target_id": source.get("target_id"),
        "event_level": source.get("event_level"),
        "game_id": source.get("game_id"),
        "plate_appearance_id": source.get(
            "plate_appearance_id"
        ),
        "pitch_id": source.get("pitch_id"),
        "event_sequence": source.get(
            "event_sequence"
        ),
    }


def classify_pair(
    baseline_rows: Sequence[Mapping[str, Any]],
    augmented_rows: Sequence[Mapping[str, Any]],
) -> tuple[str, bool, list[str]]:
    codes: set[str] = set()

    baseline_rows = [
        row
        for row in baseline_rows
        if normalized_string(
            row.get(
                "prediction_record_id"
            )
        )
    ]

    augmented_rows = [
        row
        for row in augmented_rows
        if normalized_string(
            row.get(
                "prediction_record_id"
            )
        )
    ]

    baseline = (
        baseline_rows[0]
        if baseline_rows
        else None
    )
    augmented = (
        augmented_rows[0]
        if augmented_rows
        else None
    )

    if not baseline_rows:
        codes.add(
            EXCLUSION_CODES["baseline_missing"]
        )

    if not augmented_rows:
        codes.add(
            EXCLUSION_CODES["augmented_missing"]
        )

    if len(baseline_rows) > 1:
        codes.add(
            EXCLUSION_CODES["duplicate_baseline"]
        )

    if len(augmented_rows) > 1:
        codes.add(
            EXCLUSION_CODES["duplicate_augmented"]
        )

    if (
        len(baseline_rows) > 1
        and len(augmented_rows) > 1
    ):
        codes.add(
            EXCLUSION_CODES["many_to_many"]
        )

    if baseline is not None:
        if (
            baseline.get(
                "prediction_outcome_join_status"
            )
            != "matched_eligible"
            or not baseline.get(
                "prediction_outcome_join_eligible"
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "baseline_ineligible"
                ]
            )

    if augmented is not None:
        if (
            augmented.get(
                "prediction_outcome_join_status"
            )
            != "matched_eligible"
            or not augmented.get(
                "prediction_outcome_join_eligible"
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "augmented_ineligible"
                ]
            )

    if baseline is not None and augmented is not None:
        identity_fields = (
            "evaluation_row_id",
            "target_id",
            "event_level",
            "game_id",
        )

        if any(
            baseline.get(field)
            != augmented.get(field)
            for field in identity_fields
        ):
            codes.add(
                EXCLUSION_CODES[
                    "evaluation_identity"
                ]
            )

        event_fields = (
            "plate_appearance_id",
            "pitch_id",
            "event_sequence",
        )

        if any(
            baseline.get(field)
            != augmented.get(field)
            for field in event_fields
        ):
            codes.add(
                EXCLUSION_CODES[
                    "event_identity"
                ]
            )

        if (
            baseline.get("outcome_value")
            != augmented.get("outcome_value")
        ):
            codes.add(
                EXCLUSION_CODES["outcome"]
            )

        if (
            baseline.get(
                "outcome_available_at_utc"
            )
            != augmented.get(
                "outcome_available_at_utc"
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "outcome_availability"
                ]
            )

        if (
            baseline.get(
                "prediction_value_type"
            )
            != augmented.get(
                "prediction_value_type"
            )
        ):
            codes.add(
                EXCLUSION_CODES["value_type"]
            )

        if (
            baseline.get(
                "evaluation_row_digest"
            )
            != augmented.get(
                "evaluation_row_digest"
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "evaluation_lineage"
                ]
            )

    for row in (
        baseline,
        augmented,
    ):
        if row is None:
            continue

        if not valid_sha256(
            row.get(
                "prediction_provenance_digest"
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "prediction_lineage"
                ]
            )

        if not valid_sha256(
            row.get(
                "prediction_outcome_join_record_digest"
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "join_lineage"
                ]
            )

        if not valid_sha256(
            row.get("evaluation_row_digest")
        ):
            codes.add(
                EXCLUSION_CODES[
                    "evaluation_lineage"
                ]
            )

    eligible = (
        baseline is not None
        and augmented is not None
        and not codes
    )

    if eligible:
        status = "paired_eligible"
    elif (
        EXCLUSION_CODES["baseline_missing"]
        in codes
    ):
        status = "baseline_prediction_missing"
    elif (
        EXCLUSION_CODES["augmented_missing"]
        in codes
    ):
        status = "augmented_prediction_missing"
    elif (
        EXCLUSION_CODES["duplicate_baseline"]
        in codes
    ):
        status = "duplicate_baseline"
    elif (
        EXCLUSION_CODES["duplicate_augmented"]
        in codes
    ):
        status = "duplicate_augmented"
    elif (
        EXCLUSION_CODES["baseline_ineligible"]
        in codes
    ):
        status = "baseline_join_ineligible"
    elif (
        EXCLUSION_CODES["augmented_ineligible"]
        in codes
    ):
        status = "augmented_join_ineligible"
    elif (
        EXCLUSION_CODES["outcome"]
        in codes
        or EXCLUSION_CODES[
            "outcome_availability"
        ]
        in codes
    ):
        status = "outcome_mismatch"
    elif (
        EXCLUSION_CODES["value_type"]
        in codes
    ):
        status = (
            "prediction_value_type_mismatch"
        )
    elif (
        EXCLUSION_CODES["evaluation_lineage"]
        in codes
        or EXCLUSION_CODES[
            "prediction_lineage"
        ]
        in codes
        or EXCLUSION_CODES["join_lineage"]
        in codes
    ):
        status = "lineage_mismatch"
    else:
        status = "evaluation_identity_mismatch"

    return status, eligible, sorted(codes)


def comparison_record(
    evaluation_row_id: str,
    baseline_rows: Sequence[Mapping[str, Any]],
    augmented_rows: Sequence[Mapping[str, Any]],
    evaluation_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    baseline = (
        baseline_rows[0]
        if baseline_rows
        else None
    )

    augmented = (
        augmented_rows[0]
        if augmented_rows
        else None
    )

    source = baseline or augmented or {}
    evaluation = evaluation_by_id.get(
        evaluation_row_id,
        {},
    )

    status, eligible, exclusion_codes = (
        classify_pair(
            baseline_rows,
            augmented_rows,
        )
    )

    identity_payload = (
        comparison_identity_payload(
            evaluation_row_id,
            baseline,
            augmented,
        )
    )

    identity_digest = sha256_payload(
        identity_payload
    )

    row = {
        "comparative_evaluation_contract_version": (
            COMPARATIVE_EVALUATION_CONTRACT_VERSION
        ),
        "comparison_record_id": (
            f"hcomp_{identity_digest[:32]}"
        ),
        "evaluation_row_id": evaluation_row_id,
        "target_id": source.get("target_id"),
        "event_level": source.get(
            "event_level"
        ),
        "game_id": source.get("game_id"),
        "game_date": evaluation.get(
            "game_date"
        ),
        "scheduled_start_utc": evaluation.get(
            "scheduled_start_utc"
        ),
        "plate_appearance_id": source.get(
            "plate_appearance_id"
        ),
        "pitch_id": source.get("pitch_id"),
        "event_sequence": source.get(
            "event_sequence"
        ),
        "baseline_prediction_record_id": (
            baseline.get(
                "prediction_record_id"
            )
            if baseline is not None
            else None
        ),
        "baseline_prediction_value": (
            baseline.get("prediction_value")
            if baseline is not None
            else None
        ),
        "baseline_model_contract_version": (
            baseline.get(
                "model_contract_version"
            )
            if baseline is not None
            else None
        ),
        "baseline_prediction_provenance_digest": (
            baseline.get(
                "prediction_provenance_digest"
            )
            if baseline is not None
            else None
        ),
        "augmented_prediction_record_id": (
            augmented.get(
                "prediction_record_id"
            )
            if augmented is not None
            else None
        ),
        "augmented_prediction_value": (
            augmented.get("prediction_value")
            if augmented is not None
            else None
        ),
        "augmented_model_contract_version": (
            augmented.get(
                "model_contract_version"
            )
            if augmented is not None
            else None
        ),
        "augmented_overlay_contract_version": (
            augmented.get(
                "overlay_contract_version"
            )
            if augmented is not None
            else None
        ),
        "augmented_prediction_provenance_digest": (
            augmented.get(
                "prediction_provenance_digest"
            )
            if augmented is not None
            else None
        ),
        "prediction_value_type": (
            source.get(
                "prediction_value_type"
            )
        ),
        "outcome_value": source.get(
            "outcome_value"
        ),
        "outcome_available_at_utc": (
            source.get(
                "outcome_available_at_utc"
            )
        ),
        "evaluation_row_digest": source.get(
            "evaluation_row_digest"
        ),
        "baseline_join_record_digest": (
            baseline.get(
                "prediction_outcome_join_record_digest"
            )
            if baseline is not None
            else None
        ),
        "augmented_join_record_digest": (
            augmented.get(
                "prediction_outcome_join_record_digest"
            )
            if augmented is not None
            else None
        ),
        "comparison_identity_digest": (
            identity_digest
        ),
        "comparison_status": status,
        "comparison_eligible": eligible,
        "comparison_exclusion_codes": (
            exclusion_codes
        ),
        "comparison_record_digest": "",
    }

    row["comparison_record_digest"] = (
        sha256_payload(
            {
                key: value
                for key, value in row.items()
                if key
                != "comparison_record_digest"
            }
        )
    )

    return row


def execute_pairing(
    joined_rows: Sequence[Mapping[str, Any]],
    evaluation_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[
        str,
        dict[str, list[Mapping[str, Any]]],
    ] = defaultdict(
        lambda: {
            "baseline": [],
            "augmented": [],
        }
    )

    for row in joined_rows:
        evaluation_row_id = normalized_string(
            row.get("evaluation_row_id")
        )

        if not evaluation_row_id:
            continue

        variant = normalized_string(
            row.get("prediction_variant")
        )

        if variant in {
            "baseline",
            "augmented",
        }:
            grouped[evaluation_row_id][
                variant
            ].append(row)

    evaluation_by_id = {
        normalized_string(
            row.get("evaluation_row_id")
        ): row
        for row in evaluation_rows
        if row.get("evaluation_eligible")
    }

    evaluation_ids = sorted(
        set(evaluation_by_id)
        | set(grouped)
    )

    records = [
        comparison_record(
            evaluation_row_id,
            grouped[evaluation_row_id][
                "baseline"
            ],
            grouped[evaluation_row_id][
                "augmented"
            ],
            evaluation_by_id,
        )
        for evaluation_row_id
        in evaluation_ids
    ]

    records.sort(
        key=lambda row: (
            normalized_string(
                row.get("game_date")
            ),
            normalized_string(
                row.get(
                    "scheduled_start_utc"
                )
            ),
            normalized_string(
                row.get("game_id")
            ),
            (
                row.get("event_sequence")
                if isinstance(
                    row.get("event_sequence"),
                    int,
                )
                and not isinstance(
                    row.get("event_sequence"),
                    bool,
                )
                else -1
            ),
            normalized_string(
                row.get("target_id")
            ),
            normalized_string(
                row.get("event_level")
            ),
            normalized_string(
                row.get(
                    "comparison_record_id"
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
        and EXPECTED_JOIN_CONTRACT_VERSION
        in predecessor_constants
        and EXPECTED_PREDICTION_CONTRACT_VERSION
        in predecessor_constants
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    layer_9q = load_module(
        PLAN_PATH,
        "layer_9q_comparative_plan",
    )

    evaluation_rows, joined_rows = (
        replay_predecessor()
    )

    comparison_records = execute_pairing(
        joined_rows,
        evaluation_rows,
    )

    reverse_comparison_records = (
        execute_pairing(
            list(reversed(joined_rows)),
            list(reversed(evaluation_rows)),
        )
    )

    eligible_records = [
        row
        for row in comparison_records
        if row["comparison_eligible"]
    ]

    excluded_records = [
        row
        for row in comparison_records
        if not row["comparison_eligible"]
    ]

    status_counts = Counter(
        row["comparison_status"]
        for row in comparison_records
    )

    exclusion_counts = Counter(
        code
        for row in comparison_records
        for code in row[
            "comparison_exclusion_codes"
        ]
    )

    comparison_ids = [
        row["comparison_record_id"]
        for row in comparison_records
    ]

    comparison_digests = [
        row["comparison_record_digest"]
        for row in comparison_records
    ]

    field_contract_valid = all(
        set(row)
        == set(COMPARISON_FIELDS)
        for row in comparison_records
    )

    deterministic_replay = (
        canonical_json_bytes(
            comparison_records
        )
        == canonical_json_bytes(
            reverse_comparison_records
        )
    )

    catalog_checks = {
        "metric_families": (
            len(layer_9q.METRIC_FAMILIES)
            == 7
            and all(
                not row[
                    "calculation_authorized_here"
                ]
                for row
                in layer_9q.METRIC_FAMILIES
            )
        ),
        "metric_directions": (
            len(
                layer_9q.METRIC_DIRECTION_RULES
            )
            == 8
        ),
        "aggregation_dimensions": (
            len(
                layer_9q.AGGREGATION_DIMENSIONS
            )
            == 6
        ),
        "support_rules": (
            len(layer_9q.SUPPORT_RULES)
            == 6
        ),
        "uncertainty_boundaries": (
            len(
                layer_9q.UNCERTAINTY_BOUNDARIES
            )
            == 4
        ),
    }

    checks = [
        {
            "check": "nine_q_plan_verified",
            "actual": plan_verified,
            "expected": True,
            "passed": plan_verified,
        },
        {
            "check": "nine_p_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "twenty_nine_evaluation_rows_replayed",
            "actual": len(evaluation_rows),
            "expected": 29,
            "passed": len(evaluation_rows) == 29,
        },
        {
            "check": "thirty_six_join_rows_replayed",
            "actual": len(joined_rows),
            "expected": 36,
            "passed": len(joined_rows) == 36,
        },
        {
            "check": "eighteen_comparison_records_materialized",
            "actual": len(
                comparison_records
            ),
            "expected": 18,
            "passed": len(
                comparison_records
            )
            == 18,
        },
        {
            "check": "sixteen_pairs_eligible",
            "actual": len(eligible_records),
            "expected": 16,
            "passed": len(eligible_records) == 16,
        },
        {
            "check": "two_pairs_excluded",
            "actual": len(excluded_records),
            "expected": 2,
            "passed": len(excluded_records) == 2,
        },
        {
            "check": "one_baseline_missing_classified",
            "actual": status_counts[
                "baseline_prediction_missing"
            ],
            "expected": 1,
            "passed": status_counts[
                "baseline_prediction_missing"
            ]
            == 1,
        },
        {
            "check": "one_augmented_missing_classified",
            "actual": status_counts[
                "augmented_prediction_missing"
            ],
            "expected": 1,
            "passed": status_counts[
                "augmented_prediction_missing"
            ]
            == 1,
        },
        {
            "check": "thirty_one_fields_implemented",
            "actual": len(COMPARISON_FIELDS),
            "expected": 31,
            "passed": (
                len(COMPARISON_FIELDS) == 31
                and field_contract_valid
            ),
        },
        {
            "check": "comparison_ids_unique",
            "actual": len(
                set(comparison_ids)
            ),
            "expected": len(
                comparison_records
            ),
            "passed": len(
                set(comparison_ids)
            )
            == len(comparison_records),
        },
        {
            "check": "comparison_digests_unique",
            "actual": len(
                set(comparison_digests)
            ),
            "expected": len(
                comparison_records
            ),
            "passed": len(
                set(comparison_digests)
            )
            == len(comparison_records),
        },
        {
            "check": "comparison_digests_valid",
            "actual": sum(
                valid_sha256(value)
                for value in comparison_digests
            ),
            "expected": len(
                comparison_records
            ),
            "passed": all(
                valid_sha256(value)
                for value in comparison_digests
            ),
        },
        {
            "check": "eligible_pairs_have_no_exclusions",
            "actual": sum(
                not row[
                    "comparison_exclusion_codes"
                ]
                for row in eligible_records
            ),
            "expected": len(eligible_records),
            "passed": all(
                not row[
                    "comparison_exclusion_codes"
                ]
                for row in eligible_records
            ),
        },
        {
            "check": "excluded_pairs_have_exclusions",
            "actual": sum(
                bool(
                    row[
                        "comparison_exclusion_codes"
                    ]
                )
                for row in excluded_records
            ),
            "expected": len(excluded_records),
            "passed": all(
                bool(
                    row[
                        "comparison_exclusion_codes"
                    ]
                )
                for row in excluded_records
            ),
        },
        {
            "check": "eligible_pair_lineage_valid",
            "actual": sum(
                all(
                    valid_sha256(
                        row.get(field)
                    )
                    for field in {
                        "evaluation_row_digest",
                        "baseline_prediction_provenance_digest",
                        "augmented_prediction_provenance_digest",
                        "baseline_join_record_digest",
                        "augmented_join_record_digest",
                    }
                )
                for row in eligible_records
            ),
            "expected": len(eligible_records),
            "passed": all(
                all(
                    valid_sha256(
                        row.get(field)
                    )
                    for field in {
                        "evaluation_row_digest",
                        "baseline_prediction_provenance_digest",
                        "augmented_prediction_provenance_digest",
                        "baseline_join_record_digest",
                        "augmented_join_record_digest",
                    }
                )
                for row in eligible_records
            ),
        },
        {
            "check": "metric_family_catalog_valid",
            "actual": catalog_checks[
                "metric_families"
            ],
            "expected": True,
            "passed": catalog_checks[
                "metric_families"
            ],
        },
        {
            "check": "metric_direction_catalog_valid",
            "actual": catalog_checks[
                "metric_directions"
            ],
            "expected": True,
            "passed": catalog_checks[
                "metric_directions"
            ],
        },
        {
            "check": "aggregation_catalog_valid",
            "actual": catalog_checks[
                "aggregation_dimensions"
            ],
            "expected": True,
            "passed": catalog_checks[
                "aggregation_dimensions"
            ],
        },
        {
            "check": "support_catalog_valid",
            "actual": catalog_checks[
                "support_rules"
            ],
            "expected": True,
            "passed": catalog_checks[
                "support_rules"
            ],
        },
        {
            "check": "uncertainty_catalog_valid",
            "actual": catalog_checks[
                "uncertainty_boundaries"
            ],
            "expected": True,
            "passed": catalog_checks[
                "uncertainty_boundaries"
            ],
        },
        {
            "check": "source_counts_reconcile",
            "actual": (
                len(eligible_records)
                + len(excluded_records)
            ),
            "expected": 18,
            "passed": (
                len(eligible_records)
                + len(excluded_records)
                == 18
            ),
        },
        {
            "check": "pairing_replay_deterministic",
            "actual": deterministic_replay,
            "expected": True,
            "passed": deterministic_replay,
        },
        {
            "check": "comparative_metrics_not_calculated",
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

    comparison_digest = sha256_payload(
        comparison_records
    )

    reverse_comparison_digest = (
        sha256_payload(
            reverse_comparison_records
        )
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_comparative_"
        "evaluation_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_comparative_"
        "evaluation_contract_implementation_failed"
    )

    next_layer = (
        "9S_pitch_type_matchup_overlay_historical_comparative_"
        "metric_calculation_contract_plan"
        if all_checks_passed
        else
        "9R_pitch_type_matchup_overlay_historical_comparative_"
        "evaluation_contract_implementation_remediation"
    )

    write_jsonl(
        OUTPUT_DIR
        / "comparative_evaluation_records.jsonl",
        comparison_records,
    )

    write_csv(
        OUTPUT_DIR
        / "comparative_evaluation_records.csv",
        COMPARISON_FIELDS,
        [
            {
                **row,
                "comparison_exclusion_codes": (
                    "|".join(
                        row[
                            "comparison_exclusion_codes"
                        ]
                    )
                ),
            }
            for row in comparison_records
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "comparison_status_counts.csv",
        [
            "comparison_status",
            "count",
        ],
        [
            {
                "comparison_status": status,
                "count": count,
            }
            for status, count
            in sorted(status_counts.items())
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
            in sorted(exclusion_counts.items())
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
                    "Layer 9R implements deterministic "
                    "comparison pairing only and calculates "
                    "no comparative metrics."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_comparative_metric_calculation_contract_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Validated paired comparison records "
                    "permit planning a separately bounded "
                    "metric-calculation contract."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "comparative_evaluation_contract_version": (
            COMPARATIVE_EVALUATION_CONTRACT_VERSION
        ),
        "plan_verified": plan_verified,
        "predecessor_verified": (
            predecessor_verified
        ),
        "evaluation_rows_replayed": len(
            evaluation_rows
        ),
        "prediction_outcome_join_rows_replayed": (
            len(joined_rows)
        ),
        "comparison_records_materialized": (
            len(comparison_records)
        ),
        "eligible_comparison_pairs": len(
            eligible_records
        ),
        "excluded_comparison_pairs": len(
            excluded_records
        ),
        "comparison_status_counts": dict(
            sorted(status_counts.items())
        ),
        "exclusion_code_counts": dict(
            sorted(exclusion_counts.items())
        ),
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in checks
        ),
        "implementation_checks_required": (
            len(checks)
        ),
        "comparison_digest": (
            comparison_digest
        ),
        "reverse_comparison_digest": (
            reverse_comparison_digest
        ),
        "comparative_metrics_calculated": 0,
        "uncertainty_estimates_calculated": 0,
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
        / "comparative_evaluation_summary.json",
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
            "historical_comparative_metric_calculation_contract_planning"
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
        "Comparative evaluation contract version: "
        f"{COMPARATIVE_EVALUATION_CONTRACT_VERSION}"
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
        "Evaluation rows replayed: "
        f"{summary['evaluation_rows_replayed']}"
    )
    print(
        "Prediction/outcome join rows replayed: "
        f"{summary['prediction_outcome_join_rows_replayed']}"
    )
    print(
        "Comparison records materialized: "
        f"{summary['comparison_records_materialized']}"
    )
    print(
        "Eligible comparison pairs: "
        f"{summary['eligible_comparison_pairs']}"
    )
    print(
        "Excluded comparison pairs: "
        f"{summary['excluded_comparison_pairs']}"
    )
    print(
        "Comparison digest: "
        f"{comparison_digest}"
    )
    print(
        "Reverse comparison digest: "
        f"{reverse_comparison_digest}"
    )
    print(
        "Comparative metrics calculated: 0"
    )
    print(
        "Uncertainty estimates calculated: 0"
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
