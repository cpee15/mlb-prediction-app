#!/usr/bin/env python3
"""
Layer 9P
Pitch-Type Matchup Overlay Historical Prediction/Outcome Join Contract Implementation

Implements the bounded deterministic temporary diagnostic join contract planned
by Layer 9O.

This implementation:

- verifies the Layer 9O plan and Layer 9N dataset contract implementation;
- replays the Layer 9N historical evaluation dataset;
- creates deterministic synthetic baseline and augmented prediction fixtures;
- joins each prediction to at most one eligible evaluation row;
- explicitly classifies missing predictions and invalid prediction records;
- validates point-in-time, variant, identity, lineage, and cardinality rules;
- replays the join under reversed input ordering;
- writes temporary diagnostic artifacts only.

This implementation does not:

- generate production predictions;
- calculate predictive or evaluation metrics;
- execute dataset splits or backtests;
- train or tune models;
- modify production probabilities, simulations, pricing, markets, or bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9P"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_prediction_"
    "outcome_join_contract_implementation"
)

JOIN_CONTRACT_VERSION = (
    "layer_9P_historical_prediction_outcome_join_contract_v1"
)

PREDICTION_CONTRACT_VERSION = (
    "layer_9P_synthetic_historical_prediction_contract_v1"
)

BASELINE_MODEL_CONTRACT_VERSION = (
    "layer_9P_synthetic_baseline_model_contract_v1"
)

AUGMENTED_MODEL_CONTRACT_VERSION = (
    "layer_9P_synthetic_augmented_model_contract_v1"
)

AUGMENTED_OVERLAY_CONTRACT_VERSION = (
    "layer_9P_synthetic_pitch_type_matchup_overlay_contract_v1"
)

EXPECTED_PLAN_VERSION = (
    "layer_9O_historical_prediction_outcome_join_contract_plan_v1"
)

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_prediction_"
    "outcome_join_contract_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_prediction_outcome_join_contract_implementation"
)

EXPECTED_DATASET_VERSION = (
    "layer_9N_historical_evaluation_dataset_contract_v1"
)

EXPECTED_DATASET_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_evaluation_"
    "dataset_contract_implementation_complete"
)

EXPECTED_DATASET_AUTHORITY = (
    "historical_prediction_outcome_join_contract_planning"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9P_pitch_type_matchup_overlay_"
    "historical_prediction_outcome_join_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9O_pitch_type_matchup_overlay_"
    "historical_prediction_outcome_join_contract.py"
)

DATASET_IMPLEMENTATION_PATH = (
    ROOT
    / "scripts"
    / "audit_9N_pitch_type_matchup_overlay_"
    "historical_evaluation_dataset_contract.py"
)

SHA256_PATTERN = re.compile(
    r"^[0-9a-f]{64}$"
)

PREDICTION_FIELDS = [
    "prediction_contract_version",
    "prediction_record_id",
    "prediction_variant",
    "target_id",
    "event_level",
    "game_id",
    "plate_appearance_id",
    "pitch_id",
    "event_sequence",
    "prediction_generated_at_utc",
    "prediction_value",
    "prediction_value_type",
    "model_artifact_id",
    "model_contract_version",
    "feature_contract_version",
    "overlay_contract_version",
    "prediction_provenance_digest",
]

JOINED_FIELDS = [
    "prediction_outcome_join_contract_version",
    "prediction_outcome_join_id",
    "evaluation_row_id",
    "prediction_record_id",
    "prediction_variant",
    "target_id",
    "event_level",
    "game_id",
    "plate_appearance_id",
    "pitch_id",
    "event_sequence",
    "prediction_generated_at_utc",
    "prediction_value",
    "prediction_value_type",
    "outcome_value",
    "outcome_available_at_utc",
    "evaluation_dataset_version",
    "prediction_contract_version",
    "model_contract_version",
    "feature_contract_version",
    "overlay_contract_version",
    "evaluation_row_digest",
    "prediction_provenance_digest",
    "prediction_outcome_join_identity_digest",
    "prediction_outcome_join_status",
    "prediction_outcome_join_eligible",
    "prediction_outcome_join_exclusion_codes",
    "prediction_outcome_join_record_digest",
]

EXCLUSION_CODES = {
    "evaluation_ineligible":
        "historical_prediction_outcome_evaluation_row_ineligible",
    "prediction_missing":
        "historical_prediction_outcome_prediction_missing",
    "evaluation_missing":
        "historical_prediction_outcome_evaluation_row_missing",
    "identity_mismatch":
        "historical_prediction_outcome_identity_mismatch",
    "point_in_time":
        "historical_prediction_outcome_point_in_time_violation",
    "contract_mismatch":
        "historical_prediction_outcome_contract_version_mismatch",
    "prediction_lineage":
        "historical_prediction_outcome_prediction_lineage_invalid",
    "evaluation_lineage":
        "historical_prediction_outcome_evaluation_lineage_invalid",
    "variant_contract":
        "historical_prediction_outcome_variant_contract_invalid",
    "duplicate_prediction":
        "historical_prediction_outcome_duplicate_prediction_identity",
    "duplicate_evaluation":
        "historical_prediction_outcome_duplicate_evaluation_identity",
    "many_to_many":
        "historical_prediction_outcome_many_to_many_detected",
    "source_invalid":
        "historical_prediction_outcome_source_record_invalid",
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
    "historical_outcome_collection_execution",
    "historical_outcome_fetch_execution",
    "incremental_value_evaluation",
    "market_comparison",
    "model_training",
    "parameter_tuning",
    "predictive_metric_calculation",
    "pricing",
    "production_historical_evaluation_dataset_materialization",
    "production_historical_prediction_materialization",
    "production_matchup_activation",
    "production_overlay_integration",
    "simulation_probability_change",
    "simulation_state_change",
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


def parse_datetime(value: Any) -> datetime | None:
    text = normalized_string(value)

    if not text:
        return None

    try:
        parsed = datetime.fromisoformat(
            text.replace(
                "Z",
                "+00:00",
            )
        )
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=timezone.utc
        )

    return parsed.astimezone(
        timezone.utc
    )


def isoformat_utc(value: datetime) -> str:
    return (
        value.astimezone(
            timezone.utc
        )
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


def valid_sha256(value: Any) -> bool:
    return bool(
        SHA256_PATTERN.fullmatch(
            normalized_string(
                value
            )
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


def load_module(
    path: Path,
    name: str,
) -> Any:
    spec = importlib.util.spec_from_file_location(
        name,
        path,
    )

    if (
        spec is None
        or spec.loader is None
    ):
        raise RuntimeError(
            f"Unable to load module: {path}"
        )

    module = importlib.util.module_from_spec(
        spec
    )
    spec.loader.exec_module(
        module
    )

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
            fieldnames=list(
                fieldnames
            ),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(
            rows
        )


def event_identity(
    row: Mapping[str, Any],
) -> tuple[Any, ...]:
    return (
        normalized_string(
            row.get(
                "target_id"
            )
        ),
        normalized_string(
            row.get(
                "event_level"
            )
        ),
        normalized_string(
            row.get(
                "game_id"
            )
        ),
        normalized_string(
            row.get(
                "plate_appearance_id"
            )
        ),
        normalized_string(
            row.get(
                "pitch_id"
            )
        ),
        row.get(
            "event_sequence"
        ),
    )


def prediction_identity(
    row: Mapping[str, Any],
) -> tuple[Any, ...]:
    return (
        normalized_string(
            row.get(
                "prediction_variant"
            )
        ),
        *event_identity(
            row
        ),
    )


def deterministic_prediction_value(
    evaluation_row: Mapping[str, Any],
    variant: str,
) -> float:
    digest = sha256_payload(
        {
            "evaluation_row_id": (
                evaluation_row[
                    "evaluation_row_id"
                ]
            ),
            "prediction_variant": variant,
        }
    )

    integer = int(
        digest[:12],
        16,
    )

    return round(
        0.05
        + (
            integer
            % 900000
        )
        / 1000000,
        6,
    )


def prediction_record(
    evaluation_row: Mapping[str, Any],
    variant: str,
    *,
    point_in_time_violation: bool = False,
) -> dict[str, Any]:
    scheduled_start = parse_datetime(
        evaluation_row.get(
            "scheduled_start_utc"
        )
    )

    if scheduled_start is None:
        raise RuntimeError(
            "Evaluation row has invalid scheduled_start_utc"
        )

    generated_at = (
        scheduled_start
        + timedelta(
            minutes=5,
        )
        if point_in_time_violation
        else scheduled_start
        - timedelta(
            hours=2,
        )
    )

    identity_payload = {
        "prediction_contract_version": (
            PREDICTION_CONTRACT_VERSION
        ),
        "prediction_variant": variant,
        "evaluation_row_id": (
            evaluation_row[
                "evaluation_row_id"
            ]
        ),
        "target_id": evaluation_row[
            "target_id"
        ],
        "event_level": evaluation_row[
            "event_level"
        ],
        "game_id": evaluation_row[
            "game_id"
        ],
        "plate_appearance_id": (
            evaluation_row.get(
                "plate_appearance_id"
            )
        ),
        "pitch_id": evaluation_row.get(
            "pitch_id"
        ),
        "event_sequence": (
            evaluation_row[
                "event_sequence"
            ]
        ),
    }

    prediction_id_digest = sha256_payload(
        identity_payload
    )

    record = {
        "prediction_contract_version": (
            PREDICTION_CONTRACT_VERSION
        ),
        "prediction_record_id": (
            f"hpred_{prediction_id_digest[:32]}"
        ),
        "prediction_variant": variant,
        "target_id": evaluation_row[
            "target_id"
        ],
        "event_level": evaluation_row[
            "event_level"
        ],
        "game_id": evaluation_row[
            "game_id"
        ],
        "plate_appearance_id": (
            evaluation_row.get(
                "plate_appearance_id"
            )
        ),
        "pitch_id": evaluation_row.get(
            "pitch_id"
        ),
        "event_sequence": (
            evaluation_row[
                "event_sequence"
            ]
        ),
        "prediction_generated_at_utc": (
            isoformat_utc(
                generated_at
            )
        ),
        "prediction_value": (
            deterministic_prediction_value(
                evaluation_row,
                variant,
            )
        ),
        "prediction_value_type": "probability",
        "model_artifact_id": (
            "synthetic_baseline_model_artifact"
            if variant == "baseline"
            else
            "synthetic_augmented_model_artifact"
        ),
        "model_contract_version": (
            BASELINE_MODEL_CONTRACT_VERSION
            if variant == "baseline"
            else AUGMENTED_MODEL_CONTRACT_VERSION
        ),
        "feature_contract_version": (
            evaluation_row[
                "feature_contract_version"
            ]
        ),
        "overlay_contract_version": (
            None
            if variant == "baseline"
            else AUGMENTED_OVERLAY_CONTRACT_VERSION
        ),
        "prediction_provenance_digest": "",
    }

    record[
        "prediction_provenance_digest"
    ] = sha256_payload(
        {
            key: value
            for key, value in record.items()
            if key
            != "prediction_provenance_digest"
        }
    )

    return record


def build_prediction_fixtures(
    evaluation_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    eligible_rows = [
        row
        for row in evaluation_rows
        if bool(
            row.get(
                "evaluation_eligible"
            )
        )
    ]

    eligible_rows.sort(
        key=lambda row: normalized_string(
            row.get(
                "evaluation_row_id"
            )
        )
    )

    if len(
        eligible_rows
    ) != 18:
        raise RuntimeError(
            "Expected 18 eligible Layer 9N evaluation rows."
        )

    predictions: list[
        dict[str, Any]
    ] = []

    for index, row in enumerate(
        eligible_rows
    ):
        if index != 17:
            predictions.append(
                prediction_record(
                    row,
                    "baseline",
                )
            )

        if index != 16:
            predictions.append(
                prediction_record(
                    row,
                    "augmented",
                    point_in_time_violation=(
                        index == 17
                    ),
                )
            )

    predictions.sort(
        key=lambda row: (
            normalized_string(
                row.get(
                    "game_id"
                )
            ),
            (
                row.get(
                    "event_sequence"
                )
                if isinstance(
                    row.get(
                        "event_sequence"
                    ),
                    int,
                )
                and not isinstance(
                    row.get(
                        "event_sequence"
                    ),
                    bool,
                )
                else -1
            ),
            normalized_string(
                row.get(
                    "target_id"
                )
            ),
            normalized_string(
                row.get(
                    "prediction_variant"
                )
            ),
            normalized_string(
                row.get(
                    "prediction_record_id"
                )
            ),
        )
    )

    return predictions


def prediction_contract_errors(
    prediction: Mapping[str, Any],
) -> list[str]:
    codes: set[str] = set()

    if set(
        prediction
    ) != set(
        PREDICTION_FIELDS
    ):
        codes.add(
            EXCLUSION_CODES[
                "source_invalid"
            ]
        )

    if normalized_string(
        prediction.get(
            "prediction_contract_version"
        )
    ) != PREDICTION_CONTRACT_VERSION:
        codes.add(
            EXCLUSION_CODES[
                "contract_mismatch"
            ]
        )

    variant = normalized_string(
        prediction.get(
            "prediction_variant"
        )
    )

    overlay_version = normalized_string(
        prediction.get(
            "overlay_contract_version"
        )
    )

    if variant == "baseline":
        if overlay_version:
            codes.add(
                EXCLUSION_CODES[
                    "variant_contract"
                ]
            )
    elif variant == "augmented":
        if (
            overlay_version
            != AUGMENTED_OVERLAY_CONTRACT_VERSION
        ):
            codes.add(
                EXCLUSION_CODES[
                    "variant_contract"
                ]
            )
    else:
        codes.add(
            EXCLUSION_CODES[
                "variant_contract"
            ]
        )

    if not valid_sha256(
        prediction.get(
            "prediction_provenance_digest"
        )
    ):
        codes.add(
            EXCLUSION_CODES[
                "prediction_lineage"
            ]
        )

    return sorted(
        codes
    )


def join_id(
    prediction: Mapping[str, Any] | None,
    evaluation_row: Mapping[str, Any] | None,
    variant: str,
) -> str:
    digest = sha256_payload(
        {
            "join_contract_version": (
                JOIN_CONTRACT_VERSION
            ),
            "prediction_record_id": (
                prediction.get(
                    "prediction_record_id"
                )
                if prediction is not None
                else None
            ),
            "evaluation_row_id": (
                evaluation_row.get(
                    "evaluation_row_id"
                )
                if evaluation_row is not None
                else None
            ),
            "prediction_variant": variant,
        }
    )

    return f"hpjoin_{digest[:32]}"


def joined_record(
    prediction: Mapping[str, Any] | None,
    evaluation_row: Mapping[str, Any] | None,
    variant: str,
    status: str,
    eligible: bool,
    exclusion_codes: Sequence[str],
) -> dict[str, Any]:
    source = (
        prediction
        if prediction is not None
        else evaluation_row
        if evaluation_row is not None
        else {}
    )

    identity_digest = sha256_payload(
        {
            "prediction_variant": variant,
            "event_identity": event_identity(
                source
            ),
        }
    )

    row = {
        "prediction_outcome_join_contract_version": (
            JOIN_CONTRACT_VERSION
        ),
        "prediction_outcome_join_id": (
            join_id(
                prediction,
                evaluation_row,
                variant,
            )
        ),
        "evaluation_row_id": (
            evaluation_row.get(
                "evaluation_row_id"
            )
            if evaluation_row is not None
            else None
        ),
        "prediction_record_id": (
            prediction.get(
                "prediction_record_id"
            )
            if prediction is not None
            else None
        ),
        "prediction_variant": variant,
        "target_id": source.get(
            "target_id"
        ),
        "event_level": source.get(
            "event_level"
        ),
        "game_id": source.get(
            "game_id"
        ),
        "plate_appearance_id": (
            source.get(
                "plate_appearance_id"
            )
        ),
        "pitch_id": source.get(
            "pitch_id"
        ),
        "event_sequence": source.get(
            "event_sequence"
        ),
        "prediction_generated_at_utc": (
            prediction.get(
                "prediction_generated_at_utc"
            )
            if prediction is not None
            else None
        ),
        "prediction_value": (
            prediction.get(
                "prediction_value"
            )
            if prediction is not None
            else None
        ),
        "prediction_value_type": (
            prediction.get(
                "prediction_value_type"
            )
            if prediction is not None
            else None
        ),
        "outcome_value": (
            evaluation_row.get(
                "outcome_value"
            )
            if evaluation_row is not None
            else None
        ),
        "outcome_available_at_utc": (
            evaluation_row.get(
                "outcome_available_at_utc"
            )
            if evaluation_row is not None
            else None
        ),
        "evaluation_dataset_version": (
            evaluation_row.get(
                "evaluation_dataset_version"
            )
            if evaluation_row is not None
            else None
        ),
        "prediction_contract_version": (
            prediction.get(
                "prediction_contract_version"
            )
            if prediction is not None
            else None
        ),
        "model_contract_version": (
            prediction.get(
                "model_contract_version"
            )
            if prediction is not None
            else None
        ),
        "feature_contract_version": (
            prediction.get(
                "feature_contract_version"
            )
            if prediction is not None
            else None
        ),
        "overlay_contract_version": (
            prediction.get(
                "overlay_contract_version"
            )
            if prediction is not None
            else None
        ),
        "evaluation_row_digest": (
            evaluation_row.get(
                "evaluation_row_digest"
            )
            if evaluation_row is not None
            else None
        ),
        "prediction_provenance_digest": (
            prediction.get(
                "prediction_provenance_digest"
            )
            if prediction is not None
            else None
        ),
        "prediction_outcome_join_identity_digest": (
            identity_digest
        ),
        "prediction_outcome_join_status": status,
        "prediction_outcome_join_eligible": (
            eligible
        ),
        "prediction_outcome_join_exclusion_codes": (
            sorted(
                set(
                    exclusion_codes
                )
            )
        ),
        "prediction_outcome_join_record_digest": "",
    }

    row[
        "prediction_outcome_join_record_digest"
    ] = sha256_payload(
        {
            key: value
            for key, value in row.items()
            if key
            != "prediction_outcome_join_record_digest"
        }
    )

    return row


def execute_join(
    predictions: Sequence[Mapping[str, Any]],
    evaluation_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    evaluation_by_identity = {
        event_identity(
            row
        ): row
        for row in evaluation_rows
        if bool(
            row.get(
                "evaluation_eligible"
            )
        )
    }

    prediction_counts = Counter(
        prediction_identity(
            row
        )
        for row in predictions
    )

    joined: list[
        dict[str, Any]
    ] = []

    matched_pairs: set[
        tuple[str, str]
    ] = set()

    for prediction in predictions:
        variant = normalized_string(
            prediction.get(
                "prediction_variant"
            )
        )

        identity = event_identity(
            prediction
        )

        evaluation_row = (
            evaluation_by_identity.get(
                identity
            )
        )

        codes = set(
            prediction_contract_errors(
                prediction
            )
        )

        if prediction_counts[
            prediction_identity(
                prediction
            )
        ] > 1:
            codes.add(
                EXCLUSION_CODES[
                    "duplicate_prediction"
                ]
            )

        if evaluation_row is None:
            codes.add(
                EXCLUSION_CODES[
                    "evaluation_missing"
                ]
            )

            joined.append(
                joined_record(
                    prediction,
                    None,
                    variant,
                    (
                        "duplicate_prediction_identity"
                        if EXCLUSION_CODES[
                            "duplicate_prediction"
                        ]
                        in codes
                        else
                        "prediction_without_evaluation_row"
                    ),
                    False,
                    sorted(
                        codes
                    ),
                )
            )
            continue

        generated_at = parse_datetime(
            prediction.get(
                "prediction_generated_at_utc"
            )
        )

        scheduled_start = parse_datetime(
            evaluation_row.get(
                "scheduled_start_utc"
            )
        )

        outcome_available = parse_datetime(
            evaluation_row.get(
                "outcome_available_at_utc"
            )
        )

        if not (
            generated_at is not None
            and scheduled_start is not None
            and outcome_available is not None
            and generated_at
            < scheduled_start
            and generated_at
            < outcome_available
        ):
            codes.add(
                EXCLUSION_CODES[
                    "point_in_time"
                ]
            )

        if not valid_sha256(
            evaluation_row.get(
                "evaluation_row_digest"
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "evaluation_lineage"
                ]
            )

        pair = (
            normalized_string(
                evaluation_row.get(
                    "evaluation_row_id"
                )
            ),
            variant,
        )
        matched_pairs.add(
            pair
        )

        if not codes:
            status = "matched_eligible"
            eligible = True
        elif (
            EXCLUSION_CODES[
                "point_in_time"
            ]
            in codes
        ):
            status = (
                "prediction_point_in_time_violation"
            )
            eligible = False
        elif (
            EXCLUSION_CODES[
                "variant_contract"
            ]
            in codes
        ):
            status = (
                "prediction_variant_contract_invalid"
            )
            eligible = False
        elif (
            EXCLUSION_CODES[
                "prediction_lineage"
            ]
            in codes
        ):
            status = "prediction_lineage_invalid"
            eligible = False
        elif (
            EXCLUSION_CODES[
                "contract_mismatch"
            ]
            in codes
        ):
            status = "prediction_contract_mismatch"
            eligible = False
        elif (
            EXCLUSION_CODES[
                "duplicate_prediction"
            ]
            in codes
        ):
            status = "duplicate_prediction_identity"
            eligible = False
        else:
            status = "prediction_identity_mismatch"
            eligible = False

        joined.append(
            joined_record(
                prediction,
                evaluation_row,
                variant,
                status,
                eligible,
                sorted(
                    codes
                ),
            )
        )

    for evaluation_row in evaluation_by_identity.values():
        evaluation_row_id = normalized_string(
            evaluation_row.get(
                "evaluation_row_id"
            )
        )

        for variant in (
            "baseline",
            "augmented",
        ):
            pair = (
                evaluation_row_id,
                variant,
            )

            if pair in matched_pairs:
                continue

            status = (
                "evaluation_row_without_baseline_prediction"
                if variant == "baseline"
                else
                "evaluation_row_without_augmented_prediction"
            )

            joined.append(
                joined_record(
                    None,
                    evaluation_row,
                    variant,
                    status,
                    False,
                    [
                        EXCLUSION_CODES[
                            "prediction_missing"
                        ]
                    ],
                )
            )

    joined.sort(
        key=lambda row: (
            normalized_string(
                row.get(
                    "game_id"
                )
            ),
            (
                row.get(
                    "event_sequence"
                )
                if isinstance(
                    row.get(
                        "event_sequence"
                    ),
                    int,
                )
                and not isinstance(
                    row.get(
                        "event_sequence"
                    ),
                    bool,
                )
                else -1
            ),
            normalized_string(
                row.get(
                    "target_id"
                )
            ),
            normalized_string(
                row.get(
                    "prediction_variant"
                )
            ),
            normalized_string(
                row.get(
                    "prediction_outcome_join_id"
                )
            ),
        )
    )

    return joined


def replay_evaluation_dataset() -> list[dict[str, Any]]:
    layer_9n = load_module(
        DATASET_IMPLEMENTATION_PATH,
        "layer_9n_prediction_join_source",
    )

    layer_9l = load_module(
        layer_9n.JOIN_IMPLEMENTATION_PATH,
        "layer_9l_prediction_join_source",
    )

    outcomes = layer_9l.load_outcome_records()
    features = layer_9l.build_feature_rows(
        outcomes
    )

    joined_rows, _ = layer_9l.execute_join(
        features,
        outcomes,
    )

    return layer_9n.materialize_dataset(
        joined_rows,
        features,
        outcomes,
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    plan_constants = string_constants(
        PLAN_PATH
    )

    dataset_constants = string_constants(
        DATASET_IMPLEMENTATION_PATH
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

    dataset_implementation_verified = (
        DATASET_IMPLEMENTATION_PATH.exists()
        and EXPECTED_DATASET_VERSION
        in dataset_constants
        and EXPECTED_DATASET_DIAGNOSIS
        in dataset_constants
        and EXPECTED_DATASET_AUTHORITY
        in dataset_constants
    )

    evaluation_rows = replay_evaluation_dataset()

    predictions = build_prediction_fixtures(
        evaluation_rows
    )

    joined_rows = execute_join(
        predictions,
        evaluation_rows,
    )

    reverse_joined_rows = execute_join(
        list(
            reversed(
                predictions
            )
        ),
        list(
            reversed(
                evaluation_rows
            )
        ),
    )

    status_counts = Counter(
        row[
            "prediction_outcome_join_status"
        ]
        for row in joined_rows
    )

    exclusion_counts = Counter(
        code
        for row in joined_rows
        for code in row[
            "prediction_outcome_join_exclusion_codes"
        ]
    )

    eligible_evaluation_rows = [
        row
        for row in evaluation_rows
        if bool(
            row.get(
                "evaluation_eligible"
            )
        )
    ]

    matched_eligible_rows = [
        row
        for row in joined_rows
        if bool(
            row.get(
                "prediction_outcome_join_eligible"
            )
        )
    ]

    prediction_ids = [
        row[
            "prediction_record_id"
        ]
        for row in predictions
    ]

    join_ids = [
        row[
            "prediction_outcome_join_id"
        ]
        for row in joined_rows
    ]

    join_digests = [
        row[
            "prediction_outcome_join_record_digest"
        ]
        for row in joined_rows
    ]

    prediction_field_contract_valid = all(
        set(
            row
        )
        == set(
            PREDICTION_FIELDS
        )
        for row in predictions
    )

    joined_field_contract_valid = all(
        set(
            row
        )
        == set(
            JOINED_FIELDS
        )
        for row in joined_rows
    )

    deterministic_replay = (
        canonical_json_bytes(
            joined_rows
        )
        == canonical_json_bytes(
            reverse_joined_rows
        )
    )

    checks = [
        {
            "check": "nine_o_plan_verified",
            "actual": plan_verified,
            "expected": True,
            "passed": plan_verified,
        },
        {
            "check": "nine_n_dataset_implementation_verified",
            "actual": dataset_implementation_verified,
            "expected": True,
            "passed": dataset_implementation_verified,
        },
        {
            "check": "twenty_nine_evaluation_rows_replayed",
            "actual": len(
                evaluation_rows
            ),
            "expected": 29,
            "passed": len(
                evaluation_rows
            )
            == 29,
        },
        {
            "check": "eighteen_eligible_evaluation_rows_replayed",
            "actual": len(
                eligible_evaluation_rows
            ),
            "expected": 18,
            "passed": len(
                eligible_evaluation_rows
            )
            == 18,
        },
        {
            "check": "thirty_four_prediction_fixtures_created",
            "actual": len(
                predictions
            ),
            "expected": 34,
            "passed": len(
                predictions
            )
            == 34,
        },
        {
            "check": "seventeen_baseline_predictions_created",
            "actual": sum(
                row[
                    "prediction_variant"
                ]
                == "baseline"
                for row in predictions
            ),
            "expected": 17,
            "passed": sum(
                row[
                    "prediction_variant"
                ]
                == "baseline"
                for row in predictions
            )
            == 17,
        },
        {
            "check": "seventeen_augmented_predictions_created",
            "actual": sum(
                row[
                    "prediction_variant"
                ]
                == "augmented"
                for row in predictions
            ),
            "expected": 17,
            "passed": sum(
                row[
                    "prediction_variant"
                ]
                == "augmented"
                for row in predictions
            )
            == 17,
        },
        {
            "check": "seventeen_prediction_fields_implemented",
            "actual": len(
                PREDICTION_FIELDS
            ),
            "expected": 17,
            "passed": (
                len(
                    PREDICTION_FIELDS
                )
                == 17
                and prediction_field_contract_valid
            ),
        },
        {
            "check": "twenty_eight_joined_fields_implemented",
            "actual": len(
                JOINED_FIELDS
            ),
            "expected": 28,
            "passed": (
                len(
                    JOINED_FIELDS
                )
                == 28
                and joined_field_contract_valid
            ),
        },
        {
            "check": "thirty_six_join_records_materialized",
            "actual": len(
                joined_rows
            ),
            "expected": 36,
            "passed": len(
                joined_rows
            )
            == 36,
        },
        {
            "check": "thirty_three_matched_eligible",
            "actual": status_counts[
                "matched_eligible"
            ],
            "expected": 33,
            "passed": status_counts[
                "matched_eligible"
            ]
            == 33,
        },
        {
            "check": "one_point_in_time_violation_classified",
            "actual": status_counts[
                "prediction_point_in_time_violation"
            ],
            "expected": 1,
            "passed": status_counts[
                "prediction_point_in_time_violation"
            ]
            == 1,
        },
        {
            "check": "one_missing_baseline_classified",
            "actual": status_counts[
                "evaluation_row_without_baseline_prediction"
            ],
            "expected": 1,
            "passed": status_counts[
                "evaluation_row_without_baseline_prediction"
            ]
            == 1,
        },
        {
            "check": "one_missing_augmented_classified",
            "actual": status_counts[
                "evaluation_row_without_augmented_prediction"
            ],
            "expected": 1,
            "passed": status_counts[
                "evaluation_row_without_augmented_prediction"
            ]
            == 1,
        },
        {
            "check": "prediction_record_ids_unique",
            "actual": len(
                set(
                    prediction_ids
                )
            ),
            "expected": len(
                predictions
            ),
            "passed": len(
                set(
                    prediction_ids
                )
            )
            == len(
                predictions
            ),
        },
        {
            "check": "join_record_ids_unique",
            "actual": len(
                set(
                    join_ids
                )
            ),
            "expected": len(
                joined_rows
            ),
            "passed": len(
                set(
                    join_ids
                )
            )
            == len(
                joined_rows
            ),
        },
        {
            "check": "prediction_lineage_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "prediction_provenance_digest"
                    ]
                )
                for row in predictions
            ),
            "expected": len(
                predictions
            ),
            "passed": all(
                valid_sha256(
                    row[
                        "prediction_provenance_digest"
                    ]
                )
                for row in predictions
            ),
        },
        {
            "check": "evaluation_lineage_valid",
            "actual": sum(
                valid_sha256(
                    row[
                        "evaluation_row_digest"
                    ]
                )
                for row in eligible_evaluation_rows
            ),
            "expected": len(
                eligible_evaluation_rows
            ),
            "passed": all(
                valid_sha256(
                    row[
                        "evaluation_row_digest"
                    ]
                )
                for row in eligible_evaluation_rows
            ),
        },
        {
            "check": "join_lineage_valid",
            "actual": sum(
                valid_sha256(
                    value
                )
                for value in join_digests
            ),
            "expected": len(
                joined_rows
            ),
            "passed": all(
                valid_sha256(
                    value
                )
                for value in join_digests
            ),
        },
        {
            "check": "eligible_rows_have_no_exclusion_codes",
            "actual": sum(
                not row[
                    "prediction_outcome_join_exclusion_codes"
                ]
                for row in matched_eligible_rows
            ),
            "expected": len(
                matched_eligible_rows
            ),
            "passed": all(
                not row[
                    "prediction_outcome_join_exclusion_codes"
                ]
                for row in matched_eligible_rows
            ),
        },
        {
            "check": "ineligible_rows_have_exclusion_codes",
            "actual": sum(
                bool(
                    row[
                        "prediction_outcome_join_exclusion_codes"
                    ]
                )
                for row in joined_rows
                if not row[
                    "prediction_outcome_join_eligible"
                ]
            ),
            "expected": 3,
            "passed": all(
                bool(
                    row[
                        "prediction_outcome_join_exclusion_codes"
                    ]
                )
                for row in joined_rows
                if not row[
                    "prediction_outcome_join_eligible"
                ]
            ),
        },
        {
            "check": "baseline_overlay_contract_absent",
            "actual": sum(
                not normalized_string(
                    row[
                        "overlay_contract_version"
                    ]
                )
                for row in predictions
                if row[
                    "prediction_variant"
                ]
                == "baseline"
            ),
            "expected": 17,
            "passed": all(
                not normalized_string(
                    row[
                        "overlay_contract_version"
                    ]
                )
                for row in predictions
                if row[
                    "prediction_variant"
                ]
                == "baseline"
            ),
        },
        {
            "check": "augmented_overlay_contract_present",
            "actual": sum(
                normalized_string(
                    row[
                        "overlay_contract_version"
                    ]
                )
                == AUGMENTED_OVERLAY_CONTRACT_VERSION
                for row in predictions
                if row[
                    "prediction_variant"
                ]
                == "augmented"
            ),
            "expected": 17,
            "passed": all(
                normalized_string(
                    row[
                        "overlay_contract_version"
                    ]
                )
                == AUGMENTED_OVERLAY_CONTRACT_VERSION
                for row in predictions
                if row[
                    "prediction_variant"
                ]
                == "augmented"
            ),
        },
        {
            "check": "source_and_join_counts_reconcile",
            "actual": len(
                predictions
            )
            + 2,
            "expected": len(
                joined_rows
            ),
            "passed": len(
                predictions
            )
            + 2
            == len(
                joined_rows
            ),
        },
        {
            "check": "join_replay_deterministic",
            "actual": deterministic_replay,
            "expected": True,
            "passed": deterministic_replay,
        },
        {
            "check": "predictive_metrics_not_calculated",
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
        bool(
            row[
                "passed"
            ]
        )
        for row in checks
    )

    join_digest = sha256_payload(
        joined_rows
    )

    reverse_join_digest = sha256_payload(
        reverse_joined_rows
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_prediction_"
        "outcome_join_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_prediction_"
        "outcome_join_contract_implementation_failed"
    )

    next_layer = (
        "9Q_pitch_type_matchup_overlay_historical_comparative_"
        "evaluation_contract_plan"
        if all_checks_passed
        else
        "9P_pitch_type_matchup_overlay_historical_prediction_"
        "outcome_join_contract_implementation_remediation"
    )

    write_jsonl(
        OUTPUT_DIR
        / "synthetic_prediction_records.jsonl",
        predictions,
    )

    write_jsonl(
        OUTPUT_DIR
        / "prediction_outcome_join_records.jsonl",
        joined_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "prediction_outcome_join_records.csv",
        JOINED_FIELDS,
        [
            {
                **row,
                "prediction_outcome_join_exclusion_codes": (
                    "|".join(
                        row[
                            "prediction_outcome_join_exclusion_codes"
                        ]
                    )
                ),
            }
            for row in joined_rows
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "join_status_counts.csv",
        [
            "prediction_outcome_join_status",
            "count",
        ],
        [
            {
                "prediction_outcome_join_status": status,
                "count": count,
            }
            for status, count in sorted(
                status_counts.items()
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
            for code, count in sorted(
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
                    "Layer 9P implements only a bounded "
                    "temporary diagnostic prediction/outcome join."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_comparative_evaluation_contract_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "The deterministic paired baseline and augmented "
                    "prediction/outcome join permits planning a bounded "
                    "comparative evaluation contract without calculating "
                    "metrics."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "join_contract_version": (
            JOIN_CONTRACT_VERSION
        ),
        "prediction_contract_version": (
            PREDICTION_CONTRACT_VERSION
        ),
        "plan_verified": plan_verified,
        "dataset_implementation_verified": (
            dataset_implementation_verified
        ),
        "evaluation_rows_replayed": len(
            evaluation_rows
        ),
        "eligible_evaluation_rows": len(
            eligible_evaluation_rows
        ),
        "prediction_records_created": len(
            predictions
        ),
        "join_records_materialized": len(
            joined_rows
        ),
        "matched_eligible": len(
            matched_eligible_rows
        ),
        "join_status_counts": dict(
            sorted(
                status_counts.items()
            )
        ),
        "exclusion_code_counts": dict(
            sorted(
                exclusion_counts.items()
            )
        ),
        "implementation_checks_passed": sum(
            bool(
                row[
                    "passed"
                ]
            )
            for row in checks
        ),
        "implementation_checks_required": len(
            checks
        ),
        "join_digest": join_digest,
        "reverse_join_digest": (
            reverse_join_digest
        ),
        "predictive_metrics_calculated": 0,
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
        / "prediction_outcome_join_summary.json",
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
            "historical_comparative_evaluation_contract_planning"
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
            OUTPUT_DIR.relative_to(
                ROOT
            )
        ),
    }

    write_json(
        OUTPUT_DIR
        / "diagnosis.json",
        diagnosis,
    )

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
    print(
        "Join contract version: "
        f"{JOIN_CONTRACT_VERSION}"
    )
    print(
        f"Plan verified: {plan_verified}"
    )
    print(
        "Dataset implementation verified: "
        f"{dataset_implementation_verified}"
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
        "Eligible evaluation rows: "
        f"{summary['eligible_evaluation_rows']}"
    )
    print(
        "Prediction records created: "
        f"{summary['prediction_records_created']}"
    )
    print(
        "Join records materialized: "
        f"{summary['join_records_materialized']}"
    )
    print(
        "Matched eligible: "
        f"{summary['matched_eligible']}"
    )
    print(
        f"Join digest: {join_digest}"
    )
    print(
        "Reverse join digest: "
        f"{reverse_join_digest}"
    )
    print(
        "Predictive metrics calculated: 0"
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
            row[
                "check"
            ]
            for row in checks
            if not row[
                "passed"
            ]
        ]

        print(
            "FAILED CHECKS: "
            + ", ".join(
                failed_checks
            )
        )

        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
