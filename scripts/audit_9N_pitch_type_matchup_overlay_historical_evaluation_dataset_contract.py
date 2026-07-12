#!/usr/bin/env python3
"""
Layer 9N
Pitch-Type Matchup Overlay Historical Evaluation Dataset Contract Implementation

Implements the bounded temporary diagnostic dataset contract planned by Layer 9M.

This layer:

- verifies the Layer 9M plan and Layer 9L join implementation;
- replays the deterministic Layer 9L synthetic feature/outcome join;
- classifies every joined source record;
- materializes one diagnostic evaluation row per joined record;
- preserves eligible and explicitly excluded rows;
- validates point-in-time, identity, contract, lineage, and cardinality rules;
- verifies deterministic replay under reversed source ordering;
- writes temporary diagnostic artifacts only.

This layer does not:

- execute dataset splits;
- join predictions to outcomes;
- calculate predictive or evaluation metrics;
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9N"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_evaluation_"
    "dataset_contract_implementation"
)

DATASET_VERSION = (
    "layer_9N_historical_evaluation_dataset_contract_v1"
)

EXPECTED_PLAN_VERSION = (
    "layer_9M_historical_evaluation_dataset_contract_plan_v1"
)

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_evaluation_"
    "dataset_contract_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_evaluation_dataset_contract_implementation"
)

EXPECTED_JOIN_CONTRACT_VERSION = (
    "layer_9L_historical_outcome_feature_join_contract_v1"
)

EXPECTED_FEATURE_CONTRACT_VERSION = (
    "layer_9L_synthetic_historical_feature_contract_v1"
)

EXPECTED_OUTCOME_CONTRACT_VERSION = (
    "layer_9D_historical_outcome_contract_v1"
)

EXPECTED_JOIN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "feature_join_contract_implementation_complete"
)

EXPECTED_JOIN_AUTHORITY = (
    "historical_evaluation_dataset_contract_planning"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9N_pitch_type_matchup_overlay_"
    "historical_evaluation_dataset_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9M_pitch_type_matchup_overlay_"
    "historical_evaluation_dataset_contract.py"
)

JOIN_IMPLEMENTATION_PATH = (
    ROOT
    / "scripts"
    / "audit_9L_pitch_type_matchup_overlay_"
    "historical_outcome_feature_join_contract.py"
)

SHA256_PATTERN = re.compile(
    r"^[0-9a-f]{64}$"
)

DATASET_FIELDS = [
    "evaluation_dataset_version",
    "evaluation_row_id",
    "target_id",
    "event_level",
    "game_id",
    "game_date",
    "scheduled_start_utc",
    "plate_appearance_id",
    "pitch_id",
    "event_sequence",
    "feature_row_id",
    "feature_as_of_utc",
    "feature_contract_version",
    "feature_provenance_digest",
    "historical_outcome_id",
    "outcome_value",
    "outcome_available_at_utc",
    "historical_outcome_contract_version",
    "outcome_provenance_digest",
    "join_contract_version",
    "join_identity_digest",
    "joined_record_digest",
    "evaluation_eligible",
    "evaluation_exclusion_codes",
    "evaluation_row_digest",
]

VALID_EVENT_LEVELS = {
    "event",
    "plate_appearance",
    "pitch",
    "contact",
}

EXCLUSION_CODES = {
    "join_not_eligible":
        "historical_evaluation_join_not_eligible",
    "outcome_missing":
        "historical_evaluation_outcome_missing",
    "outcome_ineligible":
        "historical_evaluation_outcome_ineligible",
    "point_in_time":
        "historical_evaluation_point_in_time_violation",
    "contract_mismatch":
        "historical_evaluation_contract_version_mismatch",
    "identity_invalid":
        "historical_evaluation_event_identity_invalid",
    "feature_lineage":
        "historical_evaluation_feature_lineage_invalid",
    "outcome_lineage":
        "historical_evaluation_outcome_lineage_invalid",
    "join_lineage":
        "historical_evaluation_join_lineage_invalid",
    "duplicate_identity":
        "historical_evaluation_duplicate_row_identity",
    "outcome_field_in_features":
        "historical_evaluation_outcome_field_in_features",
    "source_record_invalid":
        "historical_evaluation_source_record_invalid",
}

STATUS_BY_JOIN_STATUS = {
    "matched_eligible":
        "evaluation_eligible",
    "matched_ineligible":
        "excluded_outcome_ineligible",
    "matched_missing_outcome":
        "excluded_missing_outcome",
    "point_in_time_violation":
        "excluded_point_in_time_violation",
    "feature_without_outcome":
        "excluded_join_ineligible",
    "duplicate_feature_identity":
        "excluded_duplicate_identity",
    "duplicate_outcome_identity":
        "excluded_duplicate_identity",
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
    "historical_outcome_prediction_join_execution",
    "incremental_value_evaluation",
    "market_comparison",
    "model_training",
    "parameter_tuning",
    "predictive_metric_calculation",
    "pricing",
    "production_historical_evaluation_dataset_materialization",
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


def valid_sha256(value: Any) -> bool:
    return bool(
        SHA256_PATTERN.fullmatch(
            normalized_string(
                value
            )
        )
    )


def event_identity_valid(
    record: Mapping[str, Any],
) -> bool:
    event_level = normalized_string(
        record.get(
            "event_level"
        )
    )
    target_id = normalized_string(
        record.get(
            "target_id"
        )
    )
    game_id = normalized_string(
        record.get(
            "game_id"
        )
    )
    plate_appearance_id = normalized_string(
        record.get(
            "plate_appearance_id"
        )
    )
    pitch_id = normalized_string(
        record.get(
            "pitch_id"
        )
    )
    event_sequence = record.get(
        "event_sequence"
    )

    if (
        event_level not in VALID_EVENT_LEVELS
        or not target_id
        or not game_id
        or isinstance(
            event_sequence,
            bool,
        )
        or not isinstance(
            event_sequence,
            int,
        )
    ):
        return False

    if event_level == "event":
        return (
            not plate_appearance_id
            and not pitch_id
        )

    if event_level == "plate_appearance":
        return bool(
            plate_appearance_id
        ) and not pitch_id

    return bool(
        plate_appearance_id
    ) and bool(
        pitch_id
    )


def source_identity(
    record: Mapping[str, Any],
) -> tuple[
    str,
    str,
    str,
    str,
    str,
    int | str,
]:
    return (
        normalized_string(
            record.get(
                "target_id"
            )
        ),
        normalized_string(
            record.get(
                "event_level"
            )
        ),
        normalized_string(
            record.get(
                "game_id"
            )
        ),
        normalized_string(
            record.get(
                "plate_appearance_id"
            )
        ),
        normalized_string(
            record.get(
                "pitch_id"
            )
        ),
        record.get(
            "event_sequence",
            "",
        ),
    )


def classify_row(
    joined: Mapping[str, Any],
    feature: Mapping[str, Any] | None,
    outcome: Mapping[str, Any] | None,
) -> tuple[
    str,
    bool,
    list[str],
]:
    codes: set[str] = set()

    join_status = normalized_string(
        joined.get(
            "join_status"
        )
    )

    if join_status != "matched_eligible":
        codes.add(
            EXCLUSION_CODES[
                "join_not_eligible"
            ]
        )

    if outcome is None:
        codes.add(
            EXCLUSION_CODES[
                "source_record_invalid"
            ]
        )
    else:
        if bool(
            outcome.get(
                "outcome_missing"
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "outcome_missing"
                ]
            )

        if not bool(
            outcome.get(
                "historical_outcome_eligible"
            )
        ):
            codes.add(
                EXCLUSION_CODES[
                    "outcome_ineligible"
                ]
            )

    expected_contracts = (
        normalized_string(
            joined.get(
                "join_contract_version"
            )
        )
        == EXPECTED_JOIN_CONTRACT_VERSION
        and normalized_string(
            joined.get(
                "feature_contract_version"
            )
        )
        == EXPECTED_FEATURE_CONTRACT_VERSION
        and normalized_string(
            joined.get(
                "historical_outcome_contract_version"
            )
        )
        == EXPECTED_OUTCOME_CONTRACT_VERSION
    )

    if not expected_contracts:
        codes.add(
            EXCLUSION_CODES[
                "contract_mismatch"
            ]
        )

    if not event_identity_valid(
        joined
    ):
        codes.add(
            EXCLUSION_CODES[
                "identity_invalid"
            ]
        )

    feature_as_of = parse_datetime(
        joined.get(
            "feature_as_of_utc"
        )
    )
    scheduled_start = parse_datetime(
        (
            feature or {}
        ).get(
            "scheduled_start_utc"
        )
    )
    outcome_available = parse_datetime(
        joined.get(
            "outcome_available_at_utc"
        )
    )

    point_in_time_valid = (
        feature_as_of is not None
        and scheduled_start is not None
        and outcome_available is not None
        and feature_as_of
        < scheduled_start
        and feature_as_of
        < outcome_available
        and outcome_available
        >= scheduled_start
    )

    if not point_in_time_valid:
        codes.add(
            EXCLUSION_CODES[
                "point_in_time"
            ]
        )

    if (
        feature is None
        or not valid_sha256(
            feature.get(
                "feature_provenance_digest"
            )
        )
    ):
        codes.add(
            EXCLUSION_CODES[
                "feature_lineage"
            ]
        )

    if (
        outcome is None
        or not valid_sha256(
            outcome.get(
                "outcome_provenance_digest"
            )
        )
    ):
        codes.add(
            EXCLUSION_CODES[
                "outcome_lineage"
            ]
        )

    if (
        not valid_sha256(
            joined.get(
                "join_identity_digest"
            )
        )
        or not valid_sha256(
            joined.get(
                "joined_record_digest"
            )
        )
    ):
        codes.add(
            EXCLUSION_CODES[
                "join_lineage"
            ]
        )

    joined_failure_codes = set(
        joined.get(
            "join_failure_codes",
            [],
        )
    )

    if any(
        "outcome_field_in_features"
        in code
        for code in joined_failure_codes
    ):
        codes.add(
            EXCLUSION_CODES[
                "outcome_field_in_features"
            ]
        )

    eligible = (
        join_status == "matched_eligible"
        and bool(
            joined.get(
                "evaluation_eligible"
            )
        )
        and not codes
    )

    if eligible:
        status = "evaluation_eligible"
    elif (
        EXCLUSION_CODES[
            "outcome_missing"
        ]
        in codes
    ):
        status = "excluded_missing_outcome"
    elif (
        EXCLUSION_CODES[
            "point_in_time"
        ]
        in codes
    ):
        status = (
            "excluded_point_in_time_violation"
        )
    elif (
        EXCLUSION_CODES[
            "identity_invalid"
        ]
        in codes
    ):
        status = "excluded_identity_invalid"
    elif (
        EXCLUSION_CODES[
            "contract_mismatch"
        ]
        in codes
    ):
        status = "excluded_contract_mismatch"
    elif (
        EXCLUSION_CODES[
            "feature_lineage"
        ]
        in codes
        or EXCLUSION_CODES[
            "outcome_lineage"
        ]
        in codes
        or EXCLUSION_CODES[
            "join_lineage"
        ]
        in codes
    ):
        status = "excluded_lineage_invalid"
    elif (
        EXCLUSION_CODES[
            "outcome_field_in_features"
        ]
        in codes
    ):
        status = "excluded_leakage_detected"
    elif (
        EXCLUSION_CODES[
            "outcome_ineligible"
        ]
        in codes
    ):
        status = "excluded_outcome_ineligible"
    else:
        status = "excluded_join_ineligible"

    return (
        status,
        eligible,
        sorted(
            codes
        ),
    )


def evaluation_row_id(
    joined: Mapping[str, Any],
) -> str:
    digest = sha256_payload(
        {
            "evaluation_dataset_version": (
                DATASET_VERSION
            ),
            "target_id": joined.get(
                "target_id"
            ),
            "event_level": joined.get(
                "event_level"
            ),
            "game_id": joined.get(
                "game_id"
            ),
            "plate_appearance_id": joined.get(
                "plate_appearance_id"
            ),
            "pitch_id": joined.get(
                "pitch_id"
            ),
            "event_sequence": joined.get(
                "event_sequence"
            ),
            "join_identity_digest": joined.get(
                "join_identity_digest"
            ),
        }
    )

    return f"heval_{digest[:32]}"


def materialize_dataset(
    joined_rows: Sequence[Mapping[str, Any]],
    features: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    features_by_id = {
        normalized_string(
            row.get(
                "feature_row_id"
            )
        ): row
        for row in features
    }

    outcomes_by_id = {
        normalized_string(
            row.get(
                "historical_outcome_id"
            )
        ): row
        for row in outcomes
    }

    rows: list[
        dict[str, Any]
    ] = []

    for joined in joined_rows:
        feature = features_by_id.get(
            normalized_string(
                joined.get(
                    "feature_row_id"
                )
            )
        )

        outcome = outcomes_by_id.get(
            normalized_string(
                joined.get(
                    "historical_outcome_id"
                )
            )
        )

        (
            status,
            eligible,
            exclusion_codes,
        ) = classify_row(
            joined,
            feature,
            outcome,
        )

        row = {
            "evaluation_dataset_version": (
                DATASET_VERSION
            ),
            "evaluation_row_id": (
                evaluation_row_id(
                    joined
                )
            ),
            "target_id": joined.get(
                "target_id"
            ),
            "event_level": joined.get(
                "event_level"
            ),
            "game_id": joined.get(
                "game_id"
            ),
            "game_date": (
                feature.get(
                    "game_date"
                )
                if feature is not None
                else None
            ),
            "scheduled_start_utc": (
                feature.get(
                    "scheduled_start_utc"
                )
                if feature is not None
                else None
            ),
            "plate_appearance_id": (
                joined.get(
                    "plate_appearance_id"
                )
            ),
            "pitch_id": joined.get(
                "pitch_id"
            ),
            "event_sequence": joined.get(
                "event_sequence"
            ),
            "feature_row_id": joined.get(
                "feature_row_id"
            ),
            "feature_as_of_utc": joined.get(
                "feature_as_of_utc"
            ),
            "feature_contract_version": (
                joined.get(
                    "feature_contract_version"
                )
            ),
            "feature_provenance_digest": (
                feature.get(
                    "feature_provenance_digest"
                )
                if feature is not None
                else None
            ),
            "historical_outcome_id": (
                joined.get(
                    "historical_outcome_id"
                )
            ),
            "outcome_value": joined.get(
                "outcome_value"
            ),
            "outcome_available_at_utc": (
                joined.get(
                    "outcome_available_at_utc"
                )
            ),
            "historical_outcome_contract_version": (
                joined.get(
                    "historical_outcome_contract_version"
                )
            ),
            "outcome_provenance_digest": (
                outcome.get(
                    "outcome_provenance_digest"
                )
                if outcome is not None
                else None
            ),
            "join_contract_version": (
                joined.get(
                    "join_contract_version"
                )
            ),
            "join_identity_digest": (
                joined.get(
                    "join_identity_digest"
                )
            ),
            "joined_record_digest": (
                joined.get(
                    "joined_record_digest"
                )
            ),
            "evaluation_eligible": (
                eligible
            ),
            "evaluation_exclusion_codes": (
                exclusion_codes
            ),
            "evaluation_row_digest": "",
            "_dataset_status": status,
        }

        row[
            "evaluation_row_digest"
        ] = sha256_payload(
            {
                key: value
                for key, value in row.items()
                if key
                not in {
                    "evaluation_row_digest",
                    "_dataset_status",
                }
            }
        )

        rows.append(
            row
        )

    rows.sort(
        key=lambda row: (
            normalized_string(
                row.get(
                    "game_date"
                )
            ),
            normalized_string(
                row.get(
                    "scheduled_start_utc"
                )
            ),
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
                    "evaluation_row_id"
                )
            ),
        )
    )

    return rows


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    plan_constants = string_constants(
        PLAN_PATH
    )
    join_constants = string_constants(
        JOIN_IMPLEMENTATION_PATH
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

    join_implementation_verified = (
        JOIN_IMPLEMENTATION_PATH.exists()
        and EXPECTED_JOIN_CONTRACT_VERSION
        in join_constants
        and EXPECTED_FEATURE_CONTRACT_VERSION
        in join_constants
        and EXPECTED_OUTCOME_CONTRACT_VERSION
        in join_constants
        and EXPECTED_JOIN_DIAGNOSIS
        in join_constants
        and EXPECTED_JOIN_AUTHORITY
        in join_constants
    )

    layer_9l = load_module(
        JOIN_IMPLEMENTATION_PATH,
        "layer_9l_dataset_source",
    )

    outcomes = layer_9l.load_outcome_records()
    features = layer_9l.build_feature_rows(
        outcomes
    )

    joined_rows, _ = layer_9l.execute_join(
        features,
        outcomes,
    )

    reverse_joined_rows, _ = (
        layer_9l.execute_join(
            list(
                reversed(
                    features
                )
            ),
            list(
                reversed(
                    outcomes
                )
            ),
        )
    )

    dataset_rows = materialize_dataset(
        joined_rows,
        features,
        outcomes,
    )

    reverse_dataset_rows = (
        materialize_dataset(
            reverse_joined_rows,
            list(
                reversed(
                    features
                )
            ),
            list(
                reversed(
                    outcomes
                )
            ),
        )
    )

    eligible_rows = [
        row
        for row in dataset_rows
        if row[
            "evaluation_eligible"
        ]
    ]

    excluded_rows = [
        row
        for row in dataset_rows
        if not row[
            "evaluation_eligible"
        ]
    ]

    status_counts = Counter(
        row[
            "_dataset_status"
        ]
        for row in dataset_rows
    )

    exclusion_counts = Counter(
        code
        for row in dataset_rows
        for code in row[
            "evaluation_exclusion_codes"
        ]
    )

    evaluation_row_ids = [
        row[
            "evaluation_row_id"
        ]
        for row in dataset_rows
    ]

    evaluation_row_digests = [
        row[
            "evaluation_row_digest"
        ]
        for row in dataset_rows
    ]

    field_contract_valid = all(
        set(
            row
        )
        == set(
            DATASET_FIELDS
        )
        | {
            "_dataset_status"
        }
        for row in dataset_rows
    )

    eligible_semantics_valid = all(
        (
            row[
                "evaluation_eligible"
            ]
            and not row[
                "evaluation_exclusion_codes"
            ]
            and row[
                "_dataset_status"
            ]
            == "evaluation_eligible"
        )
        or (
            not row[
                "evaluation_eligible"
            ]
            and bool(
                row[
                    "evaluation_exclusion_codes"
                ]
            )
            and row[
                "_dataset_status"
            ]
            != "evaluation_eligible"
        )
        for row in dataset_rows
    )

    deterministic_replay = (
        canonical_json_bytes(
            dataset_rows
        )
        == canonical_json_bytes(
            reverse_dataset_rows
        )
    )

    checks = [
        {
            "check": "nine_m_plan_verified",
            "actual": plan_verified,
            "expected": True,
            "passed": plan_verified,
        },
        {
            "check": "nine_l_join_implementation_verified",
            "actual": (
                join_implementation_verified
            ),
            "expected": True,
            "passed": (
                join_implementation_verified
            ),
        },
        {
            "check": "twenty_nine_join_records_loaded",
            "actual": len(
                joined_rows
            ),
            "expected": 29,
            "passed": len(
                joined_rows
            )
            == 29,
        },
        {
            "check": "twenty_nine_dataset_rows_materialized",
            "actual": len(
                dataset_rows
            ),
            "expected": 29,
            "passed": len(
                dataset_rows
            )
            == 29,
        },
        {
            "check": "twenty_five_field_contract_implemented",
            "actual": len(
                DATASET_FIELDS
            ),
            "expected": 25,
            "passed": (
                len(
                    DATASET_FIELDS
                )
                == 25
                and field_contract_valid
            ),
        },
        {
            "check": "eighteen_rows_evaluation_eligible",
            "actual": len(
                eligible_rows
            ),
            "expected": 18,
            "passed": len(
                eligible_rows
            )
            == 18,
        },
        {
            "check": "eleven_rows_explicitly_excluded",
            "actual": len(
                excluded_rows
            ),
            "expected": 11,
            "passed": len(
                excluded_rows
            )
            == 11,
        },
        {
            "check": "two_missing_outcomes_excluded",
            "actual": status_counts[
                "excluded_missing_outcome"
            ],
            "expected": 2,
            "passed": status_counts[
                "excluded_missing_outcome"
            ]
            == 2,
        },
        {
            "check": "one_point_in_time_violation_excluded",
            "actual": status_counts[
                "excluded_point_in_time_violation"
            ],
            "expected": 1,
            "passed": status_counts[
                "excluded_point_in_time_violation"
            ]
            == 1,
        },
        {
            "check": "eight_other_ineligible_rows_excluded",
            "actual": (
                len(
                    excluded_rows
                )
                - status_counts[
                    "excluded_missing_outcome"
                ]
                - status_counts[
                    "excluded_point_in_time_violation"
                ]
            ),
            "expected": 8,
            "passed": (
                len(
                    excluded_rows
                )
                - status_counts[
                    "excluded_missing_outcome"
                ]
                - status_counts[
                    "excluded_point_in_time_violation"
                ]
            )
            == 8,
        },
        {
            "check": "evaluation_row_ids_unique",
            "actual": len(
                set(
                    evaluation_row_ids
                )
            ),
            "expected": len(
                dataset_rows
            ),
            "passed": len(
                set(
                    evaluation_row_ids
                )
            )
            == len(
                dataset_rows
            ),
        },
        {
            "check": "evaluation_row_digests_unique",
            "actual": len(
                set(
                    evaluation_row_digests
                )
            ),
            "expected": len(
                dataset_rows
            ),
            "passed": len(
                set(
                    evaluation_row_digests
                )
            )
            == len(
                dataset_rows
            ),
        },
        {
            "check": "evaluation_row_digests_valid_sha256",
            "actual": sum(
                valid_sha256(
                    value
                )
                for value in (
                    evaluation_row_digests
                )
            ),
            "expected": len(
                dataset_rows
            ),
            "passed": all(
                valid_sha256(
                    value
                )
                for value in (
                    evaluation_row_digests
                )
            ),
        },
        {
            "check": "feature_lineage_preserved",
            "actual": sum(
                valid_sha256(
                    row[
                        "feature_provenance_digest"
                    ]
                )
                for row in dataset_rows
            ),
            "expected": len(
                dataset_rows
            ),
            "passed": all(
                valid_sha256(
                    row[
                        "feature_provenance_digest"
                    ]
                )
                for row in dataset_rows
            ),
        },
        {
            "check": "outcome_lineage_preserved",
            "actual": sum(
                valid_sha256(
                    row[
                        "outcome_provenance_digest"
                    ]
                )
                for row in dataset_rows
            ),
            "expected": len(
                dataset_rows
            ),
            "passed": all(
                valid_sha256(
                    row[
                        "outcome_provenance_digest"
                    ]
                )
                for row in dataset_rows
            ),
        },
        {
            "check": "join_lineage_preserved",
            "actual": sum(
                valid_sha256(
                    row[
                        "joined_record_digest"
                    ]
                )
                for row in dataset_rows
            ),
            "expected": len(
                dataset_rows
            ),
            "passed": all(
                valid_sha256(
                    row[
                        "joined_record_digest"
                    ]
                )
                for row in dataset_rows
            ),
        },
        {
            "check": "eligible_and_excluded_semantics_valid",
            "actual": (
                eligible_semantics_valid
            ),
            "expected": True,
            "passed": (
                eligible_semantics_valid
            ),
        },
        {
            "check": "all_excluded_rows_have_codes",
            "actual": sum(
                bool(
                    row[
                        "evaluation_exclusion_codes"
                    ]
                )
                for row in excluded_rows
            ),
            "expected": len(
                excluded_rows
            ),
            "passed": all(
                bool(
                    row[
                        "evaluation_exclusion_codes"
                    ]
                )
                for row in excluded_rows
            ),
        },
        {
            "check": "all_eligible_rows_have_no_codes",
            "actual": sum(
                not row[
                    "evaluation_exclusion_codes"
                ]
                for row in eligible_rows
            ),
            "expected": len(
                eligible_rows
            ),
            "passed": all(
                not row[
                    "evaluation_exclusion_codes"
                ]
                for row in eligible_rows
            ),
        },
        {
            "check": "source_and_dataset_counts_reconcile",
            "actual": len(
                eligible_rows
            )
            + len(
                excluded_rows
            ),
            "expected": len(
                joined_rows
            ),
            "passed": (
                len(
                    eligible_rows
                )
                + len(
                    excluded_rows
                )
                == len(
                    joined_rows
                )
            ),
        },
        {
            "check": "dataset_replay_deterministic",
            "actual": (
                deterministic_replay
            ),
            "expected": True,
            "passed": (
                deterministic_replay
            ),
        },
        {
            "check": "dataset_split_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "prediction_outcome_join_not_executed",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "predictive_metrics_not_calculated",
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

    dataset_digest = sha256_payload(
        dataset_rows
    )

    reverse_dataset_digest = (
        sha256_payload(
            reverse_dataset_rows
        )
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_evaluation_"
        "dataset_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_evaluation_"
        "dataset_contract_implementation_failed"
    )

    next_layer = (
        "9O_pitch_type_matchup_overlay_historical_prediction_"
        "outcome_join_contract_plan"
        if all_checks_passed
        else
        "9N_pitch_type_matchup_overlay_historical_evaluation_"
        "dataset_contract_implementation_remediation"
    )

    export_rows = [
        {
            key: value
            for key, value in row.items()
            if key != "_dataset_status"
        }
        for row in dataset_rows
    ]

    write_jsonl(
        OUTPUT_DIR
        / "historical_evaluation_dataset.jsonl",
        export_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "historical_evaluation_dataset.csv",
        DATASET_FIELDS,
        [
            {
                **row,
                "evaluation_exclusion_codes": "|".join(
                    row[
                        "evaluation_exclusion_codes"
                    ]
                ),
            }
            for row in export_rows
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "dataset_status_counts.csv",
        [
            "dataset_status",
            "count",
        ],
        [
            {
                "dataset_status": status,
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
                    "Layer 9N implements only a bounded "
                    "temporary diagnostic historical "
                    "evaluation dataset contract."
                ),
            }
            for authority in (
                PROHIBITED_AUTHORITIES
            )
        ]
        + [
            {
                "authority": (
                    "historical_prediction_outcome_join_contract_planning"
                ),
                "granted": (
                    all_checks_passed
                ),
                "reason": (
                    "A deterministic, point-in-time-safe "
                    "historical evaluation dataset permits "
                    "planning the bounded prediction/outcome "
                    "join contract."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "dataset_version": DATASET_VERSION,
        "plan_verified": plan_verified,
        "join_implementation_verified": (
            join_implementation_verified
        ),
        "source_join_records_loaded": len(
            joined_rows
        ),
        "dataset_rows_materialized": len(
            dataset_rows
        ),
        "eligible_rows": len(
            eligible_rows
        ),
        "excluded_rows": len(
            excluded_rows
        ),
        "dataset_status_counts": dict(
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
        "dataset_digest": dataset_digest,
        "reverse_dataset_digest": (
            reverse_dataset_digest
        ),
        "dataset_splits_executed": 0,
        "prediction_outcome_joins_executed": 0,
        "predictive_metrics_calculated": 0,
        "production_records_materialized": 0,
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
        / "dataset_contract_summary.json",
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
            "historical_prediction_outcome_join_contract_planning"
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
        "Dataset version: "
        f"{DATASET_VERSION}"
    )
    print(
        f"Plan verified: {plan_verified}"
    )
    print(
        "Join implementation verified: "
        f"{join_implementation_verified}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Source join records loaded: "
        f"{summary['source_join_records_loaded']}"
    )
    print(
        "Dataset rows materialized: "
        f"{summary['dataset_rows_materialized']}"
    )
    print(
        f"Eligible rows: {summary['eligible_rows']}"
    )
    print(
        f"Excluded rows: {summary['excluded_rows']}"
    )
    print(
        f"Dataset digest: {dataset_digest}"
    )
    print(
        "Reverse dataset digest: "
        f"{reverse_dataset_digest}"
    )
    print(
        "Dataset splits executed: 0"
    )
    print(
        "Prediction/outcome joins executed: 0"
    )
    print(
        "Predictive metrics calculated: 0"
    )
    print(
        "Production records materialized: 0"
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
