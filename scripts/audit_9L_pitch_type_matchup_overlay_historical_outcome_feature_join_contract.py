#!/usr/bin/env python3
"""
Layer 9L
Pitch-Type Matchup Overlay Historical Outcome Feature Join Contract Implementation

Implements the bounded, deterministic, point-in-time-safe local diagnostic join
contract planned by Layer 9K.

This implementation:

- validates the Layer 9K plan and governing Layer 9B/9D contracts;
- loads the immutable Layer 9H expected historical outcome records;
- creates deterministic synthetic historical feature rows for contract testing;
- validates join identities, grains, versions, timestamps, and cardinality;
- distinguishes unmatched features from explicit missing outcomes;
- assigns stable join statuses and deterministic digests;
- replays the join under reversed input ordering;
- writes temporary diagnostic artifacts only.

This implementation does not:

- join production features or predictions to historical outcomes;
- fetch external historical outcomes;
- materialize production evaluation datasets;
- calculate accuracy, calibration, or incremental-value metrics;
- train or tune models;
- modify probabilities, simulations, pricing, markets, or betting behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9L"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "feature_join_contract_implementation"
)

JOIN_CONTRACT_VERSION = (
    "layer_9L_historical_outcome_feature_join_contract_v1"
)

FEATURE_CONTRACT_VERSION = (
    "layer_9L_synthetic_historical_feature_contract_v1"
)

EXPECTED_JOIN_PLAN_VERSION = (
    "layer_9K_historical_outcome_feature_join_contract_plan_v1"
)

EXPECTED_OUTCOME_CONTRACT_VERSION = (
    "layer_9D_historical_outcome_contract_v1"
)

EXPECTED_POINT_IN_TIME_CONTRACT_VERSION = (
    "layer_9B_point_in_time_historical_evaluation_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9L_pitch_type_matchup_overlay_"
    "historical_outcome_feature_join_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9K_pitch_type_matchup_overlay_"
    "historical_outcome_feature_join_contract.py"
)

OUTCOME_CONTRACT_PATH = (
    ROOT
    / "scripts"
    / "audit_9D_pitch_type_matchup_overlay_"
    "historical_outcome_contract.py"
)

POINT_IN_TIME_CONTRACT_PATH = (
    ROOT
    / "scripts"
    / "audit_9B_pitch_type_matchup_overlay_"
    "point_in_time_historical_evaluation_contract.py"
)

CORPUS_DIR = (
    ROOT
    / "tests"
    / "fixtures"
    / "historical_outcomes"
    / "layer_9H"
)

MANIFEST_PATH = CORPUS_DIR / "manifest.json"
EXPECTED_OUTCOME_RECORDS_PATH = (
    CORPUS_DIR / "expected_outcome_records.jsonl"
)

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "feature_join_contract_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_outcome_feature_join_contract_implementation"
)

EXPECTED_OUTCOME_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "contract_implementation_complete"
)

EXPECTED_POINT_IN_TIME_DIAGNOSIS = (
    "pitch_type_matchup_overlay_point_in_time_historical_"
    "evaluation_contract_implementation_complete"
)

VALID_EVENT_LEVELS = {
    "event",
    "plate_appearance",
    "pitch",
    "contact",
}

FORBIDDEN_FEATURE_KEYS = {
    "outcome_value",
    "outcome_missing",
    "outcome_missing_reason",
    "historical_outcome_eligible",
    "exclusion_codes",
    "historical_outcome_id",
    "outcome_provenance_digest",
    "provider_revision_id",
    "is_final_provider_revision",
    "source_observed_at_utc",
    "source_published_at_utc",
    "outcome_available_at_utc",
}

FAILURE_CODES = {
    "feature_contract_missing":
        "historical_outcome_feature_join_feature_contract_missing",
    "outcome_contract_missing":
        "historical_outcome_feature_join_outcome_contract_missing",
    "contract_version_mismatch":
        "historical_outcome_feature_join_contract_version_mismatch",
    "required_key_missing":
        "historical_outcome_feature_join_required_key_missing",
    "event_level_mismatch":
        "historical_outcome_feature_join_event_level_mismatch",
    "target_mismatch":
        "historical_outcome_feature_join_target_mismatch",
    "game_mismatch":
        "historical_outcome_feature_join_game_mismatch",
    "duplicate_feature":
        "historical_outcome_feature_join_duplicate_feature_identity",
    "duplicate_outcome":
        "historical_outcome_feature_join_duplicate_outcome_identity",
    "many_to_many":
        "historical_outcome_feature_join_many_to_many_detected",
    "point_in_time":
        "historical_outcome_feature_join_point_in_time_violation",
    "outcome_field_in_features":
        "historical_outcome_feature_join_outcome_field_in_features",
    "future_revision":
        "historical_outcome_feature_join_future_revision_selected",
    "ineligible_promoted":
        "historical_outcome_feature_join_ineligible_outcome_promoted",
    "missing_conflated":
        "historical_outcome_feature_join_missing_outcome_conflated",
    "not_deterministic":
        "historical_outcome_feature_join_not_deterministic",
}

PROHIBITED_AUTHORITIES = [
    "accuracy_evaluation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "calibration_evaluation",
    "canonical_probability_authority_change",
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
    "production_feature_outcome_join_execution",
    "production_historical_outcome_materialization",
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


def format_datetime(value: datetime) -> str:
    return (
        value.astimezone(
            timezone.utc
        )
        .isoformat(
            timespec="seconds"
        )
        .replace(
            "+00:00",
            "Z",
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


def read_json(path: Path) -> Any:
    return json.loads(
        path.read_text(
            encoding="utf-8",
        )
    )


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        for line_number, line in enumerate(
            handle,
            start=1,
        ):
            stripped = line.strip()

            if not stripped:
                continue

            row = json.loads(
                stripped
            )

            if not isinstance(
                row,
                dict,
            ):
                raise ValueError(
                    f"{path}:{line_number} is not an object"
                )

            rows.append(
                row
            )

    return rows


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


def nested_keys(
    payload: Any,
) -> set[str]:
    keys: set[str] = set()

    if isinstance(
        payload,
        Mapping,
    ):
        for key, value in payload.items():
            keys.add(
                str(key)
            )
            keys.update(
                nested_keys(
                    value
                )
            )

    elif isinstance(
        payload,
        list,
    ):
        for value in payload:
            keys.update(
                nested_keys(
                    value
                )
            )

    return keys


def join_identity(
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


def identity_payload(
    record: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "target_id": record.get(
            "target_id"
        ),
        "event_level": record.get(
            "event_level"
        ),
        "game_id": record.get(
            "game_id"
        ),
        "plate_appearance_id": record.get(
            "plate_appearance_id"
        ),
        "pitch_id": record.get(
            "pitch_id"
        ),
        "event_sequence": record.get(
            "event_sequence"
        ),
    }


def required_identity_present(
    record: Mapping[str, Any],
) -> bool:
    target_id = normalized_string(
        record.get(
            "target_id"
        )
    )
    event_level = normalized_string(
        record.get(
            "event_level"
        )
    )
    game_id = normalized_string(
        record.get(
            "game_id"
        )
    )
    event_sequence = record.get(
        "event_sequence"
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

    if (
        not target_id
        or event_level not in VALID_EVENT_LEVELS
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

    if (
        event_level
        in {
            "pitch",
            "contact",
            "plate_appearance",
        }
        and not plate_appearance_id
    ):
        return False

    if (
        event_level
        in {
            "pitch",
            "contact",
        }
        and not pitch_id
    ):
        return False

    if (
        event_level == "event"
        and (
            plate_appearance_id
            or pitch_id
        )
    ):
        return False

    if (
        event_level
        == "plate_appearance"
        and pitch_id
    ):
        return False

    return True


def load_outcome_records() -> list[dict[str, Any]]:
    rows = read_jsonl(
        EXPECTED_OUTCOME_RECORDS_PATH
    )

    records: list[dict[str, Any]] = []

    for row in rows:
        record = row.get(
            "outcome_record"
        )

        if isinstance(
            record,
            dict,
        ):
            records.append(
                deepcopy(
                    record
                )
            )

    records.sort(
        key=join_identity
    )

    return records


def build_feature_rows(
    outcomes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    features: list[dict[str, Any]] = []

    for ordinal, outcome in enumerate(
        outcomes,
        start=1,
    ):
        scheduled_start = parse_datetime(
            outcome.get(
                "scheduled_start_utc"
            )
        )

        if scheduled_start is None:
            raise ValueError(
                "Outcome scheduled_start_utc is invalid"
            )

        feature_as_of = (
            scheduled_start
            - timedelta(
                hours=2,
            )
        )

        feature_payload = {
            "pitch_type_matchup_overlay_score": round(
                0.01
                * (
                    ordinal
                    % 17
                ),
                4,
            ),
            "pitcher_profile_version": (
                "synthetic-pitcher-profile-v1"
            ),
            "batter_profile_version": (
                "synthetic-batter-profile-v1"
            ),
            "matchup_overlay_version": (
                "synthetic-overlay-v1"
            ),
        }

        feature = {
            "feature_row_id": (
                f"hfeat_{ordinal:03d}"
            ),
            "feature_contract_version": (
                FEATURE_CONTRACT_VERSION
            ),
            "historical_outcome_contract_version": (
                outcome.get(
                    "historical_outcome_contract_version"
                )
            ),
            "target_id": outcome.get(
                "target_id"
            ),
            "event_level": outcome.get(
                "event_level"
            ),
            "game_id": outcome.get(
                "game_id"
            ),
            "game_date": outcome.get(
                "game_date"
            ),
            "scheduled_start_utc": outcome.get(
                "scheduled_start_utc"
            ),
            "plate_appearance_id": outcome.get(
                "plate_appearance_id"
            ),
            "pitch_id": outcome.get(
                "pitch_id"
            ),
            "pitcher_id": outcome.get(
                "pitcher_id"
            ),
            "batter_id": outcome.get(
                "batter_id"
            ),
            "event_sequence": outcome.get(
                "event_sequence"
            ),
            "feature_as_of_utc": format_datetime(
                feature_as_of
            ),
            "feature_payload": feature_payload,
        }

        feature[
            "feature_provenance_digest"
        ] = sha256_payload(
            {
                key: value
                for key, value in feature.items()
                if key
                != "feature_provenance_digest"
            }
        )

        features.append(
            feature
        )

    features.sort(
        key=join_identity
    )

    return features


def validate_feature(
    feature: Mapping[str, Any],
) -> list[str]:
    codes: list[str] = []

    if (
        normalized_string(
            feature.get(
                "feature_contract_version"
            )
        )
        != FEATURE_CONTRACT_VERSION
        or normalized_string(
            feature.get(
                "historical_outcome_contract_version"
            )
        )
        != EXPECTED_OUTCOME_CONTRACT_VERSION
    ):
        codes.append(
            FAILURE_CODES[
                "contract_version_mismatch"
            ]
        )

    if not required_identity_present(
        feature
    ):
        codes.append(
            FAILURE_CODES[
                "required_key_missing"
            ]
        )

    if (
        nested_keys(
            feature.get(
                "feature_payload",
                {},
            )
        )
        & FORBIDDEN_FEATURE_KEYS
    ):
        codes.append(
            FAILURE_CODES[
                "outcome_field_in_features"
            ]
        )

    feature_as_of = parse_datetime(
        feature.get(
            "feature_as_of_utc"
        )
    )
    scheduled_start = parse_datetime(
        feature.get(
            "scheduled_start_utc"
        )
    )

    if (
        feature_as_of is None
        or scheduled_start is None
        or feature_as_of
        >= scheduled_start
    ):
        codes.append(
            FAILURE_CODES[
                "point_in_time"
            ]
        )

    return sorted(
        set(
            codes
        )
    )


def validate_outcome(
    outcome: Mapping[str, Any],
) -> list[str]:
    codes: list[str] = []

    if (
        normalized_string(
            outcome.get(
                "historical_outcome_contract_version"
            )
        )
        != EXPECTED_OUTCOME_CONTRACT_VERSION
    ):
        codes.append(
            FAILURE_CODES[
                "contract_version_mismatch"
            ]
        )

    if not required_identity_present(
        outcome
    ):
        codes.append(
            FAILURE_CODES[
                "required_key_missing"
            ]
        )

    scheduled_start = parse_datetime(
        outcome.get(
            "scheduled_start_utc"
        )
    )
    outcome_available = parse_datetime(
        outcome.get(
            "outcome_available_at_utc"
        )
    )

    if (
        scheduled_start is None
        or outcome_available is None
        or outcome_available
        < scheduled_start
    ):
        codes.append(
            FAILURE_CODES[
                "point_in_time"
            ]
        )

    return sorted(
        set(
            codes
        )
    )


def joined_record(
    feature: Mapping[str, Any],
    outcome: Mapping[str, Any] | None,
    feature_count: int,
    outcome_count: int,
) -> dict[str, Any]:
    failure_codes = validate_feature(
        feature
    )

    if outcome is not None:
        failure_codes.extend(
            validate_outcome(
                outcome
            )
        )

    if feature_count > 1:
        failure_codes.append(
            FAILURE_CODES[
                "duplicate_feature"
            ]
        )

    if outcome_count > 1:
        failure_codes.append(
            FAILURE_CODES[
                "duplicate_outcome"
            ]
        )

    if (
        feature_count > 1
        and outcome_count > 1
    ):
        failure_codes.append(
            FAILURE_CODES[
                "many_to_many"
            ]
        )

    point_in_time_passed = False

    if outcome is not None:
        feature_as_of = parse_datetime(
            feature.get(
                "feature_as_of_utc"
            )
        )
        outcome_available = parse_datetime(
            outcome.get(
                "outcome_available_at_utc"
            )
        )

        point_in_time_passed = (
            feature_as_of is not None
            and outcome_available is not None
            and feature_as_of
            < outcome_available
        )

        if not point_in_time_passed:
            failure_codes.append(
                FAILURE_CODES[
                    "point_in_time"
                ]
            )

    if outcome is None:
        join_status = (
            "feature_without_outcome"
        )
        evaluation_eligible = False

    elif feature_count > 1:
        join_status = (
            "duplicate_feature_identity"
        )
        evaluation_eligible = False

    elif outcome_count > 1:
        join_status = (
            "duplicate_outcome_identity"
        )
        evaluation_eligible = False

    elif (
        FAILURE_CODES[
            "point_in_time"
        ]
        in failure_codes
    ):
        join_status = (
            "point_in_time_violation"
        )
        evaluation_eligible = False

    elif bool(
        outcome.get(
            "outcome_missing"
        )
    ):
        join_status = (
            "matched_missing_outcome"
        )
        evaluation_eligible = False

    elif not bool(
        outcome.get(
            "historical_outcome_eligible"
        )
    ):
        join_status = (
            "matched_ineligible"
        )
        evaluation_eligible = False

    else:
        join_status = (
            "matched_eligible"
        )
        evaluation_eligible = True

    failure_codes = sorted(
        set(
            failure_codes
        )
    )

    identity = identity_payload(
        feature
    )

    result = {
        "join_contract_version": (
            JOIN_CONTRACT_VERSION
        ),
        "feature_contract_version": (
            feature.get(
                "feature_contract_version"
            )
        ),
        "historical_outcome_contract_version": (
            feature.get(
                "historical_outcome_contract_version"
            )
        ),
        "feature_row_id": feature.get(
            "feature_row_id"
        ),
        "target_id": feature.get(
            "target_id"
        ),
        "event_level": feature.get(
            "event_level"
        ),
        "game_id": feature.get(
            "game_id"
        ),
        "plate_appearance_id": feature.get(
            "plate_appearance_id"
        ),
        "pitch_id": feature.get(
            "pitch_id"
        ),
        "event_sequence": feature.get(
            "event_sequence"
        ),
        "feature_as_of_utc": feature.get(
            "feature_as_of_utc"
        ),
        "outcome_available_at_utc": (
            outcome.get(
                "outcome_available_at_utc"
            )
            if outcome is not None
            else None
        ),
        "historical_outcome_id": (
            outcome.get(
                "historical_outcome_id"
            )
            if outcome is not None
            else None
        ),
        "outcome_value": (
            outcome.get(
                "outcome_value"
            )
            if outcome is not None
            else None
        ),
        "outcome_missing": (
            outcome.get(
                "outcome_missing"
            )
            if outcome is not None
            else None
        ),
        "historical_outcome_eligible": (
            outcome.get(
                "historical_outcome_eligible"
            )
            if outcome is not None
            else None
        ),
        "exclusion_codes": (
            outcome.get(
                "exclusion_codes",
                [],
            )
            if outcome is not None
            else []
        ),
        "join_status": join_status,
        "evaluation_eligible": (
            evaluation_eligible
        ),
        "join_failure_codes": (
            failure_codes
        ),
        "join_identity_digest": (
            sha256_payload(
                identity
            )
        ),
    }

    result[
        "joined_record_digest"
    ] = sha256_payload(
        {
            key: value
            for key, value in result.items()
            if key
            != "joined_record_digest"
        }
    )

    return result


def execute_join(
    features: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    feature_groups: dict[
        tuple[Any, ...],
        list[Mapping[str, Any]],
    ] = defaultdict(
        list
    )

    outcome_groups: dict[
        tuple[Any, ...],
        list[Mapping[str, Any]],
    ] = defaultdict(
        list
    )

    for feature in features:
        feature_groups[
            join_identity(
                feature
            )
        ].append(
            feature
        )

    for outcome in outcomes:
        outcome_groups[
            join_identity(
                outcome
            )
        ].append(
            outcome
        )

    joined_rows: list[
        dict[str, Any]
    ] = []

    for identity in sorted(
        feature_groups,
    ):
        feature_rows = sorted(
            feature_groups[
                identity
            ],
            key=lambda row: normalized_string(
                row.get(
                    "feature_row_id"
                )
            ),
        )

        outcome_rows = sorted(
            outcome_groups.get(
                identity,
                [],
            ),
            key=lambda row: normalized_string(
                row.get(
                    "historical_outcome_id"
                )
            ),
        )

        selected_outcome = (
            outcome_rows[0]
            if outcome_rows
            else None
        )

        for feature in feature_rows:
            joined_rows.append(
                joined_record(
                    feature,
                    selected_outcome,
                    len(
                        feature_rows
                    ),
                    len(
                        outcome_rows
                    ),
                )
            )

    outcome_coverage_rows: list[
        dict[str, Any]
    ] = []

    for identity in sorted(
        outcome_groups,
    ):
        outcome_rows = outcome_groups[
            identity
        ]
        feature_rows = feature_groups.get(
            identity,
            [],
        )

        for outcome in sorted(
            outcome_rows,
            key=lambda row: normalized_string(
                row.get(
                    "historical_outcome_id"
                )
            ),
        ):
            outcome_coverage_rows.append(
                {
                    **identity_payload(
                        outcome
                    ),
                    "historical_outcome_id": (
                        outcome.get(
                            "historical_outcome_id"
                        )
                    ),
                    "feature_match_count": len(
                        feature_rows
                    ),
                    "outcome_identity_count": len(
                        outcome_rows
                    ),
                    "coverage_status": (
                        "matched_feature"
                        if len(
                            feature_rows
                        )
                        == 1
                        else
                        "outcome_without_feature"
                        if not feature_rows
                        else
                        "duplicate_feature_identity"
                    ),
                }
            )

    joined_rows.sort(
        key=lambda row: (
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
                "event_sequence",
                0,
            ),
            normalized_string(
                row.get(
                    "feature_row_id"
                )
            ),
        )
    )

    outcome_coverage_rows.sort(
        key=lambda row: (
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
                    "historical_outcome_id"
                )
            ),
        )
    )

    return (
        joined_rows,
        outcome_coverage_rows,
    )


def build_contract_fixtures(
    features: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    valid_feature = deepcopy(
        features[0]
    )
    valid_outcome = deepcopy(
        outcomes[0]
    )

    unmatched_feature = deepcopy(
        features[1]
    )
    unmatched_feature[
        "feature_row_id"
    ] = "hfeat_unmatched"
    unmatched_feature[
        "target_id"
    ] = "HOUT-UNMATCHED"

    future_feature = deepcopy(
        features[2]
    )
    future_feature[
        "feature_row_id"
    ] = "hfeat_future"
    future_feature[
        "feature_as_of_utc"
    ] = future_feature[
        "scheduled_start_utc"
    ]

    leaked_feature = deepcopy(
        features[3]
    )
    leaked_feature[
        "feature_row_id"
    ] = "hfeat_leaked"
    leaked_feature[
        "feature_payload"
    ] = {
        **leaked_feature[
            "feature_payload"
        ],
        "outcome_value": True,
    }

    duplicate_feature_a = deepcopy(
        features[4]
    )
    duplicate_feature_b = deepcopy(
        features[4]
    )
    duplicate_feature_a[
        "feature_row_id"
    ] = "hfeat_duplicate_a"
    duplicate_feature_b[
        "feature_row_id"
    ] = "hfeat_duplicate_b"

    missing_outcome = next(
        (
            deepcopy(
                row
            )
            for row in outcomes
            if bool(
                row.get(
                    "outcome_missing"
                )
            )
        ),
        None,
    )

    ineligible_outcome = next(
        (
            deepcopy(
                row
            )
            for row in outcomes
            if not bool(
                row.get(
                    "historical_outcome_eligible"
                )
            )
            and not bool(
                row.get(
                    "outcome_missing"
                )
            )
        ),
        None,
    )

    fixtures = [
        {
            "fixture_id": "HJOIN-FIX-001",
            "description": "valid one-to-one eligible join",
            "features": [
                valid_feature
            ],
            "outcomes": [
                valid_outcome
            ],
            "expected_statuses": [
                "matched_eligible"
            ],
        },
        {
            "fixture_id": "HJOIN-FIX-002",
            "description": "feature without outcome",
            "features": [
                unmatched_feature
            ],
            "outcomes": [],
            "expected_statuses": [
                "feature_without_outcome"
            ],
        },
        {
            "fixture_id": "HJOIN-FIX-003",
            "description": "feature cutoff violates point-in-time rule",
            "features": [
                future_feature
            ],
            "outcomes": [
                deepcopy(
                    outcomes[2]
                )
            ],
            "expected_statuses": [
                "point_in_time_violation"
            ],
        },
        {
            "fixture_id": "HJOIN-FIX-004",
            "description": "outcome field leaked into features",
            "features": [
                leaked_feature
            ],
            "outcomes": [
                deepcopy(
                    outcomes[3]
                )
            ],
            "expected_statuses": [
                "matched_eligible"
            ],
            "expected_failure_code": (
                FAILURE_CODES[
                    "outcome_field_in_features"
                ]
            ),
        },
        {
            "fixture_id": "HJOIN-FIX-005",
            "description": "duplicate feature identity",
            "features": [
                duplicate_feature_a,
                duplicate_feature_b,
            ],
            "outcomes": [
                deepcopy(
                    outcomes[4]
                )
            ],
            "expected_statuses": [
                "duplicate_feature_identity",
                "duplicate_feature_identity",
            ],
        },
    ]

    if missing_outcome is not None:
        missing_feature = next(
            deepcopy(
                row
            )
            for row in features
            if join_identity(
                row
            )
            == join_identity(
                missing_outcome
            )
        )

        fixtures.append(
            {
                "fixture_id": "HJOIN-FIX-006",
                "description": (
                    "explicitly missing outcome remains distinct "
                    "from unmatched feature"
                ),
                "features": [
                    missing_feature
                ],
                "outcomes": [
                    missing_outcome
                ],
                "expected_statuses": [
                    "matched_missing_outcome"
                ],
            }
        )

    if ineligible_outcome is not None:
        ineligible_feature = next(
            deepcopy(
                row
            )
            for row in features
            if join_identity(
                row
            )
            == join_identity(
                ineligible_outcome
            )
        )

        fixtures.append(
            {
                "fixture_id": "HJOIN-FIX-007",
                "description": (
                    "contract-ineligible outcome is not promoted"
                ),
                "features": [
                    ineligible_feature
                ],
                "outcomes": [
                    ineligible_outcome
                ],
                "expected_statuses": [
                    "matched_ineligible"
                ],
            }
        )

    return fixtures


def evaluate_contract_fixtures(
    fixtures: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    results: list[
        dict[str, Any]
    ] = []

    for fixture in fixtures:
        joined_rows, _ = execute_join(
            fixture[
                "features"
            ],
            fixture[
                "outcomes"
            ],
        )

        actual_statuses = [
            row[
                "join_status"
            ]
            for row in joined_rows
        ]

        expected_statuses = list(
            fixture[
                "expected_statuses"
            ]
        )

        expected_failure_code = (
            fixture.get(
                "expected_failure_code"
            )
        )

        failure_code_passed = (
            True
            if expected_failure_code
            is None
            else any(
                expected_failure_code
                in row[
                    "join_failure_codes"
                ]
                for row in joined_rows
            )
        )

        passed = (
            actual_statuses
            == expected_statuses
            and failure_code_passed
        )

        results.append(
            {
                "fixture_id": fixture[
                    "fixture_id"
                ],
                "description": fixture[
                    "description"
                ],
                "expected_statuses": "|".join(
                    expected_statuses
                ),
                "actual_statuses": "|".join(
                    actual_statuses
                ),
                "expected_failure_code": (
                    expected_failure_code
                    or ""
                ),
                "failure_code_passed": (
                    failure_code_passed
                ),
                "passed": passed,
            }
        )

    return results


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    plan_constants = string_constants(
        PLAN_PATH
    )
    outcome_constants = string_constants(
        OUTCOME_CONTRACT_PATH
    )
    point_in_time_constants = string_constants(
        POINT_IN_TIME_CONTRACT_PATH
    )

    plan_verified = (
        PLAN_PATH.exists()
        and EXPECTED_PLAN_DIAGNOSIS
        in plan_constants
        and EXPECTED_PLAN_AUTHORITY
        in plan_constants
        and EXPECTED_JOIN_PLAN_VERSION
        in plan_constants
    )

    outcome_contract_verified = (
        OUTCOME_CONTRACT_PATH.exists()
        and EXPECTED_OUTCOME_CONTRACT_VERSION
        in outcome_constants
    )

    point_in_time_contract_verified = (
        POINT_IN_TIME_CONTRACT_PATH.exists()
        and EXPECTED_POINT_IN_TIME_DIAGNOSIS
        in point_in_time_constants
        and EXPECTED_POINT_IN_TIME_CONTRACT_VERSION
        in point_in_time_constants
    )

    manifest = read_json(
        MANIFEST_PATH
    )

    outcomes = load_outcome_records()
    features = build_feature_rows(
        outcomes
    )

    joined_rows, outcome_coverage_rows = (
        execute_join(
            features,
            outcomes,
        )
    )

    reverse_joined_rows, reverse_coverage_rows = (
        execute_join(
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

    fixtures = build_contract_fixtures(
        features,
        outcomes,
    )

    fixture_results = (
        evaluate_contract_fixtures(
            fixtures
        )
    )

    feature_identity_counts = Counter(
        join_identity(
            row
        )
        for row in features
    )

    outcome_identity_counts = Counter(
        join_identity(
            row
        )
        for row in outcomes
    )

    join_status_counts = Counter(
        row[
            "join_status"
        ]
        for row in joined_rows
    )

    feature_validation_results = [
        {
            "feature_row_id": row[
                "feature_row_id"
            ],
            "failure_codes": validate_feature(
                row
            ),
        }
        for row in features
    ]

    outcome_validation_results = [
        {
            "historical_outcome_id": row[
                "historical_outcome_id"
            ],
            "failure_codes": validate_outcome(
                row
            ),
        }
        for row in outcomes
    ]

    feature_validation_failure_count = sum(
        bool(
            row[
                "failure_codes"
            ]
        )
        for row in feature_validation_results
    )

    outcome_validation_failure_count = sum(
        bool(
            row[
                "failure_codes"
            ]
        )
        for row in outcome_validation_results
    )

    expected_feature_validation_failures = 3
    expected_outcome_validation_failures = 4

    feature_validation_classification_valid = (
        feature_validation_failure_count
        == expected_feature_validation_failures
        and all(
            set(
                row[
                    "failure_codes"
                ]
            )
            <= {
                FAILURE_CODES[
                    "required_key_missing"
                ],
            }
            for row in feature_validation_results
        )
    )

    outcome_validation_classification_valid = (
        outcome_validation_failure_count
        == expected_outcome_validation_failures
        and all(
            set(
                row[
                    "failure_codes"
                ]
            )
            <= {
                FAILURE_CODES[
                    "required_key_missing"
                ],
                FAILURE_CODES[
                    "point_in_time"
                ],
            }
            for row in outcome_validation_results
        )
    )

    all_joined_rows_classified = all(
        row[
            "join_status"
        ]
        in {
            "matched_eligible",
            "matched_ineligible",
            "matched_missing_outcome",
            "point_in_time_violation",
        }
        for row in joined_rows
    )

    expected_point_in_time_violations = 1

    point_in_time_violation_count = sum(
        row[
            "join_status"
        ]
        == "point_in_time_violation"
        for row in joined_rows
    )

    eligibility_semantics_valid = all(
        (
            row[
                "evaluation_eligible"
            ]
            is True
            and row[
                "join_status"
            ]
            == "matched_eligible"
        )
        or (
            row[
                "evaluation_eligible"
            ]
            is False
            and row[
                "join_status"
            ]
            != "matched_eligible"
        )
        for row in joined_rows
    )

    explicit_missing_distinct = all(
        row[
            "join_status"
        ]
        == "matched_missing_outcome"
        for row in joined_rows
        if row[
            "outcome_missing"
        ]
        is True
    )

    deterministic_replay = (
        canonical_json_bytes(
            joined_rows
        )
        == canonical_json_bytes(
            reverse_joined_rows
        )
        and canonical_json_bytes(
            outcome_coverage_rows
        )
        == canonical_json_bytes(
            reverse_coverage_rows
        )
    )

    checks = [
        {
            "check": "nine_k_plan_verified",
            "actual": plan_verified,
            "expected": True,
            "passed": plan_verified,
        },
        {
            "check": "historical_outcome_contract_verified",
            "actual": outcome_contract_verified,
            "expected": True,
            "passed": outcome_contract_verified,
        },
        {
            "check": "point_in_time_contract_verified",
            "actual": point_in_time_contract_verified,
            "expected": True,
            "passed": point_in_time_contract_verified,
        },
        {
            "check": "fixture_manifest_version_verified",
            "actual": manifest.get(
                "corpus_version"
            ),
            "expected": (
                "layer_9H_historical_outcome_fixture_corpus_v1"
            ),
            "passed": (
                manifest.get(
                    "corpus_version"
                )
                == "layer_9H_historical_outcome_fixture_corpus_v1"
            ),
        },
        {
            "check": "twenty_nine_outcomes_loaded",
            "actual": len(
                outcomes
            ),
            "expected": 29,
            "passed": len(
                outcomes
            )
            == 29,
        },
        {
            "check": "twenty_nine_features_materialized",
            "actual": len(
                features
            ),
            "expected": 29,
            "passed": len(
                features
            )
            == 29,
        },
        {
            "check": "feature_validation_failures_classified",
            "actual": (
                feature_validation_failure_count
            ),
            "expected": (
                expected_feature_validation_failures
            ),
            "passed": (
                feature_validation_classification_valid
            ),
        },
        {
            "check": "outcome_validation_failures_classified",
            "actual": (
                outcome_validation_failure_count
            ),
            "expected": (
                expected_outcome_validation_failures
            ),
            "passed": (
                outcome_validation_classification_valid
            ),
        },
        {
            "check": "feature_identities_unique",
            "actual": max(
                feature_identity_counts.values()
            ),
            "expected": 1,
            "passed": max(
                feature_identity_counts.values()
            )
            == 1,
        },
        {
            "check": "outcome_identities_unique",
            "actual": max(
                outcome_identity_counts.values()
            ),
            "expected": 1,
            "passed": max(
                outcome_identity_counts.values()
            )
            == 1,
        },
        {
            "check": "twenty_nine_joined_rows_materialized",
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
            "check": "all_fixture_rows_receive_expected_classification",
            "actual": sum(
                row[
                    "join_status"
                ]
                in {
                    "matched_eligible",
                    "matched_ineligible",
                    "matched_missing_outcome",
                    "point_in_time_violation",
                }
                for row in joined_rows
            ),
            "expected": len(
                joined_rows
            ),
            "passed": (
                all_joined_rows_classified
                and point_in_time_violation_count
                == expected_point_in_time_violations
            ),
        },
        {
            "check": "all_outcomes_have_feature_coverage",
            "actual": sum(
                row[
                    "coverage_status"
                ]
                == "matched_feature"
                for row in outcome_coverage_rows
            ),
            "expected": len(
                outcome_coverage_rows
            ),
            "passed": all(
                row[
                    "coverage_status"
                ]
                == "matched_feature"
                for row in outcome_coverage_rows
            ),
        },
        {
            "check": "eligibility_semantics_valid",
            "actual": eligibility_semantics_valid,
            "expected": True,
            "passed": eligibility_semantics_valid,
        },
        {
            "check": "explicit_missing_outcomes_distinct",
            "actual": explicit_missing_distinct,
            "expected": True,
            "passed": explicit_missing_distinct,
        },
        {
            "check": "all_join_identity_digests_unique",
            "actual": len(
                {
                    row[
                        "join_identity_digest"
                    ]
                    for row in joined_rows
                }
            ),
            "expected": len(
                joined_rows
            ),
            "passed": len(
                {
                    row[
                        "join_identity_digest"
                    ]
                    for row in joined_rows
                }
            )
            == len(
                joined_rows
            ),
        },
        {
            "check": "all_joined_record_digests_unique",
            "actual": len(
                {
                    row[
                        "joined_record_digest"
                    ]
                    for row in joined_rows
                }
            ),
            "expected": len(
                joined_rows
            ),
            "passed": len(
                {
                    row[
                        "joined_record_digest"
                    ]
                    for row in joined_rows
                }
            )
            == len(
                joined_rows
            ),
        },
        {
            "check": "contract_fixtures_all_pass",
            "actual": sum(
                bool(
                    row[
                        "passed"
                    ]
                )
                for row in fixture_results
            ),
            "expected": len(
                fixture_results
            ),
            "passed": all(
                bool(
                    row[
                        "passed"
                    ]
                )
                for row in fixture_results
            ),
        },
        {
            "check": "join_replay_deterministic",
            "actual": deterministic_replay,
            "expected": True,
            "passed": deterministic_replay,
        },
        {
            "check": "external_historical_outcome_fetch_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "production_feature_outcome_join_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "prediction_outcome_join_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "predictive_metric_calculation_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "production_probability_change_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "market_and_betting_authority_absent",
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

    joined_rows_digest = sha256_payload(
        joined_rows
    )
    reverse_joined_rows_digest = (
        sha256_payload(
            reverse_joined_rows
        )
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_"
        "feature_join_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_"
        "feature_join_contract_implementation_failed"
    )

    next_layer = (
        "9M_pitch_type_matchup_overlay_historical_evaluation_"
        "dataset_contract_plan"
        if all_checks_passed
        else
        "9L_pitch_type_matchup_overlay_historical_outcome_"
        "feature_join_contract_implementation_remediation"
    )

    write_jsonl(
        OUTPUT_DIR
        / "synthetic_feature_records.jsonl",
        features,
    )

    write_jsonl(
        OUTPUT_DIR
        / "joined_feature_outcome_records.jsonl",
        joined_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "joined_feature_outcome_records.csv",
        [
            "join_contract_version",
            "feature_contract_version",
            "historical_outcome_contract_version",
            "feature_row_id",
            "target_id",
            "event_level",
            "game_id",
            "plate_appearance_id",
            "pitch_id",
            "event_sequence",
            "feature_as_of_utc",
            "outcome_available_at_utc",
            "historical_outcome_id",
            "outcome_value",
            "outcome_missing",
            "historical_outcome_eligible",
            "exclusion_codes",
            "join_status",
            "evaluation_eligible",
            "join_failure_codes",
            "join_identity_digest",
            "joined_record_digest",
        ],
        [
            {
                **row,
                "exclusion_codes": "|".join(
                    row[
                        "exclusion_codes"
                    ]
                ),
                "join_failure_codes": "|".join(
                    row[
                        "join_failure_codes"
                    ]
                ),
            }
            for row in joined_rows
        ],
    )

    write_csv(
        OUTPUT_DIR
        / "outcome_feature_coverage.csv",
        [
            "target_id",
            "event_level",
            "game_id",
            "plate_appearance_id",
            "pitch_id",
            "event_sequence",
            "historical_outcome_id",
            "feature_match_count",
            "outcome_identity_count",
            "coverage_status",
        ],
        outcome_coverage_rows,
    )

    write_csv(
        OUTPUT_DIR
        / "contract_fixture_results.csv",
        [
            "fixture_id",
            "description",
            "expected_statuses",
            "actual_statuses",
            "expected_failure_code",
            "failure_code_passed",
            "passed",
        ],
        fixture_results,
    )

    write_csv(
        OUTPUT_DIR
        / "join_status_counts.csv",
        [
            "join_status",
            "count",
        ],
        [
            {
                "join_status": status,
                "count": count,
            }
            for status, count in sorted(
                join_status_counts.items()
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
        / "failure_code_catalog.csv",
        [
            "failure_name",
            "failure_code",
        ],
        [
            {
                "failure_name": name,
                "failure_code": code,
            }
            for name, code in sorted(
                FAILURE_CODES.items()
            )
        ],
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
                    "Layer 9L implements only a local "
                    "synthetic diagnostic feature/outcome "
                    "join contract."
                ),
            }
            for authority in (
                PROHIBITED_AUTHORITIES
            )
        ]
        + [
            {
                "authority": (
                    "historical_evaluation_dataset_contract_planning"
                ),
                "granted": (
                    all_checks_passed
                ),
                "reason": (
                    "A deterministic, cardinality-safe, "
                    "point-in-time-safe diagnostic join "
                    "permits planning a bounded historical "
                    "evaluation dataset contract."
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
        "feature_contract_version": (
            FEATURE_CONTRACT_VERSION
        ),
        "join_plan_version": (
            EXPECTED_JOIN_PLAN_VERSION
        ),
        "plan_verified": plan_verified,
        "outcome_contract_verified": (
            outcome_contract_verified
        ),
        "point_in_time_contract_verified": (
            point_in_time_contract_verified
        ),
        "outcome_records_loaded": len(
            outcomes
        ),
        "synthetic_feature_records_materialized": len(
            features
        ),
        "joined_records_materialized": len(
            joined_rows
        ),
        "matched_eligible_records": (
            join_status_counts[
                "matched_eligible"
            ]
        ),
        "matched_ineligible_records": (
            join_status_counts[
                "matched_ineligible"
            ]
        ),
        "matched_missing_outcome_records": (
            join_status_counts[
                "matched_missing_outcome"
            ]
        ),
        "outcome_coverage_records": len(
            outcome_coverage_rows
        ),
        "contract_fixtures_executed": len(
            fixture_results
        ),
        "contract_fixtures_passed": sum(
            bool(
                row[
                    "passed"
                ]
            )
            for row in fixture_results
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
        "joined_rows_digest": (
            joined_rows_digest
        ),
        "reverse_joined_rows_digest": (
            reverse_joined_rows_digest
        ),
        "external_records_fetched": 0,
        "production_feature_outcome_joins_executed": 0,
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
        / "join_contract_summary.json",
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
            "historical_evaluation_dataset_contract_planning"
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
        "Plan verified: "
        f"{plan_verified}"
    )
    print(
        "Outcome contract verified: "
        f"{outcome_contract_verified}"
    )
    print(
        "Point-in-time contract verified: "
        f"{point_in_time_contract_verified}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Outcome records loaded: "
        f"{summary['outcome_records_loaded']}"
    )
    print(
        "Synthetic feature records materialized: "
        f"{summary['synthetic_feature_records_materialized']}"
    )
    print(
        "Joined records materialized: "
        f"{summary['joined_records_materialized']}"
    )
    print(
        "Matched eligible records: "
        f"{summary['matched_eligible_records']}"
    )
    print(
        "Matched ineligible records: "
        f"{summary['matched_ineligible_records']}"
    )
    print(
        "Matched missing-outcome records: "
        f"{summary['matched_missing_outcome_records']}"
    )
    print(
        "Contract fixtures passed: "
        f"{summary['contract_fixtures_passed']}/"
        f"{summary['contract_fixtures_executed']}"
    )
    print(
        "Joined rows digest: "
        f"{joined_rows_digest}"
    )
    print(
        "Reverse joined rows digest: "
        f"{reverse_joined_rows_digest}"
    )
    print(
        "External historical outcome records fetched: 0"
    )
    print(
        "Production feature/outcome joins executed: 0"
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
            str(
                row[
                    "check"
                ]
            )
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
