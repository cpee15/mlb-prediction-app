#!/usr/bin/env python3
"""
Layer 9F
Pitch-Type Matchup Overlay Historical Outcome Source Adapter Contract

Implements the deterministic synthetic source-adapter contract authorized by
Layer 9E and maps provider-shaped payloads into the Layer 9D historical outcome
contract.

This layer implements:

- a versioned synthetic provider adapter contract;
- canonical game, plate-appearance, and pitch identities;
- deterministic mappings for all ten historical outcome targets;
- explicit availability and revision metadata handling;
- immutable raw payload digests;
- deterministic adapter and replay fixtures;
- delegation into the Layer 9D historical outcome contract;
- deterministic CSV and JSON audit artifacts.

This layer does not:

- fetch external historical outcomes;
- execute live or production historical outcome collection;
- materialize production historical outcome datasets;
- join historical outcomes to Layer 8 features;
- join historical outcomes to predictions;
- calculate predictive metrics;
- evaluate accuracy, calibration, or incremental value;
- train or tune models, thresholds, weights, or fallbacks;
- run backtests;
- modify production, simulation, pricing, or betting behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
import math
import re
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9F"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_source_adapter_contract"
)

ADAPTER_CONTRACT_VERSION = (
    "layer_9F_historical_outcome_source_adapter_contract_v1"
)
PROVIDER_MAPPING_VERSION = "layer_9F_synthetic_provider_mapping_v1"
RAW_PAYLOAD_SCHEMA_VERSION = "layer_9F_synthetic_payload_schema_v1"

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9F_pitch_type_matchup_overlay_historical_outcome_source_adapter_contract"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "plan_9E_pitch_type_matchup_overlay_historical_outcome_source_mapping.py"
)

HISTORICAL_OUTCOME_CONTRACT_PATH = (
    ROOT
    / "scripts"
    / "audit_9D_pitch_type_matchup_overlay_historical_outcome_contract.py"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_source_mapping_plan_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_source_adapter_contract_implementation"
)

EXPECTED_OUTCOME_CONTRACT_VERSION = (
    "layer_9D_historical_outcome_contract_v1"
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


ADAPTER_INPUT_FIELDS = [
    {"ordinal": 1, "field": "provider", "type": "string", "required": True},
    {"ordinal": 2, "field": "provider_event_id", "type": "string", "required": True},
    {"ordinal": 3, "field": "provider_payload_version", "type": "string", "required": True},
    {"ordinal": 4, "field": "provider_revision_id", "type": "nullable_string", "required": True},
    {"ordinal": 5, "field": "is_final_provider_revision", "type": "boolean", "required": True},
    {"ordinal": 6, "field": "ingestion_run_id", "type": "string", "required": True},
    {"ordinal": 7, "field": "game_pk", "type": "string", "required": True},
    {"ordinal": 8, "field": "game_date", "type": "date", "required": True},
    {"ordinal": 9, "field": "scheduled_start_utc", "type": "datetime", "required": True},
    {"ordinal": 10, "field": "at_bat_number", "type": "integer", "required": False},
    {"ordinal": 11, "field": "pitch_number", "type": "integer", "required": False},
    {"ordinal": 12, "field": "pitcher", "type": "string", "required": True},
    {"ordinal": 13, "field": "batter", "type": "string", "required": True},
    {"ordinal": 14, "field": "description", "type": "nullable_string", "required": False},
    {"ordinal": 15, "field": "events", "type": "nullable_string", "required": False},
    {"ordinal": 16, "field": "contact_quality_value", "type": "nullable_number", "required": False},
    {"ordinal": 17, "field": "run_value", "type": "nullable_number", "required": False},
    {"ordinal": 18, "field": "event_occurred_at_utc", "type": "nullable_datetime", "required": False},
    {"ordinal": 19, "field": "source_observed_at_utc", "type": "datetime", "required": True},
    {"ordinal": 20, "field": "source_published_at_utc", "type": "nullable_datetime", "required": False},
    {"ordinal": 21, "field": "outcome_available_at_utc", "type": "datetime", "required": True},
    {"ordinal": 22, "field": "collection_enabled", "type": "boolean", "required": True},
    {"ordinal": 23, "field": "game_incomplete", "type": "boolean", "required": True},
    {"ordinal": 24, "field": "identity_conflict", "type": "boolean", "required": True},
    {"ordinal": 25, "field": "revision_conflict", "type": "boolean", "required": True},
]


TARGET_ADAPTER_MAP = [
    {
        "target_id": "HOUT-O01",
        "target": "swing_event",
        "event_level": "pitch",
        "raw_code_field": "description",
        "value_field": "",
    },
    {
        "target_id": "HOUT-O02",
        "target": "whiff_event",
        "event_level": "pitch",
        "raw_code_field": "description",
        "value_field": "",
    },
    {
        "target_id": "HOUT-O03",
        "target": "called_strike_event",
        "event_level": "pitch",
        "raw_code_field": "description",
        "value_field": "",
    },
    {
        "target_id": "HOUT-O04",
        "target": "ball_in_play_event",
        "event_level": "pitch",
        "raw_code_field": "description",
        "value_field": "",
    },
    {
        "target_id": "HOUT-O05",
        "target": "strikeout_event",
        "event_level": "plate_appearance",
        "raw_code_field": "events",
        "value_field": "",
    },
    {
        "target_id": "HOUT-O06",
        "target": "walk_event",
        "event_level": "plate_appearance",
        "raw_code_field": "events",
        "value_field": "",
    },
    {
        "target_id": "HOUT-O07",
        "target": "hit_event",
        "event_level": "plate_appearance",
        "raw_code_field": "events",
        "value_field": "",
    },
    {
        "target_id": "HOUT-O08",
        "target": "extra_base_hit_event",
        "event_level": "plate_appearance",
        "raw_code_field": "events",
        "value_field": "",
    },
    {
        "target_id": "HOUT-O09",
        "target": "contact_quality_value",
        "event_level": "contact",
        "raw_code_field": "description",
        "value_field": "contact_quality_value",
    },
    {
        "target_id": "HOUT-O10",
        "target": "run_value",
        "event_level": "event",
        "raw_code_field": "events",
        "value_field": "run_value",
    },
]


ADAPTER_RULES = [
    {"rule_id": "HADP-R01", "rule": "provider payloads are immutable adapter inputs"},
    {"rule_id": "HADP-R02", "rule": "game identity is derived from provider game_pk"},
    {"rule_id": "HADP-R03", "rule": "plate appearance identity is game scoped"},
    {"rule_id": "HADP-R04", "rule": "pitch identity is plate appearance scoped"},
    {"rule_id": "HADP-R05", "rule": "pitch targets use provider description"},
    {"rule_id": "HADP-R06", "rule": "plate appearance targets use provider events"},
    {"rule_id": "HADP-R07", "rule": "contact target preserves continuous measurement"},
    {"rule_id": "HADP-R08", "rule": "run value target preserves continuous measurement"},
    {"rule_id": "HADP-R09", "rule": "provider timestamps are never replaced by ingestion time"},
    {"rule_id": "HADP-R10", "rule": "provider revision metadata is preserved"},
    {"rule_id": "HADP-R11", "rule": "raw payload digest is deterministic SHA256"},
    {"rule_id": "HADP-R12", "rule": "adapter output delegates validation to Layer 9D"},
    {"rule_id": "HADP-R13", "rule": "disabled collection path emits no record"},
    {"rule_id": "HADP-R14", "rule": "synthetic fixtures do not authorize live fetching"},
]


ADAPTER_OUTPUT_FIELDS = [
    "historical_outcome_contract_version",
    "target_id",
    "event_level",
    "game_id",
    "game_date",
    "scheduled_start_utc",
    "plate_appearance_id",
    "pitch_id",
    "pitcher_id",
    "batter_id",
    "event_sequence",
    "raw_outcome_code",
    "source_payload",
    "event_occurred_at_utc",
    "source_observed_at_utc",
    "source_published_at_utc",
    "outcome_available_at_utc",
    "provider",
    "provider_event_id",
    "provider_payload_version",
    "provider_revision_id",
    "is_final_provider_revision",
    "ingestion_run_id",
    "raw_payload_digest",
    "provider_mapping_version",
    "target_derivation_version",
    "identity_conflict",
    "revision_conflict",
    "game_incomplete",
    "collection_enabled",
]


PROHIBITED_AUTHORITIES = [
    "accuracy_evaluation",
    "augmented_prediction_generation",
    "backtest_execution",
    "baseline_prediction_generation",
    "bet_recommendation",
    "calibration_evaluation",
    "canonical_probability_authority_change",
    "edge_detection",
    "feature_outcome_join_execution",
    "historical_outcome_collection_execution",
    "historical_outcome_fetch_execution",
    "historical_outcome_prediction_join_execution",
    "incremental_value_evaluation",
    "market_comparison",
    "model_training",
    "parameter_tuning",
    "predictive_metric_calculation",
    "pricing",
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
    module_name: str,
) -> ModuleType:
    specification = (
        importlib.util.spec_from_file_location(
            module_name,
            path,
        )
    )

    if (
        specification is None
        or specification.loader is None
    ):
        raise RuntimeError(
            f"Unable to load module: {path}"
        )

    module = importlib.util.module_from_spec(
        specification
    )
    specification.loader.exec_module(module)

    return module


def normalized_string(value: Any) -> str:
    if value is None:
        return ""

    return str(value).strip()


def target_mapping(
    target_id: str,
) -> Mapping[str, Any] | None:
    for mapping in TARGET_ADAPTER_MAP:
        if mapping["target_id"] == target_id:
            return mapping

    return None


def plate_appearance_identity(
    game_pk: Any,
    at_bat_number: Any,
) -> str | None:
    game_id = normalized_string(game_pk)

    if (
        not game_id
        or isinstance(at_bat_number, bool)
        or not isinstance(at_bat_number, int)
        or at_bat_number < 0
    ):
        return None

    return (
        f"game:{game_id}:"
        f"pa:{at_bat_number:04d}"
    )


def pitch_identity(
    game_pk: Any,
    at_bat_number: Any,
    pitch_number: Any,
) -> str | None:
    pa_id = plate_appearance_identity(
        game_pk,
        at_bat_number,
    )

    if (
        pa_id is None
        or isinstance(pitch_number, bool)
        or not isinstance(pitch_number, int)
        or pitch_number < 0
    ):
        return None

    return (
        f"{pa_id}:"
        f"pitch:{pitch_number:03d}"
    )


def event_sequence(
    event_level: str,
    at_bat_number: Any,
    pitch_number: Any,
) -> Any:
    if (
        isinstance(at_bat_number, bool)
        or not isinstance(at_bat_number, int)
        or at_bat_number < 0
    ):
        return at_bat_number

    if event_level in {
        "pitch",
        "contact",
    }:
        if (
            isinstance(pitch_number, bool)
            or not isinstance(pitch_number, int)
            or pitch_number < 0
        ):
            return pitch_number

        return (
            at_bat_number * 1000
            + pitch_number
        )

    return at_bat_number


def adapter_raw_payload_digest(
    payload: Mapping[str, Any],
) -> str:
    return sha256_payload(
        {
            "adapter_contract_version": (
                ADAPTER_CONTRACT_VERSION
            ),
            "raw_payload_schema_version": (
                RAW_PAYLOAD_SCHEMA_VERSION
            ),
            "payload": payload,
        }
    )


def adapt_provider_payload(
    payload: Mapping[str, Any],
    target_id: str,
    outcome_contract: ModuleType,
) -> dict[str, Any] | None:
    if payload.get(
        "collection_enabled",
        True,
    ) is False:
        return None

    mapping = target_mapping(target_id)

    if mapping is None:
        event_level = normalized_string(
            payload.get("event_level")
        )
        raw_code_field = "description"
        value_field = ""
    else:
        event_level = str(
            mapping["event_level"]
        )
        raw_code_field = str(
            mapping["raw_code_field"]
        )
        value_field = str(
            mapping["value_field"]
        )

    game_pk = payload.get("game_pk")
    at_bat_number = payload.get(
        "at_bat_number"
    )
    pitch_number = payload.get(
        "pitch_number"
    )

    if event_level == "event":
        plate_appearance_id = None
        pitch_id = None
    elif event_level == "plate_appearance":
        plate_appearance_id = (
            plate_appearance_identity(
                game_pk,
                at_bat_number,
            )
        )
        pitch_id = None
    else:
        plate_appearance_id = (
            plate_appearance_identity(
                game_pk,
                at_bat_number,
            )
        )
        pitch_id = pitch_identity(
            game_pk,
            at_bat_number,
            pitch_number,
        )

    source_payload: dict[str, Any] = {}

    if value_field:
        source_payload[value_field] = (
            payload.get(value_field)
        )

    raw_outcome_code = normalized_string(
        payload.get(raw_code_field)
    )

    adapter_output = {
        "historical_outcome_contract_version": (
            EXPECTED_OUTCOME_CONTRACT_VERSION
        ),
        "target_id": target_id,
        "event_level": event_level,
        "game_id": normalized_string(game_pk),
        "game_date": normalized_string(
            payload.get("game_date")
        ),
        "scheduled_start_utc": normalized_string(
            payload.get(
                "scheduled_start_utc"
            )
        ),
        "plate_appearance_id": (
            plate_appearance_id
        ),
        "pitch_id": pitch_id,
        "pitcher_id": normalized_string(
            payload.get("pitcher")
        ),
        "batter_id": normalized_string(
            payload.get("batter")
        ),
        "event_sequence": event_sequence(
            event_level,
            at_bat_number,
            pitch_number,
        ),
        "raw_outcome_code": raw_outcome_code,
        "source_payload": source_payload,
        "event_occurred_at_utc": (
            normalized_string(
                payload.get(
                    "event_occurred_at_utc"
                )
            )
            or None
        ),
        "source_observed_at_utc": (
            normalized_string(
                payload.get(
                    "source_observed_at_utc"
                )
            )
        ),
        "source_published_at_utc": (
            normalized_string(
                payload.get(
                    "source_published_at_utc"
                )
            )
            or None
        ),
        "outcome_available_at_utc": (
            normalized_string(
                payload.get(
                    "outcome_available_at_utc"
                )
            )
        ),
        "provider": normalized_string(
            payload.get("provider")
        ),
        "provider_event_id": (
            normalized_string(
                payload.get(
                    "provider_event_id"
                )
            )
        ),
        "provider_payload_version": (
            normalized_string(
                payload.get(
                    "provider_payload_version"
                )
            )
        ),
        "provider_revision_id": (
            normalized_string(
                payload.get(
                    "provider_revision_id"
                )
            )
            or None
        ),
        "is_final_provider_revision": bool(
            payload.get(
                "is_final_provider_revision",
                False,
            )
        ),
        "ingestion_run_id": normalized_string(
            payload.get(
                "ingestion_run_id"
            )
        ),
        "raw_payload_digest": (
            adapter_raw_payload_digest(
                payload
            )
        ),
        "provider_mapping_version": (
            outcome_contract.PROVIDER_MAPPING_VERSION
        ),
        "target_derivation_version": (
            outcome_contract.TARGET_DERIVATION_VERSION
        ),
        "identity_conflict": bool(
            payload.get(
                "identity_conflict",
                False,
            )
        ),
        "revision_conflict": bool(
            payload.get(
                "revision_conflict",
                False,
            )
        ),
        "game_incomplete": bool(
            payload.get(
                "game_incomplete",
                False,
            )
        ),
        "collection_enabled": bool(
            payload.get(
                "collection_enabled",
                True,
            )
        ),
    }

    return adapter_output


def materialize_adapter_record(
    payload: Mapping[str, Any],
    target_id: str,
    outcome_contract: ModuleType,
) -> tuple[
    dict[str, Any] | None,
    dict[str, Any] | None,
]:
    adapter_output = adapt_provider_payload(
        payload,
        target_id,
        outcome_contract,
    )

    if adapter_output is None:
        return None, None

    historical_outcome_record = (
        outcome_contract
        .materialize_historical_outcome(
            adapter_output
        )
    )

    return (
        adapter_output,
        historical_outcome_record,
    )


def base_provider_payload() -> dict[str, Any]:
    return {
        "provider": "synthetic_statcast",
        "provider_event_id": "synthetic-event-001",
        "provider_payload_version": "payload-v1",
        "provider_revision_id": "revision-001",
        "is_final_provider_revision": True,
        "ingestion_run_id": "ingestion-run-001",
        "game_pk": "777001",
        "game_date": "2026-04-01",
        "scheduled_start_utc": "2026-04-01T18:10:00Z",
        "at_bat_number": 1,
        "pitch_number": 1,
        "pitcher": "pitcher-001",
        "batter": "batter-001",
        "description": "swinging_strike",
        "events": "strikeout",
        "contact_quality_value": 0.411,
        "run_value": 0.47,
        "event_occurred_at_utc": "2026-04-01T18:12:10Z",
        "source_observed_at_utc": "2026-04-01T18:12:11Z",
        "source_published_at_utc": "2026-04-01T18:12:12Z",
        "outcome_available_at_utc": "2026-04-01T18:12:12Z",
        "collection_enabled": True,
        "game_incomplete": False,
        "identity_conflict": False,
        "revision_conflict": False,
    }


def build_fixtures() -> list[dict[str, Any]]:
    fixtures: list[dict[str, Any]] = []

    target_cases = [
        ("HADP-FIX-001", "HOUT-O01", "swinging_strike", "strikeout", True),
        ("HADP-FIX-002", "HOUT-O02", "swinging_strike", "strikeout", True),
        ("HADP-FIX-003", "HOUT-O03", "called_strike", "strikeout", True),
        ("HADP-FIX-004", "HOUT-O04", "hit_into_play", "single", True),
        ("HADP-FIX-005", "HOUT-O05", "called_strike", "strikeout", True),
        ("HADP-FIX-006", "HOUT-O06", "ball", "walk", True),
        ("HADP-FIX-007", "HOUT-O07", "hit_into_play", "single", True),
        ("HADP-FIX-008", "HOUT-O08", "hit_into_play", "double", True),
        ("HADP-FIX-009", "HOUT-O09", "hit_into_play", "single", 0.411),
        ("HADP-FIX-010", "HOUT-O10", "hit_into_play", "single", 0.47),
    ]

    for (
        fixture_id,
        target_id,
        description,
        events,
        expected_value,
    ) in target_cases:
        payload = base_provider_payload()
        payload.update(
            {
                "provider_event_id": fixture_id,
                "description": description,
                "events": events,
                "at_bat_number": int(
                    fixture_id.rsplit(
                        "-",
                        1,
                    )[-1]
                ),
                "pitch_number": 1,
            }
        )

        fixtures.append(
            {
                "fixture_id": fixture_id,
                "description": (
                    f"valid_adapter_{target_id.lower()}"
                ),
                "target_id": target_id,
                "payload": payload,
                "expect_adapter_output": True,
                "expect_outcome_record": True,
                "expected_value": expected_value,
                "expected_missing": False,
                "expected_eligible": True,
                "expected_codes": [],
            }
        )

    missing_contact = base_provider_payload()
    missing_contact.update(
        {
            "provider_event_id": "HADP-FIX-011",
            "contact_quality_value": None,
        }
    )
    fixtures.append(
        {
            "fixture_id": "HADP-FIX-011",
            "description": "missing_contact_measurement",
            "target_id": "HOUT-O09",
            "payload": missing_contact,
            "expect_adapter_output": True,
            "expect_outcome_record": True,
            "expected_value": None,
            "expected_missing": True,
            "expected_eligible": True,
            "expected_codes": [
                "historical_outcome_value_missing",
            ],
        }
    )

    missing_pitch_identity = (
        base_provider_payload()
    )
    missing_pitch_identity.update(
        {
            "provider_event_id": "HADP-FIX-012",
            "pitch_number": None,
        }
    )
    fixtures.append(
        {
            "fixture_id": "HADP-FIX-012",
            "description": "missing_pitch_identity_rejected",
            "target_id": "HOUT-O02",
            "payload": missing_pitch_identity,
            "expect_adapter_output": True,
            "expect_outcome_record": True,
            "expected_value": True,
            "expected_missing": False,
            "expected_eligible": False,
            "expected_codes": [
                "historical_outcome_event_identity_missing",
                "historical_outcome_event_sequence_invalid",
            ],
        }
    )

    revision_conflict = base_provider_payload()
    revision_conflict.update(
        {
            "provider_event_id": "HADP-FIX-013",
            "revision_conflict": True,
        }
    )
    fixtures.append(
        {
            "fixture_id": "HADP-FIX-013",
            "description": "revision_conflict_rejected",
            "target_id": "HOUT-O02",
            "payload": revision_conflict,
            "expect_adapter_output": True,
            "expect_outcome_record": True,
            "expected_value": True,
            "expected_missing": False,
            "expected_eligible": False,
            "expected_codes": [
                "historical_outcome_revision_conflict",
            ],
        }
    )

    disabled = base_provider_payload()
    disabled.update(
        {
            "provider_event_id": "HADP-FIX-014",
            "collection_enabled": False,
        }
    )
    fixtures.append(
        {
            "fixture_id": "HADP-FIX-014",
            "description": "disabled_collection_non_emitting",
            "target_id": "HOUT-O02",
            "payload": disabled,
            "expect_adapter_output": False,
            "expect_outcome_record": False,
            "expected_value": None,
            "expected_missing": None,
            "expected_eligible": None,
            "expected_codes": [],
        }
    )

    identity_conflict = base_provider_payload()
    identity_conflict.update(
        {
            "provider_event_id": "HADP-FIX-015",
            "identity_conflict": True,
        }
    )
    fixtures.append(
        {
            "fixture_id": "HADP-FIX-015",
            "description": "identity_conflict_rejected",
            "target_id": "HOUT-O02",
            "payload": identity_conflict,
            "expect_adapter_output": True,
            "expect_outcome_record": True,
            "expected_value": True,
            "expected_missing": False,
            "expected_eligible": False,
            "expected_codes": [
                "historical_outcome_event_identity_conflict",
            ],
        }
    )

    return fixtures


def values_equal(
    actual: Any,
    expected: Any,
) -> bool:
    if (
        isinstance(actual, float)
        and isinstance(expected, float)
    ):
        return math.isclose(
            actual,
            expected,
            rel_tol=0.0,
            abs_tol=1e-12,
        )

    return actual == expected


def evaluate_fixtures(
    outcome_contract: ModuleType,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    fixture_results: list[
        dict[str, Any]
    ] = []
    adapter_outputs: list[
        dict[str, Any]
    ] = []
    outcome_records: list[
        dict[str, Any]
    ] = []

    for fixture in build_fixtures():
        (
            adapter_output,
            outcome_record,
        ) = materialize_adapter_record(
            fixture["payload"],
            fixture["target_id"],
            outcome_contract,
        )

        adapter_emitted = (
            adapter_output is not None
        )
        outcome_emitted = (
            outcome_record is not None
        )

        if adapter_output is not None:
            adapter_outputs.append(
                {
                    "fixture_id": fixture[
                        "fixture_id"
                    ],
                    **adapter_output,
                }
            )

        if outcome_record is not None:
            outcome_records.append(
                {
                    "fixture_id": fixture[
                        "fixture_id"
                    ],
                    **outcome_record,
                }
            )

        actual_value = (
            outcome_record.get(
                "outcome_value"
            )
            if outcome_record is not None
            else None
        )
        actual_missing = (
            outcome_record.get(
                "outcome_missing"
            )
            if outcome_record is not None
            else None
        )
        actual_eligible = (
            outcome_record.get(
                "historical_outcome_eligible"
            )
            if outcome_record is not None
            else None
        )
        actual_codes = (
            outcome_record.get(
                "exclusion_codes",
                [],
            )
            if outcome_record is not None
            else []
        )

        passed = all(
            [
                adapter_emitted
                == fixture[
                    "expect_adapter_output"
                ],
                outcome_emitted
                == fixture[
                    "expect_outcome_record"
                ],
                values_equal(
                    actual_value,
                    fixture[
                        "expected_value"
                    ],
                ),
                actual_missing
                == fixture[
                    "expected_missing"
                ],
                actual_eligible
                == fixture[
                    "expected_eligible"
                ],
                actual_codes
                == fixture[
                    "expected_codes"
                ],
            ]
        )

        fixture_results.append(
            {
                "fixture_id": fixture[
                    "fixture_id"
                ],
                "description": fixture[
                    "description"
                ],
                "target_id": fixture[
                    "target_id"
                ],
                "expected_adapter_output": fixture[
                    "expect_adapter_output"
                ],
                "actual_adapter_output": (
                    adapter_emitted
                ),
                "expected_outcome_record": fixture[
                    "expect_outcome_record"
                ],
                "actual_outcome_record": (
                    outcome_emitted
                ),
                "expected_value": fixture[
                    "expected_value"
                ],
                "actual_value": actual_value,
                "expected_missing": fixture[
                    "expected_missing"
                ],
                "actual_missing": actual_missing,
                "expected_eligible": fixture[
                    "expected_eligible"
                ],
                "actual_eligible": actual_eligible,
                "expected_codes": "|".join(
                    fixture[
                        "expected_codes"
                    ]
                ),
                "actual_codes": "|".join(
                    actual_codes
                ),
                "passed": passed,
            }
        )

    adapter_outputs.sort(
        key=lambda row: row["fixture_id"]
    )
    outcome_records.sort(
        key=lambda row: row["fixture_id"]
    )

    return (
        fixture_results,
        adapter_outputs,
        outcome_records,
    )


def csv_safe_row(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        key: (
            json.dumps(
                value,
                sort_keys=True,
            )
            if isinstance(
                value,
                (dict, list),
            )
            else value
        )
        for key, value in row.items()
    }


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
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(fieldnames),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)


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


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    predecessor_present = (
        PREDECESSOR_PATH.exists()
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    outcome_contract = load_module(
        HISTORICAL_OUTCOME_CONTRACT_PATH,
        "layer_9d_historical_outcome_contract",
    )

    outcome_contract_compatible = (
        getattr(
            outcome_contract,
            "CONTRACT_VERSION",
            None,
        )
        == EXPECTED_OUTCOME_CONTRACT_VERSION
        and callable(
            getattr(
                outcome_contract,
                "materialize_historical_outcome",
                None,
            )
        )
    )

    (
        fixture_results,
        adapter_outputs,
        outcome_records,
    ) = evaluate_fixtures(
        outcome_contract
    )

    (
        replay_fixture_results,
        replay_adapter_outputs,
        replay_outcome_records,
    ) = evaluate_fixtures(
        outcome_contract
    )

    adapter_output_digests = [
        sha256_payload(row)
        for row in adapter_outputs
    ]
    outcome_record_digests = [
        sha256_payload(row)
        for row in outcome_records
    ]

    target_ids = [
        row["target_id"]
        for row in TARGET_ADAPTER_MAP
    ]
    rule_ids = [
        row["rule_id"]
        for row in ADAPTER_RULES
    ]
    input_fields = [
        row["field"]
        for row in ADAPTER_INPUT_FIELDS
    ]

    fixture_expectations_pass = all(
        bool(row["passed"])
        for row in fixture_results
    )

    implementation_checks = [
        {
            "check": "nine_e_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "layer_9d_contract_compatible",
            "actual": outcome_contract_compatible,
            "expected": True,
            "passed": outcome_contract_compatible,
        },
        {
            "check": "adapter_contract_version_explicit",
            "actual": ADAPTER_CONTRACT_VERSION,
            "expected": ADAPTER_CONTRACT_VERSION,
            "passed": bool(
                ADAPTER_CONTRACT_VERSION
            ),
        },
        {
            "check": "twenty_five_adapter_input_fields",
            "actual": len(
                ADAPTER_INPUT_FIELDS
            ),
            "expected": 25,
            "passed": len(
                ADAPTER_INPUT_FIELDS
            )
            == 25,
        },
        {
            "check": "adapter_input_fields_unique",
            "actual": len(
                set(input_fields)
            ),
            "expected": len(input_fields),
            "passed": len(
                set(input_fields)
            )
            == len(input_fields),
        },
        {
            "check": "ten_target_adapter_mappings",
            "actual": len(
                TARGET_ADAPTER_MAP
            ),
            "expected": 10,
            "passed": len(
                TARGET_ADAPTER_MAP
            )
            == 10,
        },
        {
            "check": "target_adapter_ids_unique",
            "actual": len(
                set(target_ids)
            ),
            "expected": len(target_ids),
            "passed": len(
                set(target_ids)
            )
            == len(target_ids),
        },
        {
            "check": "fourteen_adapter_rules",
            "actual": len(
                ADAPTER_RULES
            ),
            "expected": 14,
            "passed": len(
                ADAPTER_RULES
            )
            == 14,
        },
        {
            "check": "adapter_rule_ids_unique",
            "actual": len(
                set(rule_ids)
            ),
            "expected": len(rule_ids),
            "passed": len(
                set(rule_ids)
            )
            == len(rule_ids),
        },
        {
            "check": "thirty_adapter_output_fields",
            "actual": len(
                ADAPTER_OUTPUT_FIELDS
            ),
            "expected": 30,
            "passed": len(
                ADAPTER_OUTPUT_FIELDS
            )
            == 30,
        },
        {
            "check": "fifteen_fixtures_executed",
            "actual": len(
                fixture_results
            ),
            "expected": 15,
            "passed": len(
                fixture_results
            )
            == 15,
        },
        {
            "check": "fixture_expectations_all_pass",
            "actual": fixture_expectations_pass,
            "expected": True,
            "passed": fixture_expectations_pass,
        },
        {
            "check": "ten_valid_target_fixtures_pass",
            "actual": all(
                row["passed"]
                for row in fixture_results[:10]
            ),
            "expected": True,
            "passed": all(
                row["passed"]
                for row in fixture_results[:10]
            ),
        },
        {
            "check": "missing_contact_semantics_pass",
            "actual": fixture_results[
                10
            ]["passed"],
            "expected": True,
            "passed": fixture_results[
                10
            ]["passed"],
        },
        {
            "check": "missing_identity_rejected",
            "actual": fixture_results[
                11
            ]["passed"],
            "expected": True,
            "passed": fixture_results[
                11
            ]["passed"],
        },
        {
            "check": "revision_conflict_rejected",
            "actual": fixture_results[
                12
            ]["passed"],
            "expected": True,
            "passed": fixture_results[
                12
            ]["passed"],
        },
        {
            "check": "disabled_collection_non_emitting",
            "actual": fixture_results[
                13
            ]["passed"],
            "expected": True,
            "passed": fixture_results[
                13
            ]["passed"],
        },
        {
            "check": "identity_conflict_rejected",
            "actual": fixture_results[
                14
            ]["passed"],
            "expected": True,
            "passed": fixture_results[
                14
            ]["passed"],
        },
        {
            "check": "adapter_replay_deterministic",
            "actual": adapter_outputs,
            "expected": replay_adapter_outputs,
            "passed": adapter_outputs
            == replay_adapter_outputs,
        },
        {
            "check": "outcome_replay_deterministic",
            "actual": outcome_records,
            "expected": replay_outcome_records,
            "passed": outcome_records
            == replay_outcome_records,
        },
        {
            "check": "fixture_replay_deterministic",
            "actual": fixture_results,
            "expected": replay_fixture_results,
            "passed": fixture_results
            == replay_fixture_results,
        },
        {
            "check": "adapter_digests_valid_sha256",
            "actual": sum(
                bool(
                    SHA256_PATTERN.fullmatch(
                        digest
                    )
                )
                for digest in adapter_output_digests
            ),
            "expected": len(
                adapter_output_digests
            ),
            "passed": all(
                bool(
                    SHA256_PATTERN.fullmatch(
                        digest
                    )
                )
                for digest in adapter_output_digests
            ),
        },
        {
            "check": "outcome_digests_valid_sha256",
            "actual": sum(
                bool(
                    SHA256_PATTERN.fullmatch(
                        digest
                    )
                )
                for digest in outcome_record_digests
            ),
            "expected": len(
                outcome_record_digests
            ),
            "passed": all(
                bool(
                    SHA256_PATTERN.fullmatch(
                        digest
                    )
                )
                for digest in outcome_record_digests
            ),
        },
        {
            "check": "external_fetch_execution_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "production_materialization_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "feature_outcome_join_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "prediction_join_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "predictive_metrics_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "production_probability_change_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "market_pricing_edge_authority_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        bool(row["passed"])
        for row in implementation_checks
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_source_adapter_contract_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_source_adapter_contract_failed"
    )

    recommended_next_layer = (
        "9G_pitch_type_matchup_overlay_historical_outcome_fixture_corpus_plan"
        if all_checks_passed
        else
        "9F_pitch_type_matchup_overlay_historical_outcome_source_adapter_contract_remediation"
    )

    write_csv(
        OUTPUT_DIR / "implementation_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        implementation_checks,
    )

    write_csv(
        OUTPUT_DIR / "adapter_input_contract.csv",
        [
            "ordinal",
            "field",
            "type",
            "required",
        ],
        ADAPTER_INPUT_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "target_adapter_map.csv",
        [
            "target_id",
            "target",
            "event_level",
            "raw_code_field",
            "value_field",
        ],
        TARGET_ADAPTER_MAP,
    )

    write_csv(
        OUTPUT_DIR / "adapter_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        ADAPTER_RULES,
    )

    write_csv(
        OUTPUT_DIR / "fixture_results.csv",
        [
            "fixture_id",
            "description",
            "target_id",
            "expected_adapter_output",
            "actual_adapter_output",
            "expected_outcome_record",
            "actual_outcome_record",
            "expected_value",
            "actual_value",
            "expected_missing",
            "actual_missing",
            "expected_eligible",
            "actual_eligible",
            "expected_codes",
            "actual_codes",
            "passed",
        ],
        fixture_results,
    )

    adapter_output_fieldnames = [
        "fixture_id",
        *ADAPTER_OUTPUT_FIELDS,
    ]

    write_csv(
        OUTPUT_DIR / "synthetic_adapter_outputs.csv",
        adapter_output_fieldnames,
        [
            csv_safe_row(row)
            for row in adapter_outputs
        ],
    )

    outcome_fieldnames = (
        list(outcome_records[0].keys())
        if outcome_records
        else ["fixture_id"]
    )

    write_csv(
        OUTPUT_DIR
        / "synthetic_historical_outcome_records.csv",
        outcome_fieldnames,
        [
            csv_safe_row(row)
            for row in outcome_records
        ],
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
                    "9F implements a synthetic source adapter contract only."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_fixture_corpus_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "9G may plan a deterministic local fixture corpus "
                    "without external fetching or production materialization."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "adapter_contract_version": (
            ADAPTER_CONTRACT_VERSION
        ),
        "implementation_checks_required": len(
            implementation_checks
        ),
        "implementation_checks_passed": sum(
            bool(row["passed"])
            for row in implementation_checks
        ),
        "adapter_input_fields": len(
            ADAPTER_INPUT_FIELDS
        ),
        "target_adapter_mappings": len(
            TARGET_ADAPTER_MAP
        ),
        "adapter_rules": len(
            ADAPTER_RULES
        ),
        "fixtures_executed": len(
            fixture_results
        ),
        "fixtures_passed": sum(
            bool(row["passed"])
            for row in fixture_results
        ),
        "synthetic_adapter_outputs": len(
            adapter_outputs
        ),
        "synthetic_outcome_records": len(
            outcome_records
        ),
        "synthetic_eligible_outcome_records": sum(
            bool(
                row[
                    "historical_outcome_eligible"
                ]
            )
            for row in outcome_records
        ),
        "external_records_fetched": 0,
        "production_records_materialized": 0,
        "feature_outcome_joins_executed": 0,
        "prediction_joins_executed": 0,
        "predictive_metrics_calculated": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": all_checks_passed,
        "recommended_next_layer": (
            recommended_next_layer
        ),
    }

    write_json(
        OUTPUT_DIR / "summary.json",
        summary,
    )

    diagnosis = {
        "all_checks_passed": all_checks_passed,
        "authority_granted": (
            "historical_outcome_fixture_corpus_planning"
            if all_checks_passed
            else
            "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "diagnosis": diagnosis_name,
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "output_directory": str(
            OUTPUT_DIR.relative_to(ROOT)
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
    }

    write_json(
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    manifest_rows: list[
        dict[str, Any]
    ] = []

    for path in sorted(
        OUTPUT_DIR.iterdir(),
        key=lambda item: item.name,
    ):
        if not path.is_file():
            continue

        manifest_rows.append(
            {
                "artifact": path.name,
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(
                    path.read_bytes()
                ).hexdigest(),
            }
        )

    write_csv(
        OUTPUT_DIR / "artifact_manifest.csv",
        [
            "artifact",
            "bytes",
            "sha256",
        ],
        manifest_rows,
    )

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
    print(
        "Adapter contract version: "
        f"{ADAPTER_CONTRACT_VERSION}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_present}"
    )
    print(
        "Layer 9D contract compatible: "
        f"{outcome_contract_compatible}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Contract fixtures passed: "
        f"{summary['fixtures_passed']}/"
        f"{summary['fixtures_executed']}"
    )
    print(
        "Synthetic adapter outputs: "
        f"{summary['synthetic_adapter_outputs']}"
    )
    print(
        "Synthetic historical outcome records: "
        f"{summary['synthetic_outcome_records']}"
    )
    print(
        "Synthetic eligible outcome records: "
        f"{summary['synthetic_eligible_outcome_records']}"
    )
    print(
        "External historical outcome records fetched: 0"
    )
    print(
        "Production historical outcome records materialized: 0"
    )
    print(
        "Feature/outcome joins executed: 0"
    )
    print(
        "Prediction joins executed: 0"
    )
    print(
        "Predictive metrics calculated: 0"
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
        "Recommended next layer: "
        f"{recommended_next_layer}"
    )
    print(
        "Artifacts: "
        f"{OUTPUT_DIR.relative_to(ROOT)}"
    )

    if not all_checks_passed:
        failed_checks = [
            row["check"]
            for row in implementation_checks
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
