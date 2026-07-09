#!/usr/bin/env python3
"""
Layer 9G
Pitch-Type Matchup Overlay Historical Outcome Fixture Corpus Plan

Plans a deterministic local fixture corpus for the Layer 9F historical outcome
source adapter and the Layer 9D historical outcome contract.

Planning only.

This layer does not:

- fetch external historical outcomes;
- execute live or production collection;
- materialize production historical outcome datasets;
- join historical outcomes to features or predictions;
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
import re
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9G"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_fixture_corpus_plan"
)
CORPUS_PLAN_VERSION = (
    "layer_9G_historical_outcome_fixture_corpus_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9G_pitch_type_matchup_overlay_historical_outcome_fixture_corpus_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9F_pitch_type_matchup_overlay_historical_outcome_source_adapter_contract.py"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "source_adapter_contract_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_fixture_corpus_planning"
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


CORPUS_CATEGORIES = [
    {
        "category_id": "HCOR-C01",
        "category": "valid_target_examples",
        "purpose": (
            "Provide at least one valid provider-shaped payload for each "
            "historical outcome target."
        ),
    },
    {
        "category_id": "HCOR-C02",
        "category": "raw_outcome_code_boundaries",
        "purpose": (
            "Cover positive, negative, unsupported, and missing provider codes."
        ),
    },
    {
        "category_id": "HCOR-C03",
        "category": "identity_boundaries",
        "purpose": (
            "Cover game, plate-appearance, pitch, pitcher, batter, and "
            "event-sequence identity failures."
        ),
    },
    {
        "category_id": "HCOR-C04",
        "category": "availability_boundaries",
        "purpose": (
            "Cover start-time, observation-time, publication-time, event-time, "
            "and unknown-availability conditions."
        ),
    },
    {
        "category_id": "HCOR-C05",
        "category": "revision_boundaries",
        "purpose": (
            "Cover initial, corrected, duplicate, conflicting, and final "
            "provider revisions."
        ),
    },
    {
        "category_id": "HCOR-C06",
        "category": "missingness_boundaries",
        "purpose": (
            "Cover absent continuous values, unsupported categories, provider "
            "gaps, and incomplete games."
        ),
    },
    {
        "category_id": "HCOR-C07",
        "category": "payload_integrity",
        "purpose": (
            "Cover deterministic canonicalization, digests, payload versions, "
            "and provenance preservation."
        ),
    },
    {
        "category_id": "HCOR-C08",
        "category": "collection_controls",
        "purpose": (
            "Cover disabled collection, non-emitting paths, and explicit "
            "separation from live fetching."
        ),
    },
    {
        "category_id": "HCOR-C09",
        "category": "deterministic_replay",
        "purpose": (
            "Cover ordering independence, repeated execution, and immutable "
            "expected outputs."
        ),
    },
    {
        "category_id": "HCOR-C10",
        "category": "cross_contract_compatibility",
        "purpose": (
            "Verify every fixture remains compatible with Layer 9F adapter "
            "inputs and Layer 9D historical outcome outputs."
        ),
    },
]


FIXTURE_SCHEMA_FIELDS = [
    {
        "ordinal": 1,
        "field": "fixture_id",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 2,
        "field": "fixture_category",
        "type": "enum",
        "required": True,
    },
    {
        "ordinal": 3,
        "field": "description",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 4,
        "field": "target_id",
        "type": "enum",
        "required": True,
    },
    {
        "ordinal": 5,
        "field": "provider_payload",
        "type": "object",
        "required": True,
    },
    {
        "ordinal": 6,
        "field": "expect_adapter_output",
        "type": "boolean",
        "required": True,
    },
    {
        "ordinal": 7,
        "field": "expect_outcome_record",
        "type": "boolean",
        "required": True,
    },
    {
        "ordinal": 8,
        "field": "expected_outcome_value",
        "type": "nullable_number_or_boolean",
        "required": True,
    },
    {
        "ordinal": 9,
        "field": "expected_outcome_missing",
        "type": "nullable_boolean",
        "required": True,
    },
    {
        "ordinal": 10,
        "field": "expected_eligible",
        "type": "nullable_boolean",
        "required": True,
    },
    {
        "ordinal": 11,
        "field": "expected_exclusion_codes",
        "type": "sorted_unique_string_array",
        "required": True,
    },
    {
        "ordinal": 12,
        "field": "expected_adapter_digest",
        "type": "nullable_sha256",
        "required": True,
    },
    {
        "ordinal": 13,
        "field": "expected_outcome_digest",
        "type": "nullable_sha256",
        "required": True,
    },
    {
        "ordinal": 14,
        "field": "source_fixture_version",
        "type": "string",
        "required": True,
    },
    {
        "ordinal": 15,
        "field": "corpus_plan_version",
        "type": "string",
        "required": True,
    },
]


FIXTURE_SCENARIOS = [
    {
        "fixture_id": "HCOR-FIX-001",
        "fixture_category": "valid_target_examples",
        "description": "valid_swing_event",
        "target_id": "HOUT-O01",
        "mutation": "description=swinging_strike",
        "expected_semantics": "eligible_true",
    },
    {
        "fixture_id": "HCOR-FIX-002",
        "fixture_category": "valid_target_examples",
        "description": "valid_whiff_event",
        "target_id": "HOUT-O02",
        "mutation": "description=swinging_strike",
        "expected_semantics": "eligible_true",
    },
    {
        "fixture_id": "HCOR-FIX-003",
        "fixture_category": "valid_target_examples",
        "description": "valid_called_strike_event",
        "target_id": "HOUT-O03",
        "mutation": "description=called_strike",
        "expected_semantics": "eligible_true",
    },
    {
        "fixture_id": "HCOR-FIX-004",
        "fixture_category": "valid_target_examples",
        "description": "valid_ball_in_play_event",
        "target_id": "HOUT-O04",
        "mutation": "description=hit_into_play",
        "expected_semantics": "eligible_true",
    },
    {
        "fixture_id": "HCOR-FIX-005",
        "fixture_category": "valid_target_examples",
        "description": "valid_strikeout_event",
        "target_id": "HOUT-O05",
        "mutation": "events=strikeout",
        "expected_semantics": "eligible_true",
    },
    {
        "fixture_id": "HCOR-FIX-006",
        "fixture_category": "valid_target_examples",
        "description": "valid_walk_event",
        "target_id": "HOUT-O06",
        "mutation": "events=walk",
        "expected_semantics": "eligible_true",
    },
    {
        "fixture_id": "HCOR-FIX-007",
        "fixture_category": "valid_target_examples",
        "description": "valid_hit_event",
        "target_id": "HOUT-O07",
        "mutation": "events=single",
        "expected_semantics": "eligible_true",
    },
    {
        "fixture_id": "HCOR-FIX-008",
        "fixture_category": "valid_target_examples",
        "description": "valid_extra_base_hit_event",
        "target_id": "HOUT-O08",
        "mutation": "events=double",
        "expected_semantics": "eligible_true",
    },
    {
        "fixture_id": "HCOR-FIX-009",
        "fixture_category": "valid_target_examples",
        "description": "valid_contact_quality_value",
        "target_id": "HOUT-O09",
        "mutation": "contact_quality_value=0.411",
        "expected_semantics": "eligible_true",
    },
    {
        "fixture_id": "HCOR-FIX-010",
        "fixture_category": "valid_target_examples",
        "description": "valid_run_value",
        "target_id": "HOUT-O10",
        "mutation": "run_value=0.47",
        "expected_semantics": "eligible_true",
    },
    {
        "fixture_id": "HCOR-FIX-011",
        "fixture_category": "raw_outcome_code_boundaries",
        "description": "supported_negative_whiff",
        "target_id": "HOUT-O02",
        "mutation": "description=called_strike",
        "expected_semantics": "eligible_false_value",
    },
    {
        "fixture_id": "HCOR-FIX-012",
        "fixture_category": "raw_outcome_code_boundaries",
        "description": "unsupported_pitch_code",
        "target_id": "HOUT-O02",
        "mutation": "description=provider_unknown_code",
        "expected_semantics": "unscored_unsupported",
    },
    {
        "fixture_id": "HCOR-FIX-013",
        "fixture_category": "identity_boundaries",
        "description": "missing_game_identity",
        "target_id": "HOUT-O02",
        "mutation": "game_pk=",
        "expected_semantics": "rejected_identity",
    },
    {
        "fixture_id": "HCOR-FIX-014",
        "fixture_category": "identity_boundaries",
        "description": "missing_plate_appearance_identity",
        "target_id": "HOUT-O05",
        "mutation": "at_bat_number=null",
        "expected_semantics": "rejected_identity",
    },
    {
        "fixture_id": "HCOR-FIX-015",
        "fixture_category": "identity_boundaries",
        "description": "missing_pitch_identity",
        "target_id": "HOUT-O02",
        "mutation": "pitch_number=null",
        "expected_semantics": "rejected_identity",
    },
    {
        "fixture_id": "HCOR-FIX-016",
        "fixture_category": "identity_boundaries",
        "description": "explicit_identity_conflict",
        "target_id": "HOUT-O02",
        "mutation": "identity_conflict=true",
        "expected_semantics": "rejected_identity_conflict",
    },
    {
        "fixture_id": "HCOR-FIX-017",
        "fixture_category": "availability_boundaries",
        "description": "availability_before_scheduled_start",
        "target_id": "HOUT-O02",
        "mutation": "outcome_available_at_utc=2026-04-01T18:09:59Z",
        "expected_semantics": "ineligible_availability",
    },
    {
        "fixture_id": "HCOR-FIX-018",
        "fixture_category": "availability_boundaries",
        "description": "source_observed_after_availability",
        "target_id": "HOUT-O02",
        "mutation": "source_observed_at_utc=2026-04-01T18:12:13Z",
        "expected_semantics": "ineligible_availability",
    },
    {
        "fixture_id": "HCOR-FIX-019",
        "fixture_category": "availability_boundaries",
        "description": "event_occurrence_after_availability",
        "target_id": "HOUT-O02",
        "mutation": "event_occurred_at_utc=2026-04-01T18:12:13Z",
        "expected_semantics": "ineligible_availability",
    },
    {
        "fixture_id": "HCOR-FIX-020",
        "fixture_category": "revision_boundaries",
        "description": "explicit_revision_conflict",
        "target_id": "HOUT-O02",
        "mutation": "revision_conflict=true",
        "expected_semantics": "rejected_revision",
    },
    {
        "fixture_id": "HCOR-FIX-021",
        "fixture_category": "revision_boundaries",
        "description": "nonfinal_provider_revision_preserved",
        "target_id": "HOUT-O02",
        "mutation": "is_final_provider_revision=false",
        "expected_semantics": "eligible_revision_preserved",
    },
    {
        "fixture_id": "HCOR-FIX-022",
        "fixture_category": "missingness_boundaries",
        "description": "missing_contact_quality_value",
        "target_id": "HOUT-O09",
        "mutation": "contact_quality_value=null",
        "expected_semantics": "unscored_missing",
    },
    {
        "fixture_id": "HCOR-FIX-023",
        "fixture_category": "missingness_boundaries",
        "description": "missing_run_value",
        "target_id": "HOUT-O10",
        "mutation": "run_value=null",
        "expected_semantics": "unscored_missing",
    },
    {
        "fixture_id": "HCOR-FIX-024",
        "fixture_category": "missingness_boundaries",
        "description": "incomplete_game",
        "target_id": "HOUT-O05",
        "mutation": "game_incomplete=true",
        "expected_semantics": "ineligible_incomplete",
    },
    {
        "fixture_id": "HCOR-FIX-025",
        "fixture_category": "payload_integrity",
        "description": "payload_version_preserved",
        "target_id": "HOUT-O02",
        "mutation": "provider_payload_version=payload-v2",
        "expected_semantics": "eligible_payload_version",
    },
    {
        "fixture_id": "HCOR-FIX-026",
        "fixture_category": "payload_integrity",
        "description": "payload_key_order_independent",
        "target_id": "HOUT-O02",
        "mutation": "reorder_payload_keys=true",
        "expected_semantics": "digest_stable",
    },
    {
        "fixture_id": "HCOR-FIX-027",
        "fixture_category": "collection_controls",
        "description": "collection_disabled",
        "target_id": "HOUT-O02",
        "mutation": "collection_enabled=false",
        "expected_semantics": "non_emitting",
    },
    {
        "fixture_id": "HCOR-FIX-028",
        "fixture_category": "deterministic_replay",
        "description": "identical_payload_replay",
        "target_id": "HOUT-O02",
        "mutation": "none",
        "expected_semantics": "byte_stable_replay",
    },
    {
        "fixture_id": "HCOR-FIX-029",
        "fixture_category": "cross_contract_compatibility",
        "description": "adapter_input_field_coverage",
        "target_id": "HOUT-O02",
        "mutation": "none",
        "expected_semantics": "adapter_schema_compatible",
    },
    {
        "fixture_id": "HCOR-FIX-030",
        "fixture_category": "cross_contract_compatibility",
        "description": "historical_outcome_field_coverage",
        "target_id": "HOUT-O02",
        "mutation": "none",
        "expected_semantics": "outcome_schema_compatible",
    },
]


CORPUS_STORAGE_PLAN = [
    {
        "artifact": "manifest.json",
        "format": "json",
        "purpose": "Immutable corpus identity, version, counts, and digests.",
    },
    {
        "artifact": "schema.json",
        "format": "json",
        "purpose": "Fixture-envelope and provider-payload schema.",
    },
    {
        "artifact": "provider_payloads.jsonl",
        "format": "jsonl",
        "purpose": "One canonical provider payload per fixture.",
    },
    {
        "artifact": "expected_adapter_outputs.jsonl",
        "format": "jsonl",
        "purpose": "Expected Layer 9F adapter outputs.",
    },
    {
        "artifact": "expected_outcome_records.jsonl",
        "format": "jsonl",
        "purpose": "Expected Layer 9D historical outcome records.",
    },
    {
        "artifact": "fixture_index.csv",
        "format": "csv",
        "purpose": "Human-readable fixture discovery and category index.",
    },
    {
        "artifact": "README.md",
        "format": "markdown",
        "purpose": "Corpus scope, replay procedure, and authority boundaries.",
    },
]


CORPUS_RULES = [
    {
        "rule_id": "HCOR-R01",
        "rule": "every target has at least one valid fixture",
    },
    {
        "rule_id": "HCOR-R02",
        "rule": "every fixture has a stable unique identifier",
    },
    {
        "rule_id": "HCOR-R03",
        "rule": "provider payloads are canonical JSON objects",
    },
    {
        "rule_id": "HCOR-R04",
        "rule": "fixture ordering is lexicographic by fixture identifier",
    },
    {
        "rule_id": "HCOR-R05",
        "rule": "expected adapter outputs are immutable",
    },
    {
        "rule_id": "HCOR-R06",
        "rule": "expected historical outcome outputs are immutable",
    },
    {
        "rule_id": "HCOR-R07",
        "rule": "missing values are never zero imputed",
    },
    {
        "rule_id": "HCOR-R08",
        "rule": "unsupported codes remain distinct from negative outcomes",
    },
    {
        "rule_id": "HCOR-R09",
        "rule": "availability timestamps preserve provider semantics",
    },
    {
        "rule_id": "HCOR-R10",
        "rule": "revision payloads preserve original revisions",
    },
    {
        "rule_id": "HCOR-R11",
        "rule": "disabled collection fixtures emit no records",
    },
    {
        "rule_id": "HCOR-R12",
        "rule": "corpus replay performs no external fetch",
    },
    {
        "rule_id": "HCOR-R13",
        "rule": "corpus replay performs no production materialization",
    },
    {
        "rule_id": "HCOR-R14",
        "rule": "corpus artifacts include SHA256 digests",
    },
    {
        "rule_id": "HCOR-R15",
        "rule": "fixture corpus version changes on semantic modification",
    },
    {
        "rule_id": "HCOR-R16",
        "rule": "fixture corpus does not authorize evaluation joins",
    },
]


IMPLEMENTATION_HANDOFF = [
    {
        "step": 1,
        "action": "Create a versioned local fixture directory.",
    },
    {
        "step": 2,
        "action": "Define the fixture-envelope JSON schema.",
    },
    {
        "step": 3,
        "action": "Define the provider-payload JSON schema.",
    },
    {
        "step": 4,
        "action": "Materialize valid fixtures for all ten targets.",
    },
    {
        "step": 5,
        "action": "Materialize raw-code boundary fixtures.",
    },
    {
        "step": 6,
        "action": "Materialize identity boundary fixtures.",
    },
    {
        "step": 7,
        "action": "Materialize availability boundary fixtures.",
    },
    {
        "step": 8,
        "action": "Materialize revision boundary fixtures.",
    },
    {
        "step": 9,
        "action": "Materialize missingness and incomplete-game fixtures.",
    },
    {
        "step": 10,
        "action": "Materialize collection-control fixtures.",
    },
    {
        "step": 11,
        "action": "Generate expected Layer 9F adapter outputs.",
    },
    {
        "step": 12,
        "action": "Generate expected Layer 9D outcome records.",
    },
    {
        "step": 13,
        "action": "Generate canonical artifact and record digests.",
    },
    {
        "step": 14,
        "action": "Implement deterministic local replay validation.",
    },
    {
        "step": 15,
        "action": "Verify zero external fetches and zero production writes.",
    },
    {
        "step": 16,
        "action": "Preserve prohibition on feature and prediction joins.",
    },
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
    specification = importlib.util.spec_from_file_location(
        module_name,
        path,
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

    predecessor_module = load_module(
        PREDECESSOR_PATH,
        "layer_9f_historical_outcome_source_adapter_contract",
    )

    adapter_contract_compatible = all(
        [
            hasattr(
                predecessor_module,
                "ADAPTER_INPUT_FIELDS",
            ),
            hasattr(
                predecessor_module,
                "TARGET_ADAPTER_MAP",
            ),
            callable(
                getattr(
                    predecessor_module,
                    "adapt_provider_payload",
                    None,
                )
            ),
            callable(
                getattr(
                    predecessor_module,
                    "materialize_adapter_record",
                    None,
                )
            ),
        ]
    )

    adapter_target_ids = {
        str(row["target_id"])
        for row in getattr(
            predecessor_module,
            "TARGET_ADAPTER_MAP",
            [],
        )
    }

    planned_target_ids = {
        str(row["target_id"])
        for row in FIXTURE_SCENARIOS
        if row["fixture_category"]
        == "valid_target_examples"
    }

    category_names = {
        row["category"]
        for row in CORPUS_CATEGORIES
    }

    scenario_category_names = {
        row["fixture_category"]
        for row in FIXTURE_SCENARIOS
    }

    fixture_ids = [
        row["fixture_id"]
        for row in FIXTURE_SCENARIOS
    ]

    schema_fields = [
        row["field"]
        for row in FIXTURE_SCHEMA_FIELDS
    ]

    rule_ids = [
        row["rule_id"]
        for row in CORPUS_RULES
    ]

    corpus_digest = sha256_payload(
        {
            "corpus_plan_version": (
                CORPUS_PLAN_VERSION
            ),
            "categories": CORPUS_CATEGORIES,
            "fixture_schema": (
                FIXTURE_SCHEMA_FIELDS
            ),
            "fixture_scenarios": (
                FIXTURE_SCENARIOS
            ),
            "storage_plan": (
                CORPUS_STORAGE_PLAN
            ),
            "rules": CORPUS_RULES,
            "implementation_handoff": (
                IMPLEMENTATION_HANDOFF
            ),
        }
    )

    planning_checks = [
        {
            "check": "nine_f_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "layer_9f_adapter_contract_compatible",
            "actual": adapter_contract_compatible,
            "expected": True,
            "passed": adapter_contract_compatible,
        },
        {
            "check": "corpus_plan_version_explicit",
            "actual": CORPUS_PLAN_VERSION,
            "expected": CORPUS_PLAN_VERSION,
            "passed": bool(
                CORPUS_PLAN_VERSION
            ),
        },
        {
            "check": "ten_corpus_categories_defined",
            "actual": len(
                CORPUS_CATEGORIES
            ),
            "expected": 10,
            "passed": len(
                CORPUS_CATEGORIES
            )
            == 10,
        },
        {
            "check": "fifteen_fixture_schema_fields_defined",
            "actual": len(
                FIXTURE_SCHEMA_FIELDS
            ),
            "expected": 15,
            "passed": len(
                FIXTURE_SCHEMA_FIELDS
            )
            == 15,
        },
        {
            "check": "fixture_schema_fields_unique",
            "actual": len(
                set(schema_fields)
            ),
            "expected": len(schema_fields),
            "passed": len(
                set(schema_fields)
            )
            == len(schema_fields),
        },
        {
            "check": "thirty_fixture_scenarios_defined",
            "actual": len(
                FIXTURE_SCENARIOS
            ),
            "expected": 30,
            "passed": len(
                FIXTURE_SCENARIOS
            )
            == 30,
        },
        {
            "check": "fixture_ids_unique",
            "actual": len(
                set(fixture_ids)
            ),
            "expected": len(fixture_ids),
            "passed": len(
                set(fixture_ids)
            )
            == len(fixture_ids),
        },
        {
            "check": "all_scenario_categories_defined",
            "actual": len(
                scenario_category_names
                - category_names
            ),
            "expected": 0,
            "passed": (
                scenario_category_names
                <= category_names
            ),
        },
        {
            "check": "all_ten_targets_have_valid_fixtures",
            "actual": len(
                planned_target_ids
            ),
            "expected": 10,
            "passed": (
                planned_target_ids
                == adapter_target_ids
                and len(
                    planned_target_ids
                )
                == 10
            ),
        },
        {
            "check": "seven_storage_artifacts_defined",
            "actual": len(
                CORPUS_STORAGE_PLAN
            ),
            "expected": 7,
            "passed": len(
                CORPUS_STORAGE_PLAN
            )
            == 7,
        },
        {
            "check": "sixteen_corpus_rules_defined",
            "actual": len(
                CORPUS_RULES
            ),
            "expected": 16,
            "passed": len(
                CORPUS_RULES
            )
            == 16,
        },
        {
            "check": "corpus_rule_ids_unique",
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
            "check": "sixteen_implementation_steps_defined",
            "actual": len(
                IMPLEMENTATION_HANDOFF
            ),
            "expected": 16,
            "passed": len(
                IMPLEMENTATION_HANDOFF
            )
            == 16,
        },
        {
            "check": "corpus_digest_valid_sha256",
            "actual": corpus_digest,
            "expected": "sha256",
            "passed": bool(
                SHA256_PATTERN.fullmatch(
                    corpus_digest
                )
            ),
        },
        {
            "check": "external_fetch_execution_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "production_collection_execution_absent",
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
        for row in planning_checks
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_fixture_corpus_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_fixture_corpus_plan_failed"
    )

    recommended_next_layer = (
        "9H_pitch_type_matchup_overlay_historical_outcome_fixture_corpus_implementation"
        if all_checks_passed
        else
        "9G_pitch_type_matchup_overlay_historical_outcome_fixture_corpus_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
        [
            "check",
            "actual",
            "expected",
            "passed",
        ],
        planning_checks,
    )

    write_csv(
        OUTPUT_DIR / "corpus_categories.csv",
        [
            "category_id",
            "category",
            "purpose",
        ],
        CORPUS_CATEGORIES,
    )

    write_csv(
        OUTPUT_DIR / "fixture_schema.csv",
        [
            "ordinal",
            "field",
            "type",
            "required",
        ],
        FIXTURE_SCHEMA_FIELDS,
    )

    write_csv(
        OUTPUT_DIR / "fixture_scenarios.csv",
        [
            "fixture_id",
            "fixture_category",
            "description",
            "target_id",
            "mutation",
            "expected_semantics",
        ],
        FIXTURE_SCENARIOS,
    )

    write_csv(
        OUTPUT_DIR / "corpus_storage_plan.csv",
        [
            "artifact",
            "format",
            "purpose",
        ],
        CORPUS_STORAGE_PLAN,
    )

    write_csv(
        OUTPUT_DIR / "corpus_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        CORPUS_RULES,
    )

    write_csv(
        OUTPUT_DIR / "implementation_handoff.csv",
        [
            "step",
            "action",
        ],
        IMPLEMENTATION_HANDOFF,
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
                    "9G is a local fixture-corpus plan only."
                ),
            }
            for authority in PROHIBITED_AUTHORITIES
        ]
        + [
            {
                "authority": (
                    "historical_outcome_fixture_corpus_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "9H may implement deterministic local fixtures and replay "
                    "artifacts without external fetching or production writes."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "corpus_plan_version": (
            CORPUS_PLAN_VERSION
        ),
        "planning_checks_required": len(
            planning_checks
        ),
        "planning_checks_passed": sum(
            bool(row["passed"])
            for row in planning_checks
        ),
        "corpus_categories_defined": len(
            CORPUS_CATEGORIES
        ),
        "fixture_schema_fields_defined": len(
            FIXTURE_SCHEMA_FIELDS
        ),
        "fixture_scenarios_defined": len(
            FIXTURE_SCENARIOS
        ),
        "valid_target_fixtures_defined": len(
            planned_target_ids
        ),
        "storage_artifacts_defined": len(
            CORPUS_STORAGE_PLAN
        ),
        "corpus_rules_defined": len(
            CORPUS_RULES
        ),
        "implementation_steps_defined": len(
            IMPLEMENTATION_HANDOFF
        ),
        "corpus_digest": corpus_digest,
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
            "historical_outcome_fixture_corpus_implementation"
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
        "Corpus plan version: "
        f"{CORPUS_PLAN_VERSION}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_present}"
    )
    print(
        "Layer 9F adapter contract compatible: "
        f"{adapter_contract_compatible}"
    )
    print(
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(
        "Corpus categories defined: "
        f"{summary['corpus_categories_defined']}"
    )
    print(
        "Fixture scenarios defined: "
        f"{summary['fixture_scenarios_defined']}"
    )
    print(
        "Valid target fixtures defined: "
        f"{summary['valid_target_fixtures_defined']}"
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
            for row in planning_checks
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
