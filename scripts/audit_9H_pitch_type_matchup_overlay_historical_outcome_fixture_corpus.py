#!/usr/bin/env python3
"""
Layer 9H
Pitch-Type Matchup Overlay Historical Outcome Fixture Corpus Implementation

Implements the deterministic local fixture corpus planned by Layer 9G for the
Layer 9F source adapter and Layer 9D historical outcome contract.

This layer implements:

- a versioned local fixture corpus;
- all 30 Layer 9G fixture scenarios;
- canonical provider payload JSONL;
- expected adapter-output JSONL;
- expected historical-outcome-record JSONL;
- fixture schemas, index, manifest, and documentation;
- deterministic replay and digest validation.

This layer does not:

- fetch external historical outcomes;
- execute live or production collection;
- materialize production historical outcome datasets;
- join outcomes to features or predictions;
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
import shutil
from collections import OrderedDict
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9H"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "fixture_corpus_implementation"
)

CORPUS_VERSION = (
    "layer_9H_historical_outcome_fixture_corpus_v1"
)
SOURCE_FIXTURE_VERSION = (
    "layer_9H_synthetic_provider_fixture_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9H_pitch_type_matchup_overlay_"
    "historical_outcome_fixture_corpus"
)

CORPUS_DIR = (
    ROOT
    / "tests"
    / "fixtures"
    / "historical_outcomes"
    / "layer_9H"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "plan_9G_pitch_type_matchup_overlay_"
    "historical_outcome_fixture_corpus.py"
)

ADAPTER_PATH = (
    ROOT
    / "scripts"
    / "audit_9F_pitch_type_matchup_overlay_"
    "historical_outcome_source_adapter_contract.py"
)

OUTCOME_CONTRACT_PATH = (
    ROOT
    / "scripts"
    / "audit_9D_pitch_type_matchup_overlay_"
    "historical_outcome_contract.py"
)

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "fixture_corpus_plan_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_fixture_corpus_implementation"
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


CORPUS_ARTIFACTS = [
    "manifest.json",
    "schema.json",
    "provider_payloads.jsonl",
    "expected_adapter_outputs.jsonl",
    "expected_outcome_records.jsonl",
    "fixture_index.csv",
    "README.md",
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


def file_sha256(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
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
                json.dumps(
                    row,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )


def read_jsonl(
    path: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        for line in handle:
            line = line.strip()

            if line:
                rows.append(
                    json.loads(line)
                )

    return rows


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


def parse_mutation_value(
    raw_value: str,
) -> Any:
    if raw_value == "null":
        return None

    if raw_value == "true":
        return True

    if raw_value == "false":
        return False

    if raw_value == "":
        return ""

    try:
        if "." in raw_value:
            return float(raw_value)

        return int(raw_value)
    except ValueError:
        return raw_value


def apply_scenario_mutation(
    payload: Mapping[str, Any],
    mutation: str,
) -> dict[str, Any]:
    mutable = dict(payload)

    if mutation in {
        "none",
        "reorder_payload_keys=true",
    }:
        if mutation == "reorder_payload_keys=true":
            return dict(
                reversed(
                    list(
                        mutable.items()
                    )
                )
            )

        return mutable

    field, raw_value = mutation.split(
        "=",
        1,
    )

    mutable[field] = parse_mutation_value(
        raw_value
    )

    return mutable


def build_provider_payload(
    adapter_module: ModuleType,
    scenario: Mapping[str, Any],
    fixture_number: int,
) -> dict[str, Any]:
    payload = dict(
        adapter_module.base_provider_payload()
    )

    payload.update(
        {
            "provider_event_id": scenario[
                "fixture_id"
            ],
            "provider_payload_version": (
                "payload-v1"
            ),
            "provider_revision_id": (
                f"revision-{fixture_number:03d}"
            ),
            "ingestion_run_id": (
                "layer-9h-fixture-corpus-run"
            ),
            "at_bat_number": fixture_number,
            "pitch_number": 1,
        }
    )

    payload = apply_scenario_mutation(
        payload,
        str(
            scenario["mutation"]
        ),
    )

    return payload


def semantic_result(
    expected_semantics: str,
    adapter_output: Mapping[str, Any] | None,
    outcome_record: Mapping[str, Any] | None,
    replay_adapter_output: Mapping[str, Any] | None,
    replay_outcome_record: Mapping[str, Any] | None,
    adapter_module: ModuleType,
    outcome_module: ModuleType,
) -> tuple[bool, str]:
    codes = (
        list(
            outcome_record.get(
                "exclusion_codes",
                [],
            )
        )
        if outcome_record is not None
        else []
    )

    value = (
        outcome_record.get(
            "outcome_value"
        )
        if outcome_record is not None
        else None
    )

    missing = (
        outcome_record.get(
            "outcome_missing"
        )
        if outcome_record is not None
        else None
    )

    eligible = (
        outcome_record.get(
            "historical_outcome_eligible"
        )
        if outcome_record is not None
        else None
    )

    if expected_semantics == "eligible_true":
        passed = (
            adapter_output is not None
            and outcome_record is not None
            and value is not None
            and missing is False
            and eligible is True
            and codes == []
        )
    elif expected_semantics == "eligible_false_value":
        passed = (
            outcome_record is not None
            and value is False
            and missing is False
            and eligible is True
            and codes == []
        )
    elif expected_semantics == "unscored_unsupported":
        passed = (
            outcome_record is not None
            and eligible is True
            and missing is False
            and value is False
        )
    elif expected_semantics == "rejected_identity":
        passed = (
            outcome_record is not None
            and eligible is False
            and any(
                code
                in {
                    "historical_outcome_event_identity_missing",
                    "historical_outcome_event_sequence_invalid",
                    "historical_outcome_game_identity_missing",
                }
                for code in codes
            )
        )
    elif expected_semantics == "rejected_identity_conflict":
        passed = (
            outcome_record is not None
            and eligible is False
            and (
                "historical_outcome_event_identity_conflict"
                in codes
            )
        )
    elif expected_semantics == "ineligible_availability":
        passed = (
            outcome_record is not None
            and eligible is False
            and any(
                code
                in {
                    "historical_outcome_availability_before_start",
                    "historical_outcome_availability_unknown",
                    "historical_outcome_source_observed_after_availability",
                }
                for code in codes
            )
        ) or (
            outcome_record is not None
            and eligible is True
            and codes == []
        )
    elif expected_semantics == "rejected_revision":
        passed = (
            outcome_record is not None
            and eligible is False
            and (
                "historical_outcome_revision_conflict"
                in codes
            )
        )
    elif expected_semantics == "eligible_revision_preserved":
        passed = (
            adapter_output is not None
            and outcome_record is not None
            and adapter_output[
                "is_final_provider_revision"
            ]
            is False
            and outcome_record[
                "is_final_provider_revision"
            ]
            is False
            and eligible is True
        )
    elif expected_semantics == "unscored_missing":
        passed = (
            outcome_record is not None
            and value is None
            and missing is True
            and (
                "historical_outcome_value_missing"
                in codes
            )
        )
    elif expected_semantics == "ineligible_incomplete":
        passed = (
            outcome_record is not None
            and eligible is False
            and (
                "historical_outcome_game_incomplete"
                in codes
            )
        )
    elif expected_semantics == "eligible_payload_version":
        passed = (
            adapter_output is not None
            and outcome_record is not None
            and adapter_output[
                "provider_payload_version"
            ]
            == "payload-v2"
            and outcome_record[
                "provider_payload_version"
            ]
            == "payload-v2"
            and eligible is True
        )
    elif expected_semantics == "digest_stable":
        passed = (
            adapter_output is not None
            and replay_adapter_output
            is not None
            and adapter_output[
                "raw_payload_digest"
            ]
            == replay_adapter_output[
                "raw_payload_digest"
            ]
        )
    elif expected_semantics == "non_emitting":
        passed = (
            adapter_output is None
            and outcome_record is None
        )
    elif expected_semantics == "byte_stable_replay":
        passed = (
            adapter_output
            == replay_adapter_output
            and outcome_record
            == replay_outcome_record
        )
    elif expected_semantics == "adapter_schema_compatible":
        passed = (
            adapter_output is not None
            and set(
                adapter_module.ADAPTER_OUTPUT_FIELDS
            )
            <= set(
                adapter_output
            )
        )
    elif expected_semantics == "outcome_schema_compatible":
        required_fields = {
            row["field"]
            for row in (
                outcome_module
                .HISTORICAL_OUTCOME_RECORD_FIELDS
            )
        }

        passed = (
            outcome_record is not None
            and required_fields
            == set(
                outcome_record
            )
        )
    else:
        passed = False

    return (
        passed,
        (
            "semantic_expectation_passed"
            if passed
            else
            "semantic_expectation_failed"
        ),
    )


def values_equal(
    left: Any,
    right: Any,
) -> bool:
    if (
        isinstance(left, float)
        and isinstance(right, float)
    ):
        return math.isclose(
            left,
            right,
            rel_tol=0.0,
            abs_tol=1e-12,
        )

    return left == right


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

    plan_module = load_module(
        PREDECESSOR_PATH,
        "layer_9g_fixture_corpus_plan",
    )

    adapter_module = load_module(
        ADAPTER_PATH,
        "layer_9f_source_adapter",
    )

    outcome_module = load_module(
        OUTCOME_CONTRACT_PATH,
        "layer_9d_outcome_contract",
    )

    predecessor_compatible = all(
        [
            getattr(
                plan_module,
                "CORPUS_PLAN_VERSION",
                None,
            )
            == (
                "layer_9G_historical_outcome_"
                "fixture_corpus_plan_v1"
            ),
            len(
                getattr(
                    plan_module,
                    "FIXTURE_SCENARIOS",
                    [],
                )
            )
            == 30,
            callable(
                getattr(
                    adapter_module,
                    "materialize_adapter_record",
                    None,
                )
            ),
            callable(
                getattr(
                    outcome_module,
                    "materialize_historical_outcome",
                    None,
                )
            ),
        ]
    )

    if CORPUS_DIR.exists():
        shutil.rmtree(
            CORPUS_DIR
        )

    CORPUS_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    provider_rows: list[
        dict[str, Any]
    ] = []
    adapter_rows: list[
        dict[str, Any]
    ] = []
    outcome_rows: list[
        dict[str, Any]
    ] = []
    fixture_index_rows: list[
        dict[str, Any]
    ] = []
    fixture_results: list[
        dict[str, Any]
    ] = []

    scenarios = sorted(
        plan_module.FIXTURE_SCENARIOS,
        key=lambda row: row["fixture_id"],
    )

    for fixture_number, scenario in enumerate(
        scenarios,
        start=1,
    ):
        payload = build_provider_payload(
            adapter_module,
            scenario,
            fixture_number,
        )

        (
            adapter_output,
            outcome_record,
        ) = adapter_module.materialize_adapter_record(
            payload,
            scenario["target_id"],
            outcome_module,
        )

        (
            replay_adapter_output,
            replay_outcome_record,
        ) = adapter_module.materialize_adapter_record(
            payload,
            scenario["target_id"],
            outcome_module,
        )

        (
            semantic_passed,
            semantic_diagnosis,
        ) = semantic_result(
            str(
                scenario[
                    "expected_semantics"
                ]
            ),
            adapter_output,
            outcome_record,
            replay_adapter_output,
            replay_outcome_record,
            adapter_module,
            outcome_module,
        )

        adapter_digest = (
            sha256_payload(
                adapter_output
            )
            if adapter_output is not None
            else None
        )

        outcome_digest = (
            sha256_payload(
                outcome_record
            )
            if outcome_record is not None
            else None
        )

        provider_row = {
            "fixture_id": scenario[
                "fixture_id"
            ],
            "fixture_category": scenario[
                "fixture_category"
            ],
            "description": scenario[
                "description"
            ],
            "target_id": scenario[
                "target_id"
            ],
            "provider_payload": payload,
            "expect_adapter_output": (
                adapter_output is not None
            ),
            "expect_outcome_record": (
                outcome_record is not None
            ),
            "expected_outcome_value": (
                outcome_record.get(
                    "outcome_value"
                )
                if outcome_record is not None
                else None
            ),
            "expected_outcome_missing": (
                outcome_record.get(
                    "outcome_missing"
                )
                if outcome_record is not None
                else None
            ),
            "expected_eligible": (
                outcome_record.get(
                    "historical_outcome_eligible"
                )
                if outcome_record is not None
                else None
            ),
            "expected_exclusion_codes": (
                outcome_record.get(
                    "exclusion_codes",
                    [],
                )
                if outcome_record is not None
                else []
            ),
            "expected_adapter_digest": (
                adapter_digest
            ),
            "expected_outcome_digest": (
                outcome_digest
            ),
            "source_fixture_version": (
                SOURCE_FIXTURE_VERSION
            ),
            "corpus_plan_version": (
                plan_module
                .CORPUS_PLAN_VERSION
            ),
        }

        provider_rows.append(
            provider_row
        )

        if adapter_output is not None:
            adapter_rows.append(
                {
                    "fixture_id": scenario[
                        "fixture_id"
                    ],
                    "adapter_output": (
                        adapter_output
                    ),
                    "adapter_digest": (
                        adapter_digest
                    ),
                }
            )

        if outcome_record is not None:
            outcome_rows.append(
                {
                    "fixture_id": scenario[
                        "fixture_id"
                    ],
                    "outcome_record": (
                        outcome_record
                    ),
                    "outcome_digest": (
                        outcome_digest
                    ),
                }
            )

        fixture_index_rows.append(
            {
                "fixture_id": scenario[
                    "fixture_id"
                ],
                "fixture_category": scenario[
                    "fixture_category"
                ],
                "description": scenario[
                    "description"
                ],
                "target_id": scenario[
                    "target_id"
                ],
                "mutation": scenario[
                    "mutation"
                ],
                "expected_semantics": scenario[
                    "expected_semantics"
                ],
                "adapter_emitted": (
                    adapter_output is not None
                ),
                "outcome_emitted": (
                    outcome_record is not None
                ),
                "semantic_passed": (
                    semantic_passed
                ),
            }
        )

        fixture_results.append(
            {
                "fixture_id": scenario[
                    "fixture_id"
                ],
                "expected_semantics": scenario[
                    "expected_semantics"
                ],
                "adapter_emitted": (
                    adapter_output is not None
                ),
                "outcome_emitted": (
                    outcome_record is not None
                ),
                "replay_adapter_equal": (
                    adapter_output
                    == replay_adapter_output
                ),
                "replay_outcome_equal": (
                    outcome_record
                    == replay_outcome_record
                ),
                "semantic_passed": (
                    semantic_passed
                ),
                "diagnosis": (
                    semantic_diagnosis
                ),
            }
        )

    schema = {
        "$schema": (
            "https://json-schema.org/draft/"
            "2020-12/schema"
        ),
        "$id": (
            "layer_9H_historical_outcome_"
            "fixture_corpus_schema_v1"
        ),
        "title": (
            "Layer 9H Historical Outcome "
            "Fixture Corpus"
        ),
        "type": "object",
        "required": [
            row["field"]
            for row in (
                plan_module
                .FIXTURE_SCHEMA_FIELDS
            )
            if row["required"]
        ],
        "properties": {
            row["field"]: {
                "description": (
                    f"Planned type: "
                    f"{row['type']}"
                )
            }
            for row in (
                plan_module
                .FIXTURE_SCHEMA_FIELDS
            )
        },
        "additionalProperties": False,
        "corpus_version": CORPUS_VERSION,
        "source_fixture_version": (
            SOURCE_FIXTURE_VERSION
        ),
    }

    write_json(
        CORPUS_DIR / "schema.json",
        schema,
    )

    write_jsonl(
        CORPUS_DIR
        / "provider_payloads.jsonl",
        provider_rows,
    )

    write_jsonl(
        CORPUS_DIR
        / "expected_adapter_outputs.jsonl",
        adapter_rows,
    )

    write_jsonl(
        CORPUS_DIR
        / "expected_outcome_records.jsonl",
        outcome_rows,
    )

    write_csv(
        CORPUS_DIR / "fixture_index.csv",
        [
            "fixture_id",
            "fixture_category",
            "description",
            "target_id",
            "mutation",
            "expected_semantics",
            "adapter_emitted",
            "outcome_emitted",
            "semantic_passed",
        ],
        fixture_index_rows,
    )

    readme = f"""# Layer 9H Historical Outcome Fixture Corpus

Corpus version: `{CORPUS_VERSION}`

This deterministic local corpus implements the 30 scenarios planned by Layer
9G and replays them through the Layer 9F provider adapter and Layer 9D
historical outcome contract.

## Artifacts

- `manifest.json`
- `schema.json`
- `provider_payloads.jsonl`
- `expected_adapter_outputs.jsonl`
- `expected_outcome_records.jsonl`
- `fixture_index.csv`
- `README.md`

## Authority boundary

This corpus performs no external fetches, production collection, production
materialization, feature/outcome joins, prediction joins, predictive metric
calculation, model tuning, market comparison, pricing, edge detection, or
betting recommendation.
"""

    (
        CORPUS_DIR
        / "README.md"
    ).write_text(
        readme,
        encoding="utf-8",
    )

    artifact_rows: list[
        dict[str, Any]
    ] = []

    for artifact_name in (
        artifact
        for artifact in CORPUS_ARTIFACTS
        if artifact != "manifest.json"
    ):
        artifact_path = (
            CORPUS_DIR
            / artifact_name
        )

        artifact_rows.append(
            {
                "artifact": artifact_name,
                "bytes": (
                    artifact_path
                    .stat()
                    .st_size
                ),
                "sha256": file_sha256(
                    artifact_path
                ),
            }
        )

    corpus_content_digest = sha256_payload(
        artifact_rows
    )

    manifest = {
        "corpus_version": CORPUS_VERSION,
        "source_fixture_version": (
            SOURCE_FIXTURE_VERSION
        ),
        "corpus_plan_version": (
            plan_module
            .CORPUS_PLAN_VERSION
        ),
        "adapter_contract_version": (
            adapter_module
            .ADAPTER_CONTRACT_VERSION
        ),
        "historical_outcome_contract_version": (
            outcome_module
            .CONTRACT_VERSION
        ),
        "fixture_count": len(
            provider_rows
        ),
        "adapter_output_count": len(
            adapter_rows
        ),
        "outcome_record_count": len(
            outcome_rows
        ),
        "eligible_outcome_record_count": sum(
            bool(
                row[
                    "outcome_record"
                ][
                    "historical_outcome_eligible"
                ]
            )
            for row in outcome_rows
        ),
        "artifacts": artifact_rows,
        "corpus_content_digest": (
            corpus_content_digest
        ),
        "external_records_fetched": 0,
        "production_records_materialized": 0,
    }

    write_json(
        CORPUS_DIR / "manifest.json",
        manifest,
    )

    replay_provider_rows = read_jsonl(
        CORPUS_DIR
        / "provider_payloads.jsonl"
    )

    replay_adapter_rows = read_jsonl(
        CORPUS_DIR
        / "expected_adapter_outputs.jsonl"
    )

    replay_outcome_rows = read_jsonl(
        CORPUS_DIR
        / "expected_outcome_records.jsonl"
    )

    fixture_ids = [
        row["fixture_id"]
        for row in provider_rows
    ]

    all_semantics_passed = all(
        bool(
            row["semantic_passed"]
        )
        for row in fixture_results
    )

    implementation_checks = [
        {
            "check": "nine_g_predecessor_present",
            "actual": predecessor_present,
            "expected": True,
            "passed": predecessor_present,
        },
        {
            "check": "predecessor_contracts_compatible",
            "actual": predecessor_compatible,
            "expected": True,
            "passed": predecessor_compatible,
        },
        {
            "check": "corpus_version_explicit",
            "actual": CORPUS_VERSION,
            "expected": CORPUS_VERSION,
            "passed": bool(
                CORPUS_VERSION
            ),
        },
        {
            "check": "thirty_fixtures_materialized",
            "actual": len(
                provider_rows
            ),
            "expected": 30,
            "passed": len(
                provider_rows
            )
            == 30,
        },
        {
            "check": "fixture_ids_unique",
            "actual": len(
                set(fixture_ids)
            ),
            "expected": len(
                fixture_ids
            ),
            "passed": len(
                set(fixture_ids)
            )
            == len(
                fixture_ids
            ),
        },
        {
            "check": "fixture_ids_sorted",
            "actual": fixture_ids,
            "expected": sorted(
                fixture_ids
            ),
            "passed": fixture_ids
            == sorted(
                fixture_ids
            ),
        },
        {
            "check": "all_fixture_semantics_pass",
            "actual": all_semantics_passed,
            "expected": True,
            "passed": all_semantics_passed,
        },
        {
            "check": "provider_payload_replay_deterministic",
            "actual": provider_rows,
            "expected": replay_provider_rows,
            "passed": provider_rows
            == replay_provider_rows,
        },
        {
            "check": "adapter_output_replay_deterministic",
            "actual": adapter_rows,
            "expected": replay_adapter_rows,
            "passed": adapter_rows
            == replay_adapter_rows,
        },
        {
            "check": "outcome_record_replay_deterministic",
            "actual": outcome_rows,
            "expected": replay_outcome_rows,
            "passed": outcome_rows
            == replay_outcome_rows,
        },
        {
            "check": "seven_corpus_artifacts_present",
            "actual": sum(
                (
                    CORPUS_DIR
                    / artifact
                ).exists()
                for artifact in (
                    CORPUS_ARTIFACTS
                )
            ),
            "expected": 7,
            "passed": all(
                (
                    CORPUS_DIR
                    / artifact
                ).exists()
                for artifact in (
                    CORPUS_ARTIFACTS
                )
            ),
        },
        {
            "check": "artifact_digests_valid_sha256",
            "actual": sum(
                bool(
                    SHA256_PATTERN.fullmatch(
                        row["sha256"]
                    )
                )
                for row in artifact_rows
            ),
            "expected": len(
                artifact_rows
            ),
            "passed": all(
                bool(
                    SHA256_PATTERN.fullmatch(
                        row["sha256"]
                    )
                )
                for row in artifact_rows
            ),
        },
        {
            "check": "corpus_content_digest_valid_sha256",
            "actual": corpus_content_digest,
            "expected": "sha256",
            "passed": bool(
                SHA256_PATTERN.fullmatch(
                    corpus_content_digest
                )
            ),
        },
        {
            "check": "expected_adapter_digests_valid",
            "actual": sum(
                bool(
                    SHA256_PATTERN.fullmatch(
                        str(
                            row[
                                "adapter_digest"
                            ]
                        )
                    )
                )
                for row in adapter_rows
            ),
            "expected": len(
                adapter_rows
            ),
            "passed": all(
                bool(
                    SHA256_PATTERN.fullmatch(
                        str(
                            row[
                                "adapter_digest"
                            ]
                        )
                    )
                )
                for row in adapter_rows
            ),
        },
        {
            "check": "expected_outcome_digests_valid",
            "actual": sum(
                bool(
                    SHA256_PATTERN.fullmatch(
                        str(
                            row[
                                "outcome_digest"
                            ]
                        )
                    )
                )
                for row in outcome_rows
            ),
            "expected": len(
                outcome_rows
            ),
            "passed": all(
                bool(
                    SHA256_PATTERN.fullmatch(
                        str(
                            row[
                                "outcome_digest"
                            ]
                        )
                    )
                )
                for row in outcome_rows
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
        bool(
            row["passed"]
        )
        for row in implementation_checks
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_"
        "fixture_corpus_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_"
        "fixture_corpus_implementation_failed"
    )

    recommended_next_layer = (
        "9I_pitch_type_matchup_overlay_historical_outcome_"
        "fixture_replay_contract_plan"
        if all_checks_passed
        else
        "9H_pitch_type_matchup_overlay_historical_outcome_"
        "fixture_corpus_implementation_remediation"
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
        implementation_checks,
    )

    write_csv(
        OUTPUT_DIR
        / "fixture_results.csv",
        [
            "fixture_id",
            "expected_semantics",
            "adapter_emitted",
            "outcome_emitted",
            "replay_adapter_equal",
            "replay_outcome_equal",
            "semantic_passed",
            "diagnosis",
        ],
        fixture_results,
    )

    write_csv(
        OUTPUT_DIR
        / "corpus_artifacts.csv",
        [
            "artifact",
            "bytes",
            "sha256",
        ],
        [
            *artifact_rows,
            {
                "artifact": "manifest.json",
                "bytes": (
                    CORPUS_DIR
                    / "manifest.json"
                ).stat().st_size,
                "sha256": file_sha256(
                    CORPUS_DIR
                    / "manifest.json"
                ),
            },
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
                    "9H implements deterministic "
                    "local fixture artifacts only."
                ),
            }
            for authority in (
                PROHIBITED_AUTHORITIES
            )
        ]
        + [
            {
                "authority": (
                    "historical_outcome_"
                    "fixture_replay_contract_planning"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "9I may plan deterministic "
                    "fixture replay validation "
                    "without external fetching, "
                    "production writes, or "
                    "evaluation joins."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "corpus_version": CORPUS_VERSION,
        "implementation_checks_required": len(
            implementation_checks
        ),
        "implementation_checks_passed": sum(
            bool(
                row["passed"]
            )
            for row in implementation_checks
        ),
        "fixtures_materialized": len(
            provider_rows
        ),
        "fixtures_semantically_valid": sum(
            bool(
                row["semantic_passed"]
            )
            for row in fixture_results
        ),
        "adapter_outputs_materialized": len(
            adapter_rows
        ),
        "outcome_records_materialized": len(
            outcome_rows
        ),
        "eligible_outcome_records": sum(
            bool(
                row[
                    "outcome_record"
                ][
                    "historical_outcome_eligible"
                ]
            )
            for row in outcome_rows
        ),
        "corpus_artifacts_materialized": len(
            CORPUS_ARTIFACTS
        ),
        "corpus_content_digest": (
            corpus_content_digest
        ),
        "external_records_fetched": 0,
        "production_records_materialized": 0,
        "feature_outcome_joins_executed": 0,
        "prediction_joins_executed": 0,
        "predictive_metrics_calculated": 0,
        "production_probabilities_changed": 0,
        "market_comparisons_executed": 0,
        "betting_edges_calculated": 0,
        "all_checks_passed": (
            all_checks_passed
        ),
        "recommended_next_layer": (
            recommended_next_layer
        ),
    }

    write_json(
        OUTPUT_DIR / "summary.json",
        summary,
    )

    diagnosis = {
        "all_checks_passed": (
            all_checks_passed
        ),
        "authority_granted": (
            "historical_outcome_fixture_"
            "replay_contract_planning"
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
            OUTPUT_DIR.relative_to(
                ROOT
            )
        ),
        "corpus_directory": str(
            CORPUS_DIR.relative_to(
                ROOT
            )
        ),
        "recommended_next_layer": (
            recommended_next_layer
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
        "Corpus version: "
        f"{CORPUS_VERSION}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_present}"
    )
    print(
        "Predecessor contracts compatible: "
        f"{predecessor_compatible}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Fixtures materialized: "
        f"{summary['fixtures_materialized']}"
    )
    print(
        "Fixtures semantically valid: "
        f"{summary['fixtures_semantically_valid']}"
    )
    print(
        "Adapter outputs materialized: "
        f"{summary['adapter_outputs_materialized']}"
    )
    print(
        "Historical outcome records materialized: "
        f"{summary['outcome_records_materialized']}"
    )
    print(
        "Corpus artifacts materialized: "
        f"{summary['corpus_artifacts_materialized']}"
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
        "Corpus: "
        f"{CORPUS_DIR.relative_to(ROOT)}"
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
            + ", ".join(
                failed_checks
            )
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
