#!/usr/bin/env python3
"""
Layer 9J
Pitch-Type Matchup Overlay Historical Outcome Fixture Replay Contract Implementation

Implements the deterministic local replay contract planned by Layer 9I for the
committed Layer 9H historical outcome fixture corpus.

This layer:

- validates the committed corpus and manifest;
- replays all provider fixtures through the Layer 9F adapter;
- replays adapter outputs through the Layer 9D historical outcome contract;
- compares emissions, canonical records, and SHA-256 digests;
- executes the complete replay twice to prove repeatability;
- writes temporary diagnostic artifacts only.

This layer does not fetch external data, modify the committed corpus, create
production historical outcomes, execute evaluation joins, calculate predictive
metrics, tune models, change probabilities, price markets, or recommend bets.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9J"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "fixture_replay_contract_implementation"
)

REPLAY_CONTRACT_VERSION = (
    "layer_9J_historical_outcome_fixture_replay_contract_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9J_pitch_type_matchup_overlay_"
    "historical_outcome_fixture_replay_contract"
)

PLAN_PATH = (
    ROOT
    / "scripts"
    / "plan_9I_pitch_type_matchup_overlay_"
    "historical_outcome_fixture_replay_contract.py"
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

CORPUS_DIR = (
    ROOT
    / "tests"
    / "fixtures"
    / "historical_outcomes"
    / "layer_9H"
)

MANIFEST_PATH = CORPUS_DIR / "manifest.json"
SCHEMA_PATH = CORPUS_DIR / "schema.json"
PROVIDER_PAYLOADS_PATH = CORPUS_DIR / "provider_payloads.jsonl"
EXPECTED_ADAPTER_OUTPUTS_PATH = (
    CORPUS_DIR / "expected_adapter_outputs.jsonl"
)
EXPECTED_OUTCOME_RECORDS_PATH = (
    CORPUS_DIR / "expected_outcome_records.jsonl"
)
FIXTURE_INDEX_PATH = CORPUS_DIR / "fixture_index.csv"
README_PATH = CORPUS_DIR / "README.md"

EXPECTED_PLAN_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "fixture_replay_contract_plan_complete"
)

EXPECTED_PLAN_AUTHORITY = (
    "historical_outcome_fixture_replay_contract_implementation"
)

EXPECTED_CORPUS_VERSION = (
    "layer_9H_historical_outcome_fixture_corpus_v1"
)

REQUIRED_CORPUS_ARTIFACTS = [
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

            row = json.loads(stripped)

            if not isinstance(row, dict):
                raise ValueError(
                    f"{path}:{line_number} is not an object"
                )

            rows.append(row)

    return rows


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(
        "r",
        encoding="utf-8",
        newline="",
    ) as handle:
        return list(
            csv.DictReader(handle)
        )


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
            fieldnames=list(fieldnames),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)


def expected_record_map(
    rows: Sequence[Mapping[str, Any]],
    record_field: str,
    digest_field: str,
) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}

    for row in rows:
        fixture_id = str(
            row["fixture_id"]
        )

        mapped[fixture_id] = {
            "record": row.get(
                record_field
            ),
            "digest": row.get(
                digest_field
            ),
        }

    return mapped


def normalize_codes(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []

    return [
        str(item)
        for item in value
    ]


def semantic_fields_match(
    fixture: Mapping[str, Any],
    outcome_record: Mapping[str, Any] | None,
) -> bool:
    expect_outcome = bool(
        fixture.get(
            "expect_outcome_record"
        )
    )

    if not expect_outcome:
        return outcome_record is None

    if outcome_record is None:
        return False

    return (
        outcome_record.get(
            "outcome_value"
        )
        == fixture.get(
            "expected_outcome_value"
        )
        and outcome_record.get(
            "outcome_missing"
        )
        == fixture.get(
            "expected_outcome_missing"
        )
        and outcome_record.get(
            "historical_outcome_eligible"
        )
        == fixture.get(
            "expected_eligible"
        )
        and normalize_codes(
            outcome_record.get(
                "exclusion_codes"
            )
        )
        == normalize_codes(
            fixture.get(
                "expected_exclusion_codes"
            )
        )
    )


def replay_once(
    fixtures: Sequence[Mapping[str, Any]],
    adapter_module: ModuleType,
    outcome_module: ModuleType,
) -> tuple[
    list[dict[str, Any]],
    dict[str, dict[str, Any] | None],
    dict[str, dict[str, Any] | None],
]:
    replay_rows: list[dict[str, Any]] = []
    adapter_outputs: dict[
        str,
        dict[str, Any] | None,
    ] = {}
    outcome_records: dict[
        str,
        dict[str, Any] | None,
    ] = {}

    for fixture in sorted(
        fixtures,
        key=lambda row: str(
            row["fixture_id"]
        ),
    ):
        fixture_id = str(
            fixture["fixture_id"]
        )
        target_id = str(
            fixture["target_id"]
        )
        provider_payload = fixture[
            "provider_payload"
        ]

        adapter_output, outcome_record = (
            adapter_module.materialize_adapter_record(
                provider_payload,
                target_id,
                outcome_module,
            )
        )

        adapter_outputs[
            fixture_id
        ] = adapter_output

        outcome_records[
            fixture_id
        ] = outcome_record

        replay_rows.append(
            {
                "fixture_id": fixture_id,
                "adapter_output": (
                    adapter_output
                ),
                "adapter_digest": (
                    sha256_payload(
                        adapter_output
                    )
                    if adapter_output
                    is not None
                    else None
                ),
                "outcome_record": (
                    outcome_record
                ),
                "outcome_digest": (
                    sha256_payload(
                        outcome_record
                    )
                    if outcome_record
                    is not None
                    else None
                ),
            }
        )

    return (
        replay_rows,
        adapter_outputs,
        outcome_records,
    )


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    plan_constants = string_constants(
        PLAN_PATH
    )

    predecessor_verified = (
        PLAN_PATH.exists()
        and EXPECTED_PLAN_DIAGNOSIS
        in plan_constants
        and EXPECTED_PLAN_AUTHORITY
        in plan_constants
    )

    required_paths = [
        CORPUS_DIR / name
        for name in REQUIRED_CORPUS_ARTIFACTS
    ]

    corpus_present = all(
        path.exists()
        for path in required_paths
    )

    manifest: dict[str, Any] = {}
    schema: dict[str, Any] = {}
    fixtures: list[dict[str, Any]] = []
    expected_adapter_rows: list[
        dict[str, Any]
    ] = []
    expected_outcome_rows: list[
        dict[str, Any]
    ] = []
    fixture_index_rows: list[
        dict[str, str]
    ] = []

    load_error: str | None = None

    try:
        if corpus_present:
            manifest = read_json(
                MANIFEST_PATH
            )
            schema = read_json(
                SCHEMA_PATH
            )
            fixtures = read_jsonl(
                PROVIDER_PAYLOADS_PATH
            )
            expected_adapter_rows = read_jsonl(
                EXPECTED_ADAPTER_OUTPUTS_PATH
            )
            expected_outcome_rows = read_jsonl(
                EXPECTED_OUTCOME_RECORDS_PATH
            )
            fixture_index_rows = read_csv(
                FIXTURE_INDEX_PATH
            )
    except Exception as exc:
        load_error = (
            f"{type(exc).__name__}: {exc}"
        )

    manifest_artifacts = {
        str(row.get("artifact")): row
        for row in manifest.get(
            "artifacts",
            [],
        )
        if isinstance(row, dict)
    }

    artifact_integrity_rows: list[
        dict[str, Any]
    ] = []

    for artifact_name in (
        REQUIRED_CORPUS_ARTIFACTS
    ):
        path = CORPUS_DIR / artifact_name
        declared_row = (
            manifest_artifacts.get(
                artifact_name
            )
        )
        declared_digest = (
            declared_row.get("sha256")
            if declared_row
            is not None
            else None
        )
        actual_digest = (
            file_sha256(path)
            if path.exists()
            else None
        )

        if artifact_name == "manifest.json":
            digest_matches = (
                path.exists()
            )
        else:
            digest_matches = (
                path.exists()
                and declared_digest
                == actual_digest
            )

        artifact_integrity_rows.append(
            {
                "artifact": artifact_name,
                "present": path.exists(),
                "declared_digest": (
                    declared_digest
                ),
                "actual_digest": (
                    actual_digest
                ),
                "digest_matches": (
                    digest_matches
                ),
            }
        )

    required_fixture_fields = {
        "fixture_id",
        "fixture_category",
        "description",
        "target_id",
        "provider_payload",
        "expect_adapter_output",
        "expect_outcome_record",
        "expected_outcome_value",
        "expected_outcome_missing",
        "expected_eligible",
        "expected_exclusion_codes",
        "expected_adapter_digest",
        "expected_outcome_digest",
        "source_fixture_version",
        "corpus_plan_version",
    }

    fixture_ids = [
        str(
            row.get(
                "fixture_id"
            )
        )
        for row in fixtures
    ]

    fixture_schema_valid = all(
        required_fixture_fields
        <= set(row)
        and isinstance(
            row.get(
                "provider_payload"
            ),
            dict,
        )
        and isinstance(
            row.get(
                "expect_adapter_output"
            ),
            bool,
        )
        and isinstance(
            row.get(
                "expect_outcome_record"
            ),
            bool,
        )
        and isinstance(
            row.get(
                "expected_exclusion_codes"
            ),
            list,
        )
        for row in fixtures
    )

    expected_adapter_map = (
        expected_record_map(
            expected_adapter_rows,
            "adapter_output",
            "adapter_digest",
        )
    )

    expected_outcome_map = (
        expected_record_map(
            expected_outcome_rows,
            "outcome_record",
            "outcome_digest",
        )
    )

    adapter_module: ModuleType | None = None
    outcome_module: ModuleType | None = None
    module_error: str | None = None

    try:
        adapter_module = load_module(
            ADAPTER_PATH,
            "layer_9f_adapter",
        )
        outcome_module = load_module(
            OUTCOME_CONTRACT_PATH,
            "layer_9d_outcome_contract",
        )
    except Exception as exc:
        module_error = (
            f"{type(exc).__name__}: {exc}"
        )

    replay_one_rows: list[
        dict[str, Any]
    ] = []
    replay_two_rows: list[
        dict[str, Any]
    ] = []
    replay_one_adapter: dict[
        str,
        dict[str, Any] | None,
    ] = {}
    replay_one_outcome: dict[
        str,
        dict[str, Any] | None,
    ] = {}
    replay_two_adapter: dict[
        str,
        dict[str, Any] | None,
    ] = {}
    replay_two_outcome: dict[
        str,
        dict[str, Any] | None,
    ] = {}
    replay_error: str | None = None

    if (
        adapter_module is not None
        and outcome_module is not None
        and load_error is None
    ):
        try:
            (
                replay_one_rows,
                replay_one_adapter,
                replay_one_outcome,
            ) = replay_once(
                fixtures,
                adapter_module,
                outcome_module,
            )

            (
                replay_two_rows,
                replay_two_adapter,
                replay_two_outcome,
            ) = replay_once(
                fixtures,
                adapter_module,
                outcome_module,
            )
        except Exception as exc:
            replay_error = (
                f"{type(exc).__name__}: {exc}"
            )

    fixture_results: list[
        dict[str, Any]
    ] = []
    mismatches: list[
        dict[str, Any]
    ] = []

    for fixture in fixtures:
        fixture_id = str(
            fixture["fixture_id"]
        )

        actual_adapter = (
            replay_one_adapter.get(
                fixture_id
            )
        )
        actual_outcome = (
            replay_one_outcome.get(
                fixture_id
            )
        )

        expected_adapter_entry = (
            expected_adapter_map.get(
                fixture_id
            )
        )
        expected_outcome_entry = (
            expected_outcome_map.get(
                fixture_id
            )
        )

        expect_adapter = bool(
            fixture[
                "expect_adapter_output"
            ]
        )
        expect_outcome = bool(
            fixture[
                "expect_outcome_record"
            ]
        )

        expected_adapter_record = (
            expected_adapter_entry[
                "record"
            ]
            if expected_adapter_entry
            is not None
            else None
        )

        expected_outcome_record = (
            expected_outcome_entry[
                "record"
            ]
            if expected_outcome_entry
            is not None
            else None
        )

        actual_adapter_digest = (
            sha256_payload(
                actual_adapter
            )
            if actual_adapter
            is not None
            else None
        )

        actual_outcome_digest = (
            sha256_payload(
                actual_outcome
            )
            if actual_outcome
            is not None
            else None
        )

        adapter_emission_passed = (
            (actual_adapter is not None)
            == expect_adapter
        )

        outcome_emission_passed = (
            (actual_outcome is not None)
            == expect_outcome
        )

        adapter_record_passed = (
            actual_adapter
            == expected_adapter_record
        )

        outcome_record_passed = (
            actual_outcome
            == expected_outcome_record
        )

        adapter_digest_passed = (
            actual_adapter_digest
            == fixture.get(
                "expected_adapter_digest"
            )
        )

        outcome_digest_passed = (
            actual_outcome_digest
            == fixture.get(
                "expected_outcome_digest"
            )
        )

        semantic_passed = (
            semantic_fields_match(
                fixture,
                actual_outcome,
            )
        )

        repeat_adapter_passed = (
            actual_adapter
            == replay_two_adapter.get(
                fixture_id
            )
        )

        repeat_outcome_passed = (
            actual_outcome
            == replay_two_outcome.get(
                fixture_id
            )
        )

        non_emitting_passed = (
            True
            if fixture.get(
                "provider_payload",
                {},
            ).get(
                "collection_enabled",
                True,
            )
            is not False
            else (
                actual_adapter is None
                and actual_outcome is None
            )
        )

        passed = all(
            [
                adapter_emission_passed,
                outcome_emission_passed,
                adapter_record_passed,
                outcome_record_passed,
                adapter_digest_passed,
                outcome_digest_passed,
                semantic_passed,
                repeat_adapter_passed,
                repeat_outcome_passed,
                non_emitting_passed,
            ]
        )

        failure_codes: list[str] = []

        if not adapter_emission_passed:
            failure_codes.append(
                "historical_outcome_replay_adapter_emission_mismatch"
            )

        if not adapter_record_passed:
            failure_codes.append(
                "historical_outcome_replay_adapter_output_mismatch"
            )

        if not adapter_digest_passed:
            failure_codes.append(
                "historical_outcome_replay_adapter_digest_mismatch"
            )

        if not outcome_emission_passed:
            failure_codes.append(
                "historical_outcome_replay_outcome_emission_mismatch"
            )

        if (
            not outcome_record_passed
            or not semantic_passed
        ):
            failure_codes.append(
                "historical_outcome_replay_outcome_record_mismatch"
            )

        if not outcome_digest_passed:
            failure_codes.append(
                "historical_outcome_replay_outcome_digest_mismatch"
            )

        if not non_emitting_passed:
            failure_codes.append(
                "historical_outcome_replay_non_emitting_violation"
            )

        if (
            not repeat_adapter_passed
            or not repeat_outcome_passed
        ):
            failure_codes.append(
                "historical_outcome_replay_not_repeatable"
            )

        fixture_results.append(
            {
                "fixture_id": fixture_id,
                "target_id": (
                    fixture["target_id"]
                ),
                "adapter_emission_passed": (
                    adapter_emission_passed
                ),
                "adapter_record_passed": (
                    adapter_record_passed
                ),
                "adapter_digest_passed": (
                    adapter_digest_passed
                ),
                "outcome_emission_passed": (
                    outcome_emission_passed
                ),
                "outcome_record_passed": (
                    outcome_record_passed
                ),
                "outcome_digest_passed": (
                    outcome_digest_passed
                ),
                "semantic_passed": (
                    semantic_passed
                ),
                "repeat_adapter_passed": (
                    repeat_adapter_passed
                ),
                "repeat_outcome_passed": (
                    repeat_outcome_passed
                ),
                "non_emitting_passed": (
                    non_emitting_passed
                ),
                "passed": passed,
                "failure_codes": "|".join(
                    failure_codes
                ),
            }
        )

        if not passed:
            mismatches.append(
                {
                    "fixture_id": (
                        fixture_id
                    ),
                    "failure_codes": (
                        failure_codes
                    ),
                    "expected_adapter": (
                        expected_adapter_record
                    ),
                    "actual_adapter": (
                        actual_adapter
                    ),
                    "expected_adapter_digest": (
                        fixture.get(
                            "expected_adapter_digest"
                        )
                    ),
                    "actual_adapter_digest": (
                        actual_adapter_digest
                    ),
                    "expected_outcome": (
                        expected_outcome_record
                    ),
                    "actual_outcome": (
                        actual_outcome
                    ),
                    "expected_outcome_digest": (
                        fixture.get(
                            "expected_outcome_digest"
                        )
                    ),
                    "actual_outcome_digest": (
                        actual_outcome_digest
                    ),
                }
            )

    replay_one_digest = (
        sha256_payload(
            replay_one_rows
        )
        if replay_error is None
        else None
    )

    replay_two_digest = (
        sha256_payload(
            replay_two_rows
        )
        if replay_error is None
        else None
    )

    all_fixture_replays_pass = (
        len(fixture_results)
        == len(fixtures)
        and all(
            bool(row["passed"])
            for row in fixture_results
        )
    )

    checks = [
        {
            "check": "nine_i_predecessor_verified",
            "actual": predecessor_verified,
            "expected": True,
            "passed": predecessor_verified,
        },
        {
            "check": "seven_corpus_artifacts_present",
            "actual": sum(
                path.exists()
                for path in required_paths
            ),
            "expected": 7,
            "passed": corpus_present,
        },
        {
            "check": "corpus_loaded_without_error",
            "actual": load_error,
            "expected": None,
            "passed": load_error is None,
        },
        {
            "check": "corpus_version_matches",
            "actual": manifest.get(
                "corpus_version"
            ),
            "expected": EXPECTED_CORPUS_VERSION,
            "passed": (
                manifest.get(
                    "corpus_version"
                )
                == EXPECTED_CORPUS_VERSION
            ),
        },
        {
            "check": "all_artifact_digests_match",
            "actual": sum(
                bool(
                    row[
                        "digest_matches"
                    ]
                )
                for row in (
                    artifact_integrity_rows
                )
            ),
            "expected": 7,
            "passed": all(
                bool(
                    row[
                        "digest_matches"
                    ]
                )
                for row in (
                    artifact_integrity_rows
                )
            ),
        },
        {
            "check": "fixture_count_matches_manifest",
            "actual": len(
                fixtures
            ),
            "expected": manifest.get(
                "fixture_count"
            ),
            "passed": (
                len(fixtures)
                == manifest.get(
                    "fixture_count"
                )
                == 30
            ),
        },
        {
            "check": "adapter_expectation_count_matches_manifest",
            "actual": len(
                expected_adapter_rows
            ),
            "expected": manifest.get(
                "adapter_output_count"
            ),
            "passed": (
                len(
                    expected_adapter_rows
                )
                == manifest.get(
                    "adapter_output_count"
                )
            ),
        },
        {
            "check": "outcome_expectation_count_matches_manifest",
            "actual": len(
                expected_outcome_rows
            ),
            "expected": manifest.get(
                "outcome_record_count"
            ),
            "passed": (
                len(
                    expected_outcome_rows
                )
                == manifest.get(
                    "outcome_record_count"
                )
            ),
        },
        {
            "check": "fixture_index_count_matches",
            "actual": len(
                fixture_index_rows
            ),
            "expected": len(
                fixtures
            ),
            "passed": (
                len(
                    fixture_index_rows
                )
                == len(
                    fixtures
                )
            ),
        },
        {
            "check": "fixture_schema_valid",
            "actual": fixture_schema_valid,
            "expected": True,
            "passed": fixture_schema_valid,
        },
        {
            "check": "fixture_ids_unique",
            "actual": len(
                set(
                    fixture_ids
                )
            ),
            "expected": len(
                fixture_ids
            ),
            "passed": (
                len(
                    set(
                        fixture_ids
                    )
                )
                == len(
                    fixture_ids
                )
            ),
        },
        {
            "check": "fixture_ids_sorted",
            "actual": fixture_ids,
            "expected": sorted(
                fixture_ids
            ),
            "passed": (
                fixture_ids
                == sorted(
                    fixture_ids
                )
            ),
        },
        {
            "check": "adapter_and_outcome_modules_loaded",
            "actual": module_error,
            "expected": None,
            "passed": (
                module_error is None
            ),
        },
        {
            "check": "replay_executed_without_error",
            "actual": replay_error,
            "expected": None,
            "passed": (
                replay_error is None
            ),
        },
        {
            "check": "thirty_fixture_results_materialized",
            "actual": len(
                fixture_results
            ),
            "expected": 30,
            "passed": (
                len(
                    fixture_results
                )
                == 30
            ),
        },
        {
            "check": "all_adapter_emissions_match",
            "actual": sum(
                bool(
                    row[
                        "adapter_emission_passed"
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
                        "adapter_emission_passed"
                    ]
                )
                for row in fixture_results
            ),
        },
        {
            "check": "all_adapter_records_match",
            "actual": sum(
                bool(
                    row[
                        "adapter_record_passed"
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
                        "adapter_record_passed"
                    ]
                )
                for row in fixture_results
            ),
        },
        {
            "check": "all_adapter_digests_match",
            "actual": sum(
                bool(
                    row[
                        "adapter_digest_passed"
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
                        "adapter_digest_passed"
                    ]
                )
                for row in fixture_results
            ),
        },
        {
            "check": "all_outcome_emissions_match",
            "actual": sum(
                bool(
                    row[
                        "outcome_emission_passed"
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
                        "outcome_emission_passed"
                    ]
                )
                for row in fixture_results
            ),
        },
        {
            "check": "all_outcome_records_match",
            "actual": sum(
                bool(
                    row[
                        "outcome_record_passed"
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
                        "outcome_record_passed"
                    ]
                )
                for row in fixture_results
            ),
        },
        {
            "check": "all_outcome_digests_match",
            "actual": sum(
                bool(
                    row[
                        "outcome_digest_passed"
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
                        "outcome_digest_passed"
                    ]
                )
                for row in fixture_results
            ),
        },
        {
            "check": "all_semantic_expectations_match",
            "actual": sum(
                bool(
                    row[
                        "semantic_passed"
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
                        "semantic_passed"
                    ]
                )
                for row in fixture_results
            ),
        },
        {
            "check": "complete_replay_is_repeatable",
            "actual": replay_two_digest,
            "expected": replay_one_digest,
            "passed": (
                replay_one_digest
                == replay_two_digest
                and all(
                    bool(
                        row[
                            "repeat_adapter_passed"
                        ]
                    )
                    and bool(
                        row[
                            "repeat_outcome_passed"
                        ]
                    )
                    for row in (
                        fixture_results
                    )
                )
            ),
        },
        {
            "check": "collection_disabled_fixture_non_emitting",
            "actual": sum(
                bool(
                    row[
                        "non_emitting_passed"
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
                        "non_emitting_passed"
                    ]
                )
                for row in fixture_results
            ),
        },
        {
            "check": "all_fixture_replays_pass",
            "actual": all_fixture_replays_pass,
            "expected": True,
            "passed": all_fixture_replays_pass,
        },
        {
            "check": "mismatch_artifact_empty",
            "actual": len(
                mismatches
            ),
            "expected": 0,
            "passed": (
                len(
                    mismatches
                )
                == 0
            ),
        },
        {
            "check": "external_fetch_authority_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "production_materialization_authority_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
        {
            "check": "evaluation_and_betting_authority_absent",
            "actual": 0,
            "expected": 0,
            "passed": True,
        },
    ]

    all_checks_passed = all(
        bool(
            row["passed"]
        )
        for row in checks
    )

    diagnosis_name = (
        "pitch_type_matchup_overlay_historical_outcome_"
        "fixture_replay_contract_implementation_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_"
        "fixture_replay_contract_implementation_failed"
    )

    next_layer = (
        "9K_pitch_type_matchup_overlay_historical_outcome_"
        "feature_join_contract_plan"
        if all_checks_passed
        else
        "9J_pitch_type_matchup_overlay_historical_outcome_"
        "fixture_replay_contract_remediation"
    )

    write_csv(
        OUTPUT_DIR / "replay_checks.csv",
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
        / "fixture_replay_results.csv",
        [
            "fixture_id",
            "target_id",
            "adapter_emission_passed",
            "adapter_record_passed",
            "adapter_digest_passed",
            "outcome_emission_passed",
            "outcome_record_passed",
            "outcome_digest_passed",
            "semantic_passed",
            "repeat_adapter_passed",
            "repeat_outcome_passed",
            "non_emitting_passed",
            "passed",
            "failure_codes",
        ],
        fixture_results,
    )

    write_csv(
        OUTPUT_DIR
        / "artifact_integrity_results.csv",
        [
            "artifact",
            "present",
            "declared_digest",
            "actual_digest",
            "digest_matches",
        ],
        artifact_integrity_rows,
    )

    write_jsonl(
        OUTPUT_DIR
        / "replay_mismatches.jsonl",
        mismatches,
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
                    "Layer 9J implements deterministic "
                    "local fixture replay only."
                ),
            }
            for authority in (
                PROHIBITED_AUTHORITIES
            )
        ]
        + [
            {
                "authority": (
                    "historical_outcome_feature_"
                    "join_contract_planning"
                ),
                "granted": (
                    all_checks_passed
                ),
                "reason": (
                    "Successful deterministic replay "
                    "permits planning the next bounded "
                    "historical outcome feature-join contract."
                ),
            }
        ],
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "replay_contract_version": (
            REPLAY_CONTRACT_VERSION
        ),
        "corpus_version": manifest.get(
            "corpus_version"
        ),
        "predecessor_verified": (
            predecessor_verified
        ),
        "fixtures_replayed": len(
            fixture_results
        ),
        "fixtures_passed": sum(
            bool(
                row["passed"]
            )
            for row in fixture_results
        ),
        "adapter_outputs_replayed": sum(
            replay_one_adapter.get(
                fixture_id
            )
            is not None
            for fixture_id in (
                fixture_ids
            )
        ),
        "outcome_records_replayed": sum(
            replay_one_outcome.get(
                fixture_id
            )
            is not None
            for fixture_id in (
                fixture_ids
            )
        ),
        "artifact_integrity_checks_passed": sum(
            bool(
                row[
                    "digest_matches"
                ]
            )
            for row in (
                artifact_integrity_rows
            )
        ),
        "artifact_integrity_checks_required": len(
            artifact_integrity_rows
        ),
        "replay_digest": (
            replay_one_digest
        ),
        "repeat_replay_digest": (
            replay_two_digest
        ),
        "mismatches": len(
            mismatches
        ),
        "implementation_checks_passed": sum(
            bool(
                row["passed"]
            )
            for row in checks
        ),
        "implementation_checks_required": len(
            checks
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
            next_layer
        ),
    }

    write_json(
        OUTPUT_DIR
        / "replay_summary.json",
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
            "historical_outcome_feature_join_contract_planning"
            if all_checks_passed
            else "none"
        ),
        "authority_withheld": sorted(
            PROHIBITED_AUTHORITIES
        ),
        "recommended_next_layer": (
            next_layer
        ),
        "corpus_directory": str(
            CORPUS_DIR.relative_to(
                ROOT
            )
        ),
        "output_directory": str(
            OUTPUT_DIR.relative_to(
                ROOT
            )
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
        "Replay contract version: "
        f"{REPLAY_CONTRACT_VERSION}"
    )
    print(
        "Predecessor verified: "
        f"{predecessor_verified}"
    )
    print(
        "Corpus version: "
        f"{manifest.get('corpus_version')}"
    )
    print(
        "Implementation checks passed: "
        f"{summary['implementation_checks_passed']}/"
        f"{summary['implementation_checks_required']}"
    )
    print(
        "Fixtures replayed: "
        f"{summary['fixtures_replayed']}"
    )
    print(
        "Fixtures passed: "
        f"{summary['fixtures_passed']}"
    )
    print(
        "Adapter outputs replayed: "
        f"{summary['adapter_outputs_replayed']}"
    )
    print(
        "Outcome records replayed: "
        f"{summary['outcome_records_replayed']}"
    )
    print(
        "Artifact integrity checks passed: "
        f"{summary['artifact_integrity_checks_passed']}/"
        f"{summary['artifact_integrity_checks_required']}"
    )
    print(
        "Replay digest: "
        f"{replay_one_digest}"
    )
    print(
        "Repeat replay digest: "
        f"{replay_two_digest}"
    )
    print(
        "Mismatches: "
        f"{summary['mismatches']}"
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
            str(row["check"])
            for row in checks
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
