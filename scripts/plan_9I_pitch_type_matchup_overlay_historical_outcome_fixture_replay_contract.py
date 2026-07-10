#!/usr/bin/env python3
"""
Layer 9I
Pitch-Type Matchup Overlay Historical Outcome Fixture Replay Contract Plan

Plans the deterministic replay contract for the Layer 9H historical outcome
fixture corpus.

Planning only.

This layer does not:

- modify the Layer 9H fixture corpus;
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
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


LAYER_ID = "9I"
LAYER_NAME = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "fixture_replay_contract_plan"
)

REPLAY_PLAN_VERSION = (
    "layer_9I_historical_outcome_fixture_replay_contract_plan_v1"
)

ROOT = Path(__file__).resolve().parents[1]

OUTPUT_DIR = (
    ROOT
    / "tmp"
    / "layer_9I_pitch_type_matchup_overlay_"
    "historical_outcome_fixture_replay_contract_plan"
)

PREDECESSOR_PATH = (
    ROOT
    / "scripts"
    / "audit_9H_pitch_type_matchup_overlay_"
    "historical_outcome_fixture_corpus.py"
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
PROVIDER_PAYLOADS_PATH = (
    CORPUS_DIR / "provider_payloads.jsonl"
)
EXPECTED_ADAPTER_OUTPUTS_PATH = (
    CORPUS_DIR / "expected_adapter_outputs.jsonl"
)
EXPECTED_OUTCOME_RECORDS_PATH = (
    CORPUS_DIR / "expected_outcome_records.jsonl"
)
FIXTURE_INDEX_PATH = CORPUS_DIR / "fixture_index.csv"
README_PATH = CORPUS_DIR / "README.md"

EXPECTED_PREDECESSOR_DIAGNOSIS = (
    "pitch_type_matchup_overlay_historical_outcome_"
    "fixture_corpus_implementation_complete"
)

EXPECTED_PREDECESSOR_AUTHORITY = (
    "historical_outcome_fixture_replay_contract_planning"
)

EXPECTED_CORPUS_VERSION = (
    "layer_9H_historical_outcome_fixture_corpus_v1"
)

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


REPLAY_CONTRACT_DIMENSIONS = [
    {
        "dimension_id": "HREP-D01",
        "dimension": "corpus_discovery",
        "purpose": (
            "Locate the versioned Layer 9H corpus and all required artifacts "
            "without external or production data access."
        ),
    },
    {
        "dimension_id": "HREP-D02",
        "dimension": "manifest_integrity",
        "purpose": (
            "Validate corpus version, fixture counts, artifact inventory, and "
            "manifest-declared digests before replay."
        ),
    },
    {
        "dimension_id": "HREP-D03",
        "dimension": "artifact_integrity",
        "purpose": (
            "Recalculate every corpus artifact SHA-256 digest and compare it "
            "with the immutable manifest."
        ),
    },
    {
        "dimension_id": "HREP-D04",
        "dimension": "fixture_schema_validation",
        "purpose": (
            "Validate required fixture fields, types, fixture identifiers, "
            "target identifiers, and expected-output flags."
        ),
    },
    {
        "dimension_id": "HREP-D05",
        "dimension": "adapter_replay",
        "purpose": (
            "Replay provider payloads through the Layer 9F adapter and compare "
            "canonical adapter outputs with Layer 9H expectations."
        ),
    },
    {
        "dimension_id": "HREP-D06",
        "dimension": "outcome_contract_replay",
        "purpose": (
            "Replay adapter outputs through the Layer 9D historical outcome "
            "contract and compare canonical outcome records."
        ),
    },
    {
        "dimension_id": "HREP-D07",
        "dimension": "non_emitting_behavior",
        "purpose": (
            "Verify collection-disabled fixtures remain non-emitting through "
            "both adapter and outcome replay."
        ),
    },
    {
        "dimension_id": "HREP-D08",
        "dimension": "canonicalization",
        "purpose": (
            "Verify key ordering and irrelevant JSON formatting cannot alter "
            "canonical replay outputs or digests."
        ),
    },
    {
        "dimension_id": "HREP-D09",
        "dimension": "repeatability",
        "purpose": (
            "Execute repeated local replays and require byte-equivalent "
            "canonical results."
        ),
    },
    {
        "dimension_id": "HREP-D10",
        "dimension": "failure_classification",
        "purpose": (
            "Classify discovery, integrity, schema, adapter, outcome, and "
            "determinism failures with stable machine-readable codes."
        ),
    },
    {
        "dimension_id": "HREP-D11",
        "dimension": "diagnostic_artifacts",
        "purpose": (
            "Write replay summaries, per-fixture results, mismatch details, "
            "and authority boundaries to temporary diagnostic artifacts."
        ),
    },
    {
        "dimension_id": "HREP-D12",
        "dimension": "authority_boundary",
        "purpose": (
            "Keep replay local and diagnostic-only, with no production writes, "
            "evaluation joins, metrics, tuning, pricing, or betting authority."
        ),
    },
]


REPLAY_INPUT_ARTIFACTS = [
    {
        "ordinal": 1,
        "artifact": "manifest.json",
        "required": True,
        "role": "corpus inventory, versions, counts, and artifact digests",
    },
    {
        "ordinal": 2,
        "artifact": "schema.json",
        "required": True,
        "role": "fixture corpus schema declaration",
    },
    {
        "ordinal": 3,
        "artifact": "provider_payloads.jsonl",
        "required": True,
        "role": "canonical provider-shaped replay inputs",
    },
    {
        "ordinal": 4,
        "artifact": "expected_adapter_outputs.jsonl",
        "required": True,
        "role": "immutable expected Layer 9F adapter outputs",
    },
    {
        "ordinal": 5,
        "artifact": "expected_outcome_records.jsonl",
        "required": True,
        "role": "immutable expected Layer 9D outcome records",
    },
    {
        "ordinal": 6,
        "artifact": "fixture_index.csv",
        "required": True,
        "role": "human-readable scenario and semantic index",
    },
    {
        "ordinal": 7,
        "artifact": "README.md",
        "required": True,
        "role": "corpus documentation and authority boundary",
    },
]


REPLAY_OUTPUT_ARTIFACTS = [
    {
        "ordinal": 1,
        "artifact": "replay_summary.json",
        "purpose": (
            "Aggregate replay counts, pass/fail state, corpus version, and "
            "recommended next layer."
        ),
    },
    {
        "ordinal": 2,
        "artifact": "fixture_replay_results.csv",
        "purpose": (
            "One row per fixture with adapter, outcome, digest, schema, and "
            "determinism results."
        ),
    },
    {
        "ordinal": 3,
        "artifact": "artifact_integrity_results.csv",
        "purpose": (
            "Manifest-declared and recalculated digest results for each corpus "
            "artifact."
        ),
    },
    {
        "ordinal": 4,
        "artifact": "replay_mismatches.jsonl",
        "purpose": (
            "Machine-readable expected-versus-actual mismatches, empty when "
            "the replay is clean."
        ),
    },
    {
        "ordinal": 5,
        "artifact": "replay_checks.csv",
        "purpose": (
            "Contract-level checks covering discovery, integrity, schema, "
            "adapter, outcome, repeatability, and authority."
        ),
    },
    {
        "ordinal": 6,
        "artifact": "authority_boundaries.csv",
        "purpose": (
            "Explicit granted and withheld authorities for the replay layer."
        ),
    },
    {
        "ordinal": 7,
        "artifact": "diagnosis.json",
        "purpose": (
            "Stable terminal diagnosis and next-layer authority."
        ),
    },
]


FAILURE_CODES = [
    {
        "failure_code": "historical_outcome_replay_corpus_missing",
        "failure_class": "discovery",
        "condition": "The Layer 9H corpus directory or a required artifact is absent.",
    },
    {
        "failure_code": "historical_outcome_replay_manifest_invalid",
        "failure_class": "manifest",
        "condition": "The manifest cannot be parsed or required manifest fields are absent.",
    },
    {
        "failure_code": "historical_outcome_replay_version_mismatch",
        "failure_class": "manifest",
        "condition": "The corpus, adapter, outcome, or plan version is incompatible.",
    },
    {
        "failure_code": "historical_outcome_replay_artifact_digest_mismatch",
        "failure_class": "integrity",
        "condition": "A recalculated artifact SHA-256 differs from the manifest.",
    },
    {
        "failure_code": "historical_outcome_replay_fixture_schema_invalid",
        "failure_class": "schema",
        "condition": "A provider fixture violates the declared fixture schema.",
    },
    {
        "failure_code": "historical_outcome_replay_fixture_id_duplicate",
        "failure_class": "schema",
        "condition": "Provider fixture identifiers are not unique.",
    },
    {
        "failure_code": "historical_outcome_replay_fixture_count_mismatch",
        "failure_class": "manifest",
        "condition": "Parsed fixture or output counts differ from the manifest.",
    },
    {
        "failure_code": "historical_outcome_replay_adapter_emission_mismatch",
        "failure_class": "adapter",
        "condition": "Adapter emission or non-emission differs from the fixture expectation.",
    },
    {
        "failure_code": "historical_outcome_replay_adapter_output_mismatch",
        "failure_class": "adapter",
        "condition": "Canonical adapter replay output differs from the expected output.",
    },
    {
        "failure_code": "historical_outcome_replay_adapter_digest_mismatch",
        "failure_class": "adapter",
        "condition": "The replayed adapter output digest differs from the expected digest.",
    },
    {
        "failure_code": "historical_outcome_replay_outcome_emission_mismatch",
        "failure_class": "outcome_contract",
        "condition": "Outcome-record emission differs from the fixture expectation.",
    },
    {
        "failure_code": "historical_outcome_replay_outcome_record_mismatch",
        "failure_class": "outcome_contract",
        "condition": "Canonical outcome replay record differs from the expected record.",
    },
    {
        "failure_code": "historical_outcome_replay_outcome_digest_mismatch",
        "failure_class": "outcome_contract",
        "condition": "The replayed outcome record digest differs from the expected digest.",
    },
    {
        "failure_code": "historical_outcome_replay_non_emitting_violation",
        "failure_class": "collection_control",
        "condition": "A collection-disabled fixture emits an adapter or outcome record.",
    },
    {
        "failure_code": "historical_outcome_replay_not_repeatable",
        "failure_class": "determinism",
        "condition": "Repeated local replay produces non-equivalent canonical results.",
    },
    {
        "failure_code": "historical_outcome_replay_unexpected_exception",
        "failure_class": "execution",
        "condition": "Replay raises an unclassified exception.",
    },
]


REPLAY_RULES = [
    {
        "rule_id": "HREP-R01",
        "rule": "Replay must read only committed local Layer 9H corpus artifacts.",
    },
    {
        "rule_id": "HREP-R02",
        "rule": "Replay must not fetch external historical outcomes.",
    },
    {
        "rule_id": "HREP-R03",
        "rule": "Replay must validate artifact integrity before fixture execution.",
    },
    {
        "rule_id": "HREP-R04",
        "rule": "Replay must process fixtures in ascending fixture_id order.",
    },
    {
        "rule_id": "HREP-R05",
        "rule": "Replay must use canonical sorted-key compact JSON for record comparison.",
    },
    {
        "rule_id": "HREP-R06",
        "rule": "Replay must compare both structural equality and SHA-256 digests.",
    },
    {
        "rule_id": "HREP-R07",
        "rule": "Replay must distinguish adapter non-emission from an adapter failure.",
    },
    {
        "rule_id": "HREP-R08",
        "rule": "Replay must distinguish outcome non-emission from an outcome failure.",
    },
    {
        "rule_id": "HREP-R09",
        "rule": "Replay must verify the collection-disabled fixture remains non-emitting.",
    },
    {
        "rule_id": "HREP-R10",
        "rule": "Replay must execute every fixture at least twice for repeatability.",
    },
    {
        "rule_id": "HREP-R11",
        "rule": "Replay must preserve expected continuous numeric values without boolean coercion.",
    },
    {
        "rule_id": "HREP-R12",
        "rule": "Replay must preserve expected exclusion-code ordering and uniqueness.",
    },
    {
        "rule_id": "HREP-R13",
        "rule": "Replay must emit one stable failure code per failed contract assertion.",
    },
    {
        "rule_id": "HREP-R14",
        "rule": "Replay mismatch artifacts must be empty when all fixtures pass.",
    },
    {
        "rule_id": "HREP-R15",
        "rule": "Replay diagnostics must be written only beneath the Layer 9J tmp directory.",
    },
    {
        "rule_id": "HREP-R16",
        "rule": "Replay must not modify committed Layer 9H corpus artifacts.",
    },
    {
        "rule_id": "HREP-R17",
        "rule": "Replay must not execute feature/outcome or prediction joins.",
    },
    {
        "rule_id": "HREP-R18",
        "rule": "Replay must not calculate predictive, accuracy, calibration, or value metrics.",
    },
    {
        "rule_id": "HREP-R19",
        "rule": "Replay must not modify production probabilities, simulations, pricing, or betting behavior.",
    },
    {
        "rule_id": "HREP-R20",
        "rule": "Successful replay grants only Layer 9J replay-contract implementation authority.",
    },
]


IMPLEMENTATION_STEPS = [
    {
        "step": 1,
        "action": (
            "Load the Layer 9H manifest and verify corpus, adapter-contract, "
            "and historical-outcome-contract versions."
        ),
    },
    {
        "step": 2,
        "action": (
            "Verify all seven required corpus artifacts are present."
        ),
    },
    {
        "step": 3,
        "action": (
            "Recalculate and validate manifest-declared SHA-256 artifact digests."
        ),
    },
    {
        "step": 4,
        "action": (
            "Parse provider fixtures and expected adapter/outcome JSONL records."
        ),
    },
    {
        "step": 5,
        "action": (
            "Parse the fixture index and establish fixture_id-to-expectation maps."
        ),
    },
    {
        "step": 6,
        "action": (
            "Validate provider fixture required fields, identifier uniqueness, "
            "target coverage, and manifest counts."
        ),
    },
    {
        "step": 7,
        "action": (
            "Load the Layer 9F adapter and Layer 9D historical outcome contract "
            "directly from committed scripts."
        ),
    },
    {
        "step": 8,
        "action": (
            "Replay every provider payload through the adapter and outcome contract."
        ),
    },
    {
        "step": 9,
        "action": (
            "Compare emission behavior, canonical records, semantic fields, and "
            "digests against Layer 9H expectations."
        ),
    },
    {
        "step": 10,
        "action": (
            "Repeat the complete replay and verify byte-equivalent canonical output."
        ),
    },
    {
        "step": 11,
        "action": (
            "Classify all failures with stable replay-contract failure codes."
        ),
    },
    {
        "step": 12,
        "action": (
            "Write temporary replay summaries, per-fixture results, mismatch "
            "details, integrity checks, and diagnosis artifacts."
        ),
    },
    {
        "step": 13,
        "action": (
            "Grant only historical outcome fixture replay-contract implementation "
            "authority when every check passes."
        ),
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
        for line in handle:
            line = line.strip()

            if line:
                rows.append(
                    json.loads(line)
                )

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


def main() -> int:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    predecessor_constants = string_constants(
        PREDECESSOR_PATH
    )

    predecessor_verified = (
        PREDECESSOR_PATH.exists()
        and EXPECTED_PREDECESSOR_DIAGNOSIS
        in predecessor_constants
        and EXPECTED_PREDECESSOR_AUTHORITY
        in predecessor_constants
    )

    required_paths = [
        MANIFEST_PATH,
        SCHEMA_PATH,
        PROVIDER_PAYLOADS_PATH,
        EXPECTED_ADAPTER_OUTPUTS_PATH,
        EXPECTED_OUTCOME_RECORDS_PATH,
        FIXTURE_INDEX_PATH,
        README_PATH,
    ]

    corpus_artifacts_present = all(
        path.exists()
        for path in required_paths
    )

    manifest: dict[str, Any] = {}
    provider_rows: list[dict[str, Any]] = []
    adapter_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    fixture_index_rows: list[dict[str, str]] = []

    if corpus_artifacts_present:
        manifest = read_json(
            MANIFEST_PATH
        )
        provider_rows = read_jsonl(
            PROVIDER_PAYLOADS_PATH
        )
        adapter_rows = read_jsonl(
            EXPECTED_ADAPTER_OUTPUTS_PATH
        )
        outcome_rows = read_jsonl(
            EXPECTED_OUTCOME_RECORDS_PATH
        )
        fixture_index_rows = read_csv(
            FIXTURE_INDEX_PATH
        )

    artifact_integrity_rows: list[
        dict[str, Any]
    ] = []

    manifest_artifact_map = {
        row["artifact"]: row
        for row in manifest.get(
            "artifacts",
            [],
        )
    }

    for artifact in REPLAY_INPUT_ARTIFACTS:
        name = artifact["artifact"]
        path = CORPUS_DIR / name
        declared = manifest_artifact_map.get(
            name
        )

        actual_digest = (
            file_sha256(path)
            if path.exists()
            else None
        )

        declared_digest = (
            declared.get("sha256")
            if declared is not None
            else None
        )

        digest_matches = (
            name == "manifest.json"
            or (
                path.exists()
                and declared_digest
                == actual_digest
            )
        )

        artifact_integrity_rows.append(
            {
                "artifact": name,
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

    fixture_ids = [
        row.get("fixture_id")
        for row in provider_rows
    ]

    target_ids = sorted(
        {
            str(
                row.get("target_id")
            )
            for row in provider_rows
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

    checks = [
        {
            "check": "nine_h_predecessor_verified",
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
            "passed": corpus_artifacts_present,
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
            "check": "manifest_fixture_count_is_thirty",
            "actual": manifest.get(
                "fixture_count"
            ),
            "expected": 30,
            "passed": (
                manifest.get(
                    "fixture_count"
                )
                == 30
            ),
        },
        {
            "check": "provider_fixture_count_matches_manifest",
            "actual": len(
                provider_rows
            ),
            "expected": manifest.get(
                "fixture_count"
            ),
            "passed": (
                len(
                    provider_rows
                )
                == manifest.get(
                    "fixture_count"
                )
            ),
        },
        {
            "check": "adapter_output_count_matches_manifest",
            "actual": len(
                adapter_rows
            ),
            "expected": manifest.get(
                "adapter_output_count"
            ),
            "passed": (
                len(
                    adapter_rows
                )
                == manifest.get(
                    "adapter_output_count"
                )
            ),
        },
        {
            "check": "outcome_record_count_matches_manifest",
            "actual": len(
                outcome_rows
            ),
            "expected": manifest.get(
                "outcome_record_count"
            ),
            "passed": (
                len(
                    outcome_rows
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
                provider_rows
            ),
            "passed": (
                len(
                    fixture_index_rows
                )
                == len(
                    provider_rows
                )
            ),
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
            "check": "all_provider_fixture_fields_present",
            "actual": sum(
                required_fixture_fields
                <= set(row)
                for row in provider_rows
            ),
            "expected": len(
                provider_rows
            ),
            "passed": all(
                required_fixture_fields
                <= set(row)
                for row in provider_rows
            ),
        },
        {
            "check": "all_expected_digests_valid",
            "actual": sum(
                bool(
                    digest is None
                    or SHA256_PATTERN.fullmatch(
                        str(digest)
                    )
                )
                for row in provider_rows
                for digest in (
                    row.get(
                        "expected_adapter_digest"
                    ),
                    row.get(
                        "expected_outcome_digest"
                    ),
                )
            ),
            "expected": (
                len(
                    provider_rows
                )
                * 2
            ),
            "passed": all(
                digest is None
                or bool(
                    SHA256_PATTERN.fullmatch(
                        str(digest)
                    )
                )
                for row in provider_rows
                for digest in (
                    row.get(
                        "expected_adapter_digest"
                    ),
                    row.get(
                        "expected_outcome_digest"
                    ),
                )
            ),
        },
        {
            "check": "manifest_artifact_digests_match",
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
            "expected": len(
                artifact_integrity_rows
            ),
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
            "check": "all_ten_targets_covered",
            "actual": target_ids,
            "expected": [
                f"HOUT-O{number:02d}"
                for number in range(
                    1,
                    11,
                )
            ],
            "passed": target_ids
            == [
                f"HOUT-O{number:02d}"
                for number in range(
                    1,
                    11,
                )
            ],
        },
        {
            "check": "twelve_replay_dimensions_defined",
            "actual": len(
                REPLAY_CONTRACT_DIMENSIONS
            ),
            "expected": 12,
            "passed": len(
                REPLAY_CONTRACT_DIMENSIONS
            )
            == 12,
        },
        {
            "check": "seven_replay_inputs_defined",
            "actual": len(
                REPLAY_INPUT_ARTIFACTS
            ),
            "expected": 7,
            "passed": len(
                REPLAY_INPUT_ARTIFACTS
            )
            == 7,
        },
        {
            "check": "seven_replay_outputs_defined",
            "actual": len(
                REPLAY_OUTPUT_ARTIFACTS
            ),
            "expected": 7,
            "passed": len(
                REPLAY_OUTPUT_ARTIFACTS
            )
            == 7,
        },
        {
            "check": "sixteen_failure_codes_defined",
            "actual": len(
                FAILURE_CODES
            ),
            "expected": 16,
            "passed": len(
                FAILURE_CODES
            )
            == 16,
        },
        {
            "check": "twenty_replay_rules_defined",
            "actual": len(
                REPLAY_RULES
            ),
            "expected": 20,
            "passed": len(
                REPLAY_RULES
            )
            == 20,
        },
        {
            "check": "thirteen_implementation_steps_defined",
            "actual": len(
                IMPLEMENTATION_STEPS
            ),
            "expected": 13,
            "passed": len(
                IMPLEMENTATION_STEPS
            )
            == 13,
        },
        {
            "check": "external_fetch_authority_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "production_materialization_authority_absent",
            "actual": False,
            "expected": False,
            "passed": True,
        },
        {
            "check": "evaluation_and_betting_authority_absent",
            "actual": False,
            "expected": False,
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
        "fixture_replay_contract_plan_complete"
        if all_checks_passed
        else
        "pitch_type_matchup_overlay_historical_outcome_"
        "fixture_replay_contract_plan_failed"
    )

    next_layer = (
        "9J_pitch_type_matchup_overlay_historical_outcome_"
        "fixture_replay_contract_implementation"
        if all_checks_passed
        else
        "9I_pitch_type_matchup_overlay_historical_outcome_"
        "fixture_replay_contract_plan_remediation"
    )

    write_csv(
        OUTPUT_DIR / "planning_checks.csv",
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
        / "replay_contract_dimensions.csv",
        [
            "dimension_id",
            "dimension",
            "purpose",
        ],
        REPLAY_CONTRACT_DIMENSIONS,
    )

    write_csv(
        OUTPUT_DIR
        / "replay_input_artifacts.csv",
        [
            "ordinal",
            "artifact",
            "required",
            "role",
        ],
        REPLAY_INPUT_ARTIFACTS,
    )

    write_csv(
        OUTPUT_DIR
        / "replay_output_artifacts.csv",
        [
            "ordinal",
            "artifact",
            "purpose",
        ],
        REPLAY_OUTPUT_ARTIFACTS,
    )

    write_csv(
        OUTPUT_DIR
        / "failure_codes.csv",
        [
            "failure_code",
            "failure_class",
            "condition",
        ],
        FAILURE_CODES,
    )

    write_csv(
        OUTPUT_DIR
        / "replay_rules.csv",
        [
            "rule_id",
            "rule",
        ],
        REPLAY_RULES,
    )

    write_csv(
        OUTPUT_DIR
        / "implementation_steps.csv",
        [
            "step",
            "action",
        ],
        IMPLEMENTATION_STEPS,
    )

    write_csv(
        OUTPUT_DIR
        / "corpus_integrity_snapshot.csv",
        [
            "artifact",
            "present",
            "declared_digest",
            "actual_digest",
            "digest_matches",
        ],
        artifact_integrity_rows,
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
                    "Layer 9I plans deterministic "
                    "local replay only."
                ),
            }
            for authority in (
                PROHIBITED_AUTHORITIES
            )
        ]
        + [
            {
                "authority": (
                    "historical_outcome_fixture_"
                    "replay_contract_implementation"
                ),
                "granted": all_checks_passed,
                "reason": (
                    "Layer 9J may implement local "
                    "fixture replay and diagnostics "
                    "within the planned boundary."
                ),
            }
        ],
    )

    plan_digest = sha256_payload(
        {
            "replay_plan_version": (
                REPLAY_PLAN_VERSION
            ),
            "dimensions": (
                REPLAY_CONTRACT_DIMENSIONS
            ),
            "inputs": (
                REPLAY_INPUT_ARTIFACTS
            ),
            "outputs": (
                REPLAY_OUTPUT_ARTIFACTS
            ),
            "failure_codes": (
                FAILURE_CODES
            ),
            "rules": REPLAY_RULES,
            "implementation_steps": (
                IMPLEMENTATION_STEPS
            ),
        }
    )

    summary = {
        "layer_id": LAYER_ID,
        "layer_name": LAYER_NAME,
        "replay_plan_version": (
            REPLAY_PLAN_VERSION
        ),
        "predecessor_verified": (
            predecessor_verified
        ),
        "corpus_version": manifest.get(
            "corpus_version"
        ),
        "fixtures_discovered": len(
            provider_rows
        ),
        "adapter_outputs_discovered": len(
            adapter_rows
        ),
        "outcome_records_discovered": len(
            outcome_rows
        ),
        "targets_covered": len(
            target_ids
        ),
        "replay_contract_dimensions": len(
            REPLAY_CONTRACT_DIMENSIONS
        ),
        "replay_input_artifacts": len(
            REPLAY_INPUT_ARTIFACTS
        ),
        "replay_output_artifacts": len(
            REPLAY_OUTPUT_ARTIFACTS
        ),
        "failure_codes": len(
            FAILURE_CODES
        ),
        "replay_rules": len(
            REPLAY_RULES
        ),
        "implementation_steps": len(
            IMPLEMENTATION_STEPS
        ),
        "planning_checks_required": len(
            checks
        ),
        "planning_checks_passed": sum(
            bool(
                row["passed"]
            )
            for row in checks
        ),
        "plan_digest": plan_digest,
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
        OUTPUT_DIR / "summary.json",
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
            "historical_outcome_fixture_"
            "replay_contract_implementation"
            if all_checks_passed
            else
            "none"
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
        OUTPUT_DIR / "diagnosis.json",
        diagnosis,
    )

    print(
        f"Layer: {LAYER_ID} — {LAYER_NAME}"
    )
    print(
        "Replay plan version: "
        f"{REPLAY_PLAN_VERSION}"
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
        "Planning checks passed: "
        f"{summary['planning_checks_passed']}/"
        f"{summary['planning_checks_required']}"
    )
    print(
        "Fixtures discovered: "
        f"{summary['fixtures_discovered']}"
    )
    print(
        "Adapter outputs discovered: "
        f"{summary['adapter_outputs_discovered']}"
    )
    print(
        "Outcome records discovered: "
        f"{summary['outcome_records_discovered']}"
    )
    print(
        "Targets covered: "
        f"{summary['targets_covered']}"
    )
    print(
        "Replay contract dimensions: "
        f"{summary['replay_contract_dimensions']}"
    )
    print(
        "Replay input artifacts: "
        f"{summary['replay_input_artifacts']}"
    )
    print(
        "Replay output artifacts: "
        f"{summary['replay_output_artifacts']}"
    )
    print(
        "Failure codes: "
        f"{summary['failure_codes']}"
    )
    print(
        "Replay rules: "
        f"{summary['replay_rules']}"
    )
    print(
        "Implementation steps: "
        f"{summary['implementation_steps']}"
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
            row["check"]
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
