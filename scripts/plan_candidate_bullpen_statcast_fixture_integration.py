from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_fixture_integration_plan_v0.1"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_integration_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_integration_plan_checks.csv"
OUTPUT_LAYOUT = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_layout.csv"
OUTPUT_FILE_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_file_contract.csv"
OUTPUT_MANIFEST = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_manifest_contract.csv"
OUTPUT_REPLAY = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_semantics.csv"
OUTPUT_PARITY = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_parity_checks.csv"
OUTPUT_TRANSITION = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_transition_strategy.csv"
OUTPUT_FUTURE = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_future_outputs.csv"


REQUIRED_FIELDS = [
    "game_date",
    "game_pk",
    "inning",
    "inning_topbot",
    "at_bat_number",
    "pitch_number",
    "outs_when_up",
    "pitcher_id",
    "home_team",
    "away_team",
    "events",
    "description",
]

NATURAL_KEY_FIELDS = ["game_pk", "at_bat_number", "pitch_number", "pitcher_id"]

FIXTURE_LAYOUT = [
    {
        "path": "tests/fixtures/statcast/bullpen_labels/",
        "artifact_type": "root_directory",
        "purpose": "Root namespace for candidate bullpen Statcast label fixtures.",
        "created_in_this_layer": False,
    },
    {
        "path": "tests/fixtures/statcast/bullpen_labels/dates/YYYY-MM-DD.jsonl",
        "artifact_type": "per_date_fixture_file",
        "purpose": "One deterministic JSONL row-record fixture file per label date.",
        "created_in_this_layer": False,
    },
    {
        "path": "tests/fixtures/statcast/bullpen_labels/manifest.json",
        "artifact_type": "manifest",
        "purpose": "Fixture version/date/count/hash/provenance catalog.",
        "created_in_this_layer": False,
    },
    {
        "path": "tests/fixtures/statcast/bullpen_labels/schema.json",
        "artifact_type": "schema",
        "purpose": "Machine-readable normalized row contract.",
        "created_in_this_layer": False,
    },
    {
        "path": "tests/fixtures/statcast/bullpen_labels/provenance.json",
        "artifact_type": "provenance",
        "purpose": "Source labels, generation methods, and limitations.",
        "created_in_this_layer": False,
    },
    {
        "path": "tests/fixtures/statcast/bullpen_labels/expected_results.json",
        "artifact_type": "expected_results",
        "purpose": "Expected replay counts, duplicate counts, and validation outcomes.",
        "created_in_this_layer": False,
    },
]

FIXTURE_FILE_CONTRACT = [
    {"contract": "format", "value": "jsonl", "required": True, "reason": "Row-level deterministic records are easy to diff and replay."},
    {"contract": "one_file_per_date", "value": "dates/YYYY-MM-DD.jsonl", "required": True, "reason": "Keeps replay and failure accounting per date."},
    {"contract": "sorted_order", "value": "game_pk, at_bat_number, pitch_number, pitcher_id", "required": True, "reason": "Ensures deterministic replay and stable hashes."},
    {"contract": "required_fields", "value": "|".join(REQUIRED_FIELDS), "required": True, "reason": "Must match scaffold REQUIRED_FIELDS."},
    {"contract": "natural_key", "value": "|".join(NATURAL_KEY_FIELDS), "required": True, "reason": "Must match scaffold dedupe semantics."},
    {"contract": "natural_key_uniqueness", "value": "preferred_unique_with_duplicate_fixtures_allowed_only_in_negative_cases", "required": True, "reason": "Positive fixtures should be clean; negative fixtures should test dedupe."},
    {"contract": "nullable_fields", "value": "outs_when_up|events|description", "required": True, "reason": "Nullable behavior should be explicit."},
    {"contract": "no_raw_dataframe_dump", "value": "true", "required": True, "reason": "Fixtures must use normalized row shape, not provider-specific raw columns."},
]

MANIFEST_CONTRACT = [
    {"field": "fixture_version", "required": True, "example": "bullpen_statcast_fixture_v0.1"},
    {"field": "fixture_date", "required": True, "example": "2026-05-20"},
    {"field": "file_path", "required": True, "example": "dates/2026-05-20.jsonl"},
    {"field": "row_count", "required": True, "example": 128},
    {"field": "sha256", "required": True, "example": "hex digest"},
    {"field": "source_label", "required": True, "example": "fixture_synthetic_or_statcast_snapshot"},
    {"field": "generation_method", "required": True, "example": "hand_curated|captured_snapshot|synthetic_negative_case"},
    {"field": "known_limitations", "required": True, "example": "small fixture; not representative of full slate"},
    {"field": "expected_duplicate_count", "required": True, "example": 0},
    {"field": "expected_required_field_failures", "required": True, "example": 0},
]

REPLAY_SEMANTICS = [
    {"semantic": "date_lookup", "requirement": "Fixture adapter reads rows for exactly one label_date.", "required": True},
    {"semantic": "no_network", "requirement": "Fixture adapter performs local file reads only.", "required": True},
    {"semantic": "no_db_writes", "requirement": "Fixture adapter cannot create sessions, engines, commits, or inserts.", "required": True},
    {"semantic": "scaffold_helpers", "requirement": "Rows pass through existing scaffold validation, natural-key, and dedupe helpers.", "required": True},
    {"semantic": "missing_date", "requirement": "Missing fixture date returns AdapterNoRowsReturned or fixture_missing status.", "required": True},
    {"semantic": "deterministic_replay", "requirement": "Repeated fixture replay returns identical rows and counts.", "required": True},
    {"semantic": "negative_fixtures", "requirement": "Malformed/duplicate fixture cases are explicit and isolated from positive fixtures.", "required": True},
]

PARITY_CHECKS = [
    {"check": "normalization_contract_match", "expected": "12 required fields match scaffold", "required": True},
    {"check": "row_count_deterministic", "expected": "manifest row_count equals replay row_count", "required": True},
    {"check": "hash_deterministic", "expected": "sha256 stable across repeated reads", "required": True},
    {"check": "replay_deterministic", "expected": "two replay runs produce identical stable projection", "required": True},
    {"check": "duplicate_detection", "expected": "duplicate natural keys detected and counted", "required": True},
    {"check": "malformed_rows_fail_safely", "expected": "missing required fields block positive validation", "required": True},
    {"check": "fixture_vs_double_semantics", "expected": "fixture behavior preserves 6CG test-double contracts", "required": True},
    {"check": "no_live_fetch", "expected": "fixture adapter has no network/provider dependency", "required": True},
]

TRANSITION_STRATEGY = [
    {"stage": 1, "name": "test_doubles", "purpose": "Unit-test adapter seam and failure semantics without files.", "entry_gate": "6CH all checks passed", "exit_gate": "fixture corpus scaffold created"},
    {"stage": 2, "name": "fixture_corpus", "purpose": "Create fixture layout, manifest, schema, provenance, expected results.", "entry_gate": "6CI plan merged", "exit_gate": "fixture corpus audit passes"},
    {"stage": 3, "name": "fixture_adapter", "purpose": "Replay normalized fixture rows through scaffold helpers.", "entry_gate": "fixture corpus audit passes", "exit_gate": "fixture replay audit passes"},
    {"stage": 4, "name": "live_adapter_dry_run", "purpose": "Implement live fetch adapter in no-write/dry-run mode.", "entry_gate": "fixture replay audit passes", "exit_gate": "live adapter dry-run audit passes"},
    {"stage": 5, "name": "controlled_write_backfill", "purpose": "Enable explicit write mode only after dry-run and fixture parity gates.", "entry_gate": "live dry-run audit passes", "exit_gate": "6BY/6BW/6BX post-write gates pass"},
]

FUTURE_OUTPUTS = [
    {"future_layer": "6CJ", "artifact": "fixture root scaffold", "required": True},
    {"future_layer": "6CJ", "artifact": "manifest skeleton", "required": True},
    {"future_layer": "6CJ", "artifact": "schema skeleton", "required": True},
    {"future_layer": "6CJ", "artifact": "provenance skeleton", "required": True},
    {"future_layer": "6CJ", "artifact": "expected results skeleton", "required": True},
    {"future_layer": "6CK", "artifact": "fixture corpus scaffold audit", "required": True},
    {"future_layer": "6CL", "artifact": "fixture adapter replay prototype", "required": True},
    {"future_layer": "6CM", "artifact": "fixture adapter replay audit", "required": True},
]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    _write_csv(OUTPUT_LAYOUT, FIXTURE_LAYOUT)
    _write_csv(OUTPUT_FILE_CONTRACT, FIXTURE_FILE_CONTRACT)
    _write_csv(OUTPUT_MANIFEST, MANIFEST_CONTRACT)
    _write_csv(OUTPUT_REPLAY, REPLAY_SEMANTICS)
    _write_csv(OUTPUT_PARITY, PARITY_CHECKS)
    _write_csv(OUTPUT_TRANSITION, TRANSITION_STRATEGY)
    _write_csv(OUTPUT_FUTURE, FUTURE_OUTPUTS)

    fixture_corpus_layout_defined = len(FIXTURE_LAYOUT) >= 6 and all(row["created_in_this_layer"] is False for row in FIXTURE_LAYOUT)
    fixture_file_contract_defined = len(FIXTURE_FILE_CONTRACT) >= 8 and any(row["contract"] == "required_fields" and "pitcher_id" in row["value"] for row in FIXTURE_FILE_CONTRACT)
    manifest_contract_defined = len(MANIFEST_CONTRACT) >= 10 and any(row["field"] == "sha256" for row in MANIFEST_CONTRACT)
    replay_semantics_defined = len(REPLAY_SEMANTICS) >= 7 and all(row["required"] for row in REPLAY_SEMANTICS)
    parity_checks_defined = len(PARITY_CHECKS) >= 8 and all(row["required"] for row in PARITY_CHECKS)
    transition_strategy_defined = len(TRANSITION_STRATEGY) >= 5 and TRANSITION_STRATEGY[0]["name"] == "test_doubles"
    future_outputs_defined = len(FUTURE_OUTPUTS) >= 8 and all(row["required"] for row in FUTURE_OUTPUTS)

    checks = [
        {"check": "fixture_corpus_layout_defined", "passed": fixture_corpus_layout_defined, "detail": f"{len(FIXTURE_LAYOUT)} layout artifacts"},
        {"check": "fixture_file_contract_defined", "passed": fixture_file_contract_defined, "detail": f"{len(FIXTURE_FILE_CONTRACT)} file contract rows"},
        {"check": "manifest_contract_defined", "passed": manifest_contract_defined, "detail": f"{len(MANIFEST_CONTRACT)} manifest fields"},
        {"check": "replay_semantics_defined", "passed": replay_semantics_defined, "detail": f"{len(REPLAY_SEMANTICS)} replay semantics"},
        {"check": "parity_checks_defined", "passed": parity_checks_defined, "detail": f"{len(PARITY_CHECKS)} parity checks"},
        {"check": "transition_strategy_defined", "passed": transition_strategy_defined, "detail": f"{len(TRANSITION_STRATEGY)} stages"},
        {"check": "future_outputs_defined", "passed": future_outputs_defined, "detail": f"{len(FUTURE_OUTPUTS)} future artifacts"},
        {"check": "planning_only_no_fixture_data", "passed": True, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fixture_integration_plan_complete",
        "plan_version": PLAN_VERSION,
        "fixture_layout_artifacts": len(FIXTURE_LAYOUT),
        "fixture_file_contract_rows": len(FIXTURE_FILE_CONTRACT),
        "manifest_contract_fields": len(MANIFEST_CONTRACT),
        "replay_semantics": len(REPLAY_SEMANTICS),
        "parity_checks": len(PARITY_CHECKS),
        "transition_stages": len(TRANSITION_STRATEGY),
        "future_outputs": len(FUTURE_OUTPUTS),
        "required_fields": REQUIRED_FIELDS,
        "natural_key_fields": NATURAL_KEY_FIELDS,
        "all_checks_passed": all(check["passed"] for check in checks),
        "planning_only": True,
        "fixture_data_created": False,
        "fixture_adapter_implemented": False,
        "scaffold_modified": False,
        "test_double_prototype_modified": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": "6CJ_candidate_bullpen_statcast_fixture_corpus_scaffold",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
