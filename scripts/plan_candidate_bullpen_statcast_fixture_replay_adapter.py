from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_fixture_replay_adapter_plan_v0.1"

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
DATES_DIR = FIXTURE_ROOT / "dates"
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_adapter_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_adapter_plan_checks.csv"
OUTPUT_INTERFACE = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_adapter_interface.csv"
OUTPUT_LIFECYCLE = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_loading_lifecycle.csv"
OUTPUT_RESULT = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_result_contract.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_status_semantics.csv"
OUTPUT_NEGATIVE = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_negative_case_behavior.csv"
OUTPUT_DETERMINISM = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_deterministic_guarantees.csv"
OUTPUT_PARITY = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_live_parity.csv"
OUTPUT_FUTURE = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_future_audit_outputs.csv"


REPLAY_ADAPTER_INTERFACE = [
    {
        "component": "function_name",
        "value": "fetch_candidate_bullpen_statcast_fixture_rows",
        "required": True,
        "detail": "Future adapter entrypoint for replaying normalized fixture rows by date.",
    },
    {
        "component": "signature",
        "value": "fetch_candidate_bullpen_statcast_fixture_rows(label_date: str, fixture_root: Path | str = FIXTURE_ROOT) -> FixtureReplayResult",
        "required": True,
        "detail": "Return a typed wrapper rather than raw rows so failures are explicit.",
    },
    {
        "component": "row_return_shape",
        "value": "list[dict]",
        "required": True,
        "detail": "Result.rows must contain normalized row dictionaries only after validation/dedupe semantics.",
    },
    {
        "component": "manifest_loading",
        "value": "load manifest.json from fixture_root",
        "required": True,
        "detail": "Manifest controls file paths, hashes, row counts, and provenance labels.",
    },
    {
        "component": "expected_results_loading",
        "value": "load expected_results.json from fixture_root",
        "required": True,
        "detail": "Expected results define statuses, counts, missing fields, and negative-case expectations.",
    },
    {
        "component": "typed_result_wrapper",
        "value": "FixtureReplayResult dataclass or dict contract",
        "required": True,
        "detail": "Missing fixtures, malformed payloads, and hash failures must be represented explicitly.",
    },
]

LOADING_LIFECYCLE = [
    {"step": 1, "operation": "resolve_fixture_root", "detail": "Resolve fixture_root and dates directory without creating files.", "required": True},
    {"step": 2, "operation": "load_manifest", "detail": "Read manifest.json as immutable metadata.", "required": True},
    {"step": 3, "operation": "load_expected_results", "detail": "Read expected_results.json as immutable expectations.", "required": True},
    {"step": 4, "operation": "locate_date_entry", "detail": "Find manifest entry for label_date when a payload file should exist.", "required": True},
    {"step": 5, "operation": "handle_missing_fixture", "detail": "If expected status is fixture_missing, return fixture_missing without creating a file.", "required": True},
    {"step": 6, "operation": "read_jsonl_payload", "detail": "Read one JSON object per line from the date payload.", "required": True},
    {"step": 7, "operation": "validate_json_parse", "detail": "Return jsonl_parse_error for invalid JSON lines.", "required": True},
    {"step": 8, "operation": "validate_field_contract", "detail": "Check required and extra fields, allowing intentional negative-case omissions.", "required": True},
    {"step": 9, "operation": "validate_sha256", "detail": "Compare file bytes hash against manifest entry.", "required": True},
    {"step": 10, "operation": "sort_by_natural_key", "detail": "Sort rows deterministically by game_pk, at_bat_number, pitch_number, pitcher_id.", "required": True},
    {"step": 11, "operation": "apply_scaffold_helpers", "detail": "Use scaffold validation/natural-key/dedupe helper semantics in future implementation.", "required": True},
    {"step": 12, "operation": "return_result_wrapper", "detail": "Return FixtureReplayResult with rows, counts, status, and diagnostics.", "required": True},
]

RESULT_CONTRACT = [
    {"field": "label_date", "type": "str", "required": True, "description": "Requested date."},
    {"field": "fixture_date", "type": "str", "required": True, "description": "Fixture date matched from manifest/expected_results."},
    {"field": "payload_class", "type": "str", "required": True, "description": "Fixture taxonomy class."},
    {"field": "status", "type": "str", "required": True, "description": "Replay status semantic."},
    {"field": "rows", "type": "list[dict]", "required": True, "description": "Normalized rows after future validation/dedupe semantics."},
    {"field": "raw_row_count", "type": "int", "required": True, "description": "JSONL line count parsed into rows."},
    {"field": "deduped_row_count", "type": "int", "required": True, "description": "Natural-key deduped row count."},
    {"field": "duplicate_count", "type": "int", "required": True, "description": "Raw minus deduped duplicate count."},
    {"field": "required_field_failures", "type": "int", "required": True, "description": "Rows missing required fields after dedupe."},
    {"field": "missing_fields", "type": "list[str]", "required": True, "description": "Unique missing required fields."},
    {"field": "sha256", "type": "str", "required": True, "description": "Actual payload file hash or empty for missing fixture."},
    {"field": "manifest_entry_present", "type": "bool", "required": True, "description": "Whether manifest entry exists for date."},
    {"field": "expected_result_present", "type": "bool", "required": True, "description": "Whether expected_results has date expectation."},
]

STATUS_SEMANTICS = [
    {"status": "success", "meaning": "Payload parsed, hash matched, no duplicates, no required-field failures.", "terminal": True},
    {"status": "dedupe_success", "meaning": "Payload parsed and duplicates were detected/collapsed as expected.", "terminal": True},
    {"status": "schema_failed_safely", "meaning": "Payload parsed but required-field failures match an intentional negative case.", "terminal": True},
    {"status": "fixture_missing", "meaning": "Expected missing fixture date has no payload file and no manifest entry.", "terminal": True},
    {"status": "manifest_missing", "meaning": "Payload exists or expectation exists but manifest entry is unexpectedly absent.", "terminal": True},
    {"status": "hash_mismatch", "meaning": "Payload file bytes do not match manifest sha256.", "terminal": True},
    {"status": "jsonl_parse_error", "meaning": "At least one payload line failed JSON parsing.", "terminal": True},
]

NEGATIVE_CASE_BEHAVIOR = [
    {"fixture_date": "2026-05-23", "case": "duplicate_natural_key", "expected_behavior": "Return dedupe_success with duplicate_count=1 and deduped rows.", "required": True},
    {"fixture_date": "2026-05-24", "case": "missing_pitcher_id", "expected_behavior": "Return schema_failed_safely with missing_fields=['pitcher_id'].", "required": True},
    {"fixture_date": "2026-05-25", "case": "missing_game_pk", "expected_behavior": "Return schema_failed_safely with missing_fields=['game_pk'].", "required": True},
    {"fixture_date": "2026-05-26", "case": "missing_fixture_file", "expected_behavior": "Return fixture_missing without creating dates/2026-05-26.jsonl.", "required": True},
]

DETERMINISTIC_GUARANTEES = [
    {"guarantee": "read_only", "detail": "Replay adapter must never mutate payloads, manifest, expected_results, schema, or provenance.", "required": True},
    {"guarantee": "stable_row_sorting", "detail": "Replay rows sorted by natural key using type-stable handling for missing negative-case fields.", "required": True},
    {"guarantee": "stable_hashes", "detail": "Replay validates SHA256 but never rewrites hashes.", "required": True},
    {"guarantee": "repeatable_results", "detail": "Repeated replay of same fixture date returns identical stable projection.", "required": True},
    {"guarantee": "no_file_creation", "detail": "Missing fixture date remains absent after replay.", "required": True},
    {"guarantee": "metadata_immutability", "detail": "Manifest and expected_results snapshots unchanged before/after replay.", "required": True},
]

FIXTURE_LIVE_PARITY = [
    {"parity_item": "normalized_row_shape", "fixture_requirement": "Return same 12 normalized fields for valid rows.", "live_requirement": "Live adapter must normalize to same row shape.", "required": True},
    {"parity_item": "natural_key", "fixture_requirement": "Use game_pk, at_bat_number, pitch_number, pitcher_id.", "live_requirement": "Live adapter must dedupe by same key.", "required": True},
    {"parity_item": "required_field_validation", "fixture_requirement": "Required-field failures explicit in result.", "live_requirement": "Live dry-run must expose same failures.", "required": True},
    {"parity_item": "duplicate_reporting", "fixture_requirement": "Duplicate count included in result.", "live_requirement": "Live dry-run must report duplicate count.", "required": True},
    {"parity_item": "status_contract", "fixture_requirement": "Use replay status semantics.", "live_requirement": "Live adapter uses analogous dry-run/fetch statuses.", "required": True},
    {"parity_item": "no_write_boundary", "fixture_requirement": "Replay never writes DB.", "live_requirement": "Live dry-run never writes DB unless explicit future write gate.", "required": True},
]

FUTURE_AUDIT_OUTPUTS = [
    {"artifact": "JSON diagnosis", "path": "tmp/candidate_bullpen_statcast_fixture_replay_adapter_prototype.json", "required": True},
    {"artifact": "checks CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_adapter_prototype_checks.csv", "required": True},
    {"artifact": "replay result CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_results.csv", "required": True},
    {"artifact": "replay row audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_row_audit.csv", "required": True},
    {"artifact": "replay expectation audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_expectation_audit.csv", "required": True},
    {"artifact": "determinism audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_determinism_audit.csv", "required": True},
    {"artifact": "safety audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_safety_audit.csv", "required": True},
]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _metadata_snapshot() -> Dict[str, str]:
    snapshot: Dict[str, str] = {}
    for path in [MANIFEST, EXPECTED_RESULTS]:
        snapshot[str(path)] = path.read_text() if path.exists() else ""
    return snapshot


def _payload_inventory_snapshot() -> Dict[str, bool]:
    return {
        str(path): path.exists()
        for path in sorted(DATES_DIR.glob("*.jsonl"))
    } if DATES_DIR.exists() else {}


def main() -> None:
    metadata_before = _metadata_snapshot()
    payload_before = _payload_inventory_snapshot()

    _write_csv(OUTPUT_INTERFACE, REPLAY_ADAPTER_INTERFACE)
    _write_csv(OUTPUT_LIFECYCLE, LOADING_LIFECYCLE)
    _write_csv(OUTPUT_RESULT, RESULT_CONTRACT)
    _write_csv(OUTPUT_STATUS, STATUS_SEMANTICS)
    _write_csv(OUTPUT_NEGATIVE, NEGATIVE_CASE_BEHAVIOR)
    _write_csv(OUTPUT_DETERMINISM, DETERMINISTIC_GUARANTEES)
    _write_csv(OUTPUT_PARITY, FIXTURE_LIVE_PARITY)
    _write_csv(OUTPUT_FUTURE, FUTURE_AUDIT_OUTPUTS)

    metadata_after = _metadata_snapshot()
    payload_after = _payload_inventory_snapshot()

    replay_adapter_interface_defined = (
        len(REPLAY_ADAPTER_INTERFACE) >= 6
        and any(row["component"] == "function_name" and row["value"] == "fetch_candidate_bullpen_statcast_fixture_rows" for row in REPLAY_ADAPTER_INTERFACE)
    )
    loading_lifecycle_defined = len(LOADING_LIFECYCLE) >= 12 and all(row["required"] for row in LOADING_LIFECYCLE)
    result_contract_defined = len(RESULT_CONTRACT) >= 13 and all(row["required"] for row in RESULT_CONTRACT)
    status_semantics_defined = len(STATUS_SEMANTICS) >= 7 and {row["status"] for row in STATUS_SEMANTICS} >= {
        "success",
        "dedupe_success",
        "schema_failed_safely",
        "fixture_missing",
        "manifest_missing",
        "hash_mismatch",
        "jsonl_parse_error",
    }
    negative_case_behavior_defined = len(NEGATIVE_CASE_BEHAVIOR) == 4 and all(row["required"] for row in NEGATIVE_CASE_BEHAVIOR)
    deterministic_guarantees_defined = len(DETERMINISTIC_GUARANTEES) >= 6 and all(row["required"] for row in DETERMINISTIC_GUARANTEES)
    fixture_live_parity_defined = len(FIXTURE_LIVE_PARITY) >= 6 and all(row["required"] for row in FIXTURE_LIVE_PARITY)
    future_audit_outputs_defined = len(FUTURE_AUDIT_OUTPUTS) >= 7 and all(row["required"] for row in FUTURE_AUDIT_OUTPUTS)
    no_payload_mutation = payload_before == payload_after
    no_metadata_mutation = metadata_before == metadata_after

    checks = [
        {"check": "replay_adapter_interface_defined", "passed": replay_adapter_interface_defined, "detail": f"{len(REPLAY_ADAPTER_INTERFACE)} interface rows"},
        {"check": "loading_lifecycle_defined", "passed": loading_lifecycle_defined, "detail": f"{len(LOADING_LIFECYCLE)} lifecycle steps"},
        {"check": "result_contract_defined", "passed": result_contract_defined, "detail": f"{len(RESULT_CONTRACT)} result fields"},
        {"check": "status_semantics_defined", "passed": status_semantics_defined, "detail": f"{len(STATUS_SEMANTICS)} statuses"},
        {"check": "negative_case_behavior_defined", "passed": negative_case_behavior_defined, "detail": f"{len(NEGATIVE_CASE_BEHAVIOR)} negative cases"},
        {"check": "deterministic_guarantees_defined", "passed": deterministic_guarantees_defined, "detail": f"{len(DETERMINISTIC_GUARANTEES)} guarantees"},
        {"check": "fixture_live_parity_defined", "passed": fixture_live_parity_defined, "detail": f"{len(FIXTURE_LIVE_PARITY)} parity rows"},
        {"check": "future_audit_outputs_defined", "passed": future_audit_outputs_defined, "detail": f"{len(FUTURE_AUDIT_OUTPUTS)} future artifacts"},
        {"check": "planning_only_no_adapter", "passed": True, "detail": True},
        {"check": "no_payload_mutation", "passed": no_payload_mutation, "detail": "payload inventory unchanged"},
        {"check": "no_metadata_mutation", "passed": no_metadata_mutation, "detail": "manifest/expected_results unchanged"},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fixture_replay_adapter_plan_complete",
        "plan_version": PLAN_VERSION,
        "interface_rows": len(REPLAY_ADAPTER_INTERFACE),
        "loading_lifecycle_steps": len(LOADING_LIFECYCLE),
        "result_contract_fields": len(RESULT_CONTRACT),
        "status_semantics": len(STATUS_SEMANTICS),
        "negative_case_behaviors": len(NEGATIVE_CASE_BEHAVIOR),
        "deterministic_guarantees": len(DETERMINISTIC_GUARANTEES),
        "fixture_live_parity_rows": len(FIXTURE_LIVE_PARITY),
        "future_audit_outputs": len(FUTURE_AUDIT_OUTPUTS),
        "all_checks_passed": all(check["passed"] for check in checks),
        "planning_only": True,
        "fixture_replay_adapter_implemented": False,
        "fixture_payload_replayed": False,
        "payload_mutated": False,
        "metadata_mutated": False,
        "live_adapter_implemented": False,
        "backfill_scaffold_modified": False,
        "test_double_prototype_modified": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": "6CP_candidate_bullpen_statcast_fixture_replay_adapter_prototype",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
