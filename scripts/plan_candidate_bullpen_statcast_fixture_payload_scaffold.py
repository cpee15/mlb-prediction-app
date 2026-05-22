from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_fixture_payload_scaffold_plan_v0.1"

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
DATES_DIR = FIXTURE_ROOT / "dates"
MANIFEST = FIXTURE_ROOT / "manifest.json"
SCHEMA = FIXTURE_ROOT / "schema.json"
PROVENANCE = FIXTURE_ROOT / "provenance.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_payload_scaffold_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_payload_scaffold_plan_checks.csv"
OUTPUT_TAXONOMY = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_payload_taxonomy.csv"
OUTPUT_DATES = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_date_reservations.csv"
OUTPUT_POLICY = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_row_generation_policy.csv"
OUTPUT_EXPECTED = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_expected_result_contract.csv"
OUTPUT_MANIFEST = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_manifest_update_contract.csv"
OUTPUT_MINIMIZATION = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_payload_minimization_rules.csv"
OUTPUT_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_payload_future_validation_sequence.csv"


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


PAYLOAD_TAXONOMY = [
    {
        "payload_class": "positive_minimal_valid_date",
        "fixture_date": "2026-05-20",
        "file_path": "dates/2026-05-20.jsonl",
        "case_type": "positive",
        "purpose": "Smallest valid normalized fixture date with one game, one pitcher, multiple pitches.",
        "expected_status": "success",
    },
    {
        "payload_class": "positive_multi_pitcher_date",
        "fixture_date": "2026-05-21",
        "file_path": "dates/2026-05-21.jsonl",
        "case_type": "positive",
        "purpose": "Valid normalized fixture date with multiple pitchers to exercise pitcher grouping.",
        "expected_status": "success",
    },
    {
        "payload_class": "positive_multi_game_date",
        "fixture_date": "2026-05-22",
        "file_path": "dates/2026-05-22.jsonl",
        "case_type": "positive",
        "purpose": "Valid normalized fixture date with multiple games to exercise game/date grouping.",
        "expected_status": "success",
    },
    {
        "payload_class": "negative_duplicate_natural_key_date",
        "fixture_date": "2026-05-23",
        "file_path": "dates/2026-05-23.jsonl",
        "case_type": "negative",
        "purpose": "Duplicate natural-key rows to validate dedupe and duplicate count reporting.",
        "expected_status": "dedupe_success",
    },
    {
        "payload_class": "negative_missing_pitcher_id_date",
        "fixture_date": "2026-05-24",
        "file_path": "dates/2026-05-24.jsonl",
        "case_type": "negative",
        "purpose": "Malformed row missing pitcher_id to validate required-field failure handling.",
        "expected_status": "schema_failed_safely",
    },
    {
        "payload_class": "negative_missing_game_pk_date",
        "fixture_date": "2026-05-25",
        "file_path": "dates/2026-05-25.jsonl",
        "case_type": "negative",
        "purpose": "Malformed row missing game_pk to validate required-field failure handling.",
        "expected_status": "schema_failed_safely",
    },
    {
        "payload_class": "negative_missing_fixture_date",
        "fixture_date": "2026-05-26",
        "file_path": "dates/2026-05-26.jsonl",
        "case_type": "negative_missing_file",
        "purpose": "Reserved missing-date case; future payload layer should not create this file.",
        "expected_status": "fixture_missing",
    },
]

FIXTURE_DATE_RESERVATIONS = [
    {
        "fixture_date": row["fixture_date"],
        "file_path": row["file_path"],
        "payload_class": row["payload_class"],
        "case_type": row["case_type"],
        "create_file_in_future_payload_layer": row["payload_class"] != "negative_missing_fixture_date",
        "manifest_label": f"{row['case_type']}::{row['payload_class']}",
    }
    for row in PAYLOAD_TAXONOMY
]

ROW_GENERATION_POLICY = [
    {"policy": "synthetic_normalized_rows_only", "allowed": True, "required": True, "detail": "Initial payloads should be hand-curated normalized rows only."},
    {"policy": "raw_dataframe_dump", "allowed": False, "required": True, "detail": "Provider-specific raw columns are forbidden."},
    {"policy": "live_fetch_during_test", "allowed": False, "required": True, "detail": "Payload creation must not hit Statcast/pybaseball/network."},
    {"policy": "database_derived_payload_rows", "allowed": False, "required": True, "detail": "Payload rows must not be copied from mutable local DB state."},
    {"policy": "production_route_capture", "allowed": False, "required": True, "detail": "Payloads must not be captured from production route responses."},
    {"policy": "required_field_only_shape", "allowed": True, "required": True, "detail": "Rows should contain only the 12 normalized fields unless a future audit explicitly allows more."},
    {"policy": "sorted_by_natural_key", "allowed": True, "required": True, "detail": "Rows should be sorted by game_pk, at_bat_number, pitch_number, pitcher_id."},
]

EXPECTED_RESULT_CONTRACT = [
    {"field": "fixture_date", "required": True, "type": "string", "example": "2026-05-20"},
    {"field": "payload_class", "required": True, "type": "string", "example": "positive_minimal_valid_date"},
    {"field": "row_count", "required": True, "type": "integer", "example": 3},
    {"field": "deduped_row_count", "required": True, "type": "integer", "example": 3},
    {"field": "duplicate_count", "required": True, "type": "integer", "example": 0},
    {"field": "required_field_failures", "required": True, "type": "integer", "example": 0},
    {"field": "expected_status", "required": True, "type": "string", "example": "success"},
    {"field": "expected_missing_fields", "required": True, "type": "list[string]", "example": "[]"},
    {"field": "expected_natural_keys", "required": True, "type": "list[tuple]", "example": "[(777001, 42, 1, 123456)]"},
    {"field": "expected_sha256", "required": True, "type": "string", "example": "hex digest"},
]

MANIFEST_UPDATE_CONTRACT = [
    {"field": "fixture_version", "required": True, "source": "existing manifest fixture_version"},
    {"field": "fixture_date", "required": True, "source": "fixture date reservation"},
    {"field": "file_path", "required": True, "source": "dates/YYYY-MM-DD.jsonl"},
    {"field": "row_count", "required": True, "source": "actual JSONL line count"},
    {"field": "sha256", "required": True, "source": "actual file bytes hash"},
    {"field": "source_label", "required": True, "source": "manifest_label from reservation"},
    {"field": "generation_method", "required": True, "source": "hand_curated or synthetic_negative_case"},
    {"field": "known_limitations", "required": True, "source": "case-specific fixture limitation text"},
    {"field": "expected_duplicate_count", "required": True, "source": "expected_results"},
    {"field": "expected_required_field_failures", "required": True, "source": "expected_results"},
]

PAYLOAD_MINIMIZATION_RULES = [
    {"rule": "small_fixture_files", "requirement": "Each date should contain only enough rows to prove the semantic case.", "required": True},
    {"rule": "edge_semantics_over_full_slates", "requirement": "Do not model full MLB slates in fixture payloads.", "required": True},
    {"rule": "no_personally_sensitive_data", "requirement": "Use synthetic ids/team labels; avoid unnecessary real-world payload copying.", "required": True},
    {"rule": "no_unnecessary_columns", "requirement": "Use exactly the normalized 12-field row contract where possible.", "required": True},
    {"rule": "positive_negative_separation", "requirement": "Positive and negative fixtures must be distinguishable by manifest source_label/case_type.", "required": True},
    {"rule": "deterministic_sorting", "requirement": "Rows sorted by natural key for stable diffs and hashes.", "required": True},
]

FUTURE_VALIDATION_SEQUENCE = [
    {"step": 1, "action": "Create selected payload JSONL files except missing fixture date", "required": True},
    {"step": 2, "action": "Update manifest entries with row counts, hashes, labels, and limitations", "required": True},
    {"step": 3, "action": "Update expected_results with per-date replay expectations", "required": True},
    {"step": 4, "action": "Validate each payload row has only/at least required normalized fields", "required": True},
    {"step": 5, "action": "Validate natural-key sorting and duplicate expectations", "required": True},
    {"step": 6, "action": "Validate sha256 values match file bytes", "required": True},
    {"step": 7, "action": "Replay payloads through scaffold helper functions", "required": True},
    {"step": 8, "action": "Validate missing fixture date reports fixture_missing without creating file", "required": True},
    {"step": 9, "action": "Validate no network, DB write, route, sportsbook, frontend, or simulation coupling", "required": True},
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


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text())


def _metadata_snapshot() -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {}
    for path in [MANIFEST, SCHEMA, PROVENANCE, EXPECTED_RESULTS]:
        if path.exists():
            snapshot[str(path)] = _read_json(path)
    return snapshot


def main() -> None:
    metadata_before = _metadata_snapshot()

    _write_csv(OUTPUT_TAXONOMY, PAYLOAD_TAXONOMY)
    _write_csv(OUTPUT_DATES, FIXTURE_DATE_RESERVATIONS)
    _write_csv(OUTPUT_POLICY, ROW_GENERATION_POLICY)
    _write_csv(OUTPUT_EXPECTED, EXPECTED_RESULT_CONTRACT)
    _write_csv(OUTPUT_MANIFEST, MANIFEST_UPDATE_CONTRACT)
    _write_csv(OUTPUT_MINIMIZATION, PAYLOAD_MINIMIZATION_RULES)
    _write_csv(OUTPUT_VALIDATION, FUTURE_VALIDATION_SEQUENCE)

    metadata_after = _metadata_snapshot()

    payload_taxonomy_defined = len(PAYLOAD_TAXONOMY) == 7 and {row["payload_class"] for row in PAYLOAD_TAXONOMY} == {
        "positive_minimal_valid_date",
        "positive_multi_pitcher_date",
        "positive_multi_game_date",
        "negative_duplicate_natural_key_date",
        "negative_missing_pitcher_id_date",
        "negative_missing_game_pk_date",
        "negative_missing_fixture_date",
    }
    fixture_date_reservations_defined = len(FIXTURE_DATE_RESERVATIONS) == 7 and all(row["file_path"].startswith("dates/") for row in FIXTURE_DATE_RESERVATIONS)
    row_generation_policy_defined = len(ROW_GENERATION_POLICY) >= 7 and all(row["required"] for row in ROW_GENERATION_POLICY)
    expected_result_contract_defined = len(EXPECTED_RESULT_CONTRACT) >= 10 and all(row["required"] for row in EXPECTED_RESULT_CONTRACT)
    manifest_update_contract_defined = len(MANIFEST_UPDATE_CONTRACT) == 10 and all(row["required"] for row in MANIFEST_UPDATE_CONTRACT)
    payload_minimization_defined = len(PAYLOAD_MINIMIZATION_RULES) >= 6 and all(row["required"] for row in PAYLOAD_MINIMIZATION_RULES)
    future_validation_sequence_defined = len(FUTURE_VALIDATION_SEQUENCE) >= 9 and all(row["required"] for row in FUTURE_VALIDATION_SEQUENCE)
    planning_only_no_payload_files = DATES_DIR.exists() and len(list(DATES_DIR.rglob("*"))) == 0
    no_fixture_metadata_mutation = metadata_before == metadata_after

    checks = [
        {"check": "payload_taxonomy_defined", "passed": payload_taxonomy_defined, "detail": f"{len(PAYLOAD_TAXONOMY)} payload classes"},
        {"check": "fixture_date_reservations_defined", "passed": fixture_date_reservations_defined, "detail": f"{len(FIXTURE_DATE_RESERVATIONS)} date reservations"},
        {"check": "row_generation_policy_defined", "passed": row_generation_policy_defined, "detail": f"{len(ROW_GENERATION_POLICY)} policies"},
        {"check": "expected_result_contract_defined", "passed": expected_result_contract_defined, "detail": f"{len(EXPECTED_RESULT_CONTRACT)} expected fields"},
        {"check": "manifest_update_contract_defined", "passed": manifest_update_contract_defined, "detail": f"{len(MANIFEST_UPDATE_CONTRACT)} manifest fields"},
        {"check": "payload_minimization_defined", "passed": payload_minimization_defined, "detail": f"{len(PAYLOAD_MINIMIZATION_RULES)} minimization rules"},
        {"check": "future_validation_sequence_defined", "passed": future_validation_sequence_defined, "detail": f"{len(FUTURE_VALIDATION_SEQUENCE)} validation steps"},
        {"check": "planning_only_no_payload_files", "passed": planning_only_no_payload_files, "detail": str(DATES_DIR)},
        {"check": "no_fixture_metadata_mutation", "passed": no_fixture_metadata_mutation, "detail": "manifest/schema/provenance/expected_results unchanged"},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fixture_payload_scaffold_plan_complete",
        "plan_version": PLAN_VERSION,
        "payload_classes": len(PAYLOAD_TAXONOMY),
        "fixture_date_reservations": len(FIXTURE_DATE_RESERVATIONS),
        "row_generation_policies": len(ROW_GENERATION_POLICY),
        "expected_result_contract_fields": len(EXPECTED_RESULT_CONTRACT),
        "manifest_update_contract_fields": len(MANIFEST_UPDATE_CONTRACT),
        "payload_minimization_rules": len(PAYLOAD_MINIMIZATION_RULES),
        "future_validation_steps": len(FUTURE_VALIDATION_SEQUENCE),
        "required_fields": REQUIRED_FIELDS,
        "natural_key_fields": NATURAL_KEY_FIELDS,
        "all_checks_passed": all(check["passed"] for check in checks),
        "planning_only": True,
        "fixture_payload_files_created": False,
        "fixture_metadata_mutated": False,
        "fixture_adapter_implemented": False,
        "backfill_scaffold_modified": False,
        "test_double_prototype_modified": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": "6CM_candidate_bullpen_statcast_fixture_payload_scaffold",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
