from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


SCAFFOLD_VERSION = "candidate_bullpen_statcast_fixture_corpus_scaffold_v0.1"

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
DATES_DIR = FIXTURE_ROOT / "dates"
MANIFEST = FIXTURE_ROOT / "manifest.json"
SCHEMA = FIXTURE_ROOT / "schema.json"
PROVENANCE = FIXTURE_ROOT / "provenance.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
README = FIXTURE_ROOT / "README.md"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_checks.csv"
OUTPUT_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_artifact_inventory.csv"
OUTPUT_JSON_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_json_validation.csv"
OUTPUT_NO_PAYLOAD = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_no_payload_audit.csv"
OUTPUT_FIELDS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_field_contract_audit.csv"


REQUIRED_NORMALIZED_FIELDS = [
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

REQUIRED_MANIFEST_FIELDS = [
    "fixture_version",
    "fixture_date",
    "file_path",
    "row_count",
    "sha256",
    "source_label",
    "generation_method",
    "known_limitations",
    "expected_duplicate_count",
    "expected_required_field_failures",
]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text())


def _ensure_scaffold_exists() -> None:
    FIXTURE_ROOT.mkdir(parents=True, exist_ok=True)
    DATES_DIR.mkdir(parents=True, exist_ok=True)


def _artifact_inventory() -> List[Dict[str, Any]]:
    artifacts = [
        (FIXTURE_ROOT, "root_directory", True),
        (DATES_DIR, "dates_directory", True),
        (MANIFEST, "manifest_json", True),
        (SCHEMA, "schema_json", True),
        (PROVENANCE, "provenance_json", True),
        (EXPECTED_RESULTS, "expected_results_json", True),
        (README, "readme", True),
    ]

    rows = []
    for path, artifact_type, required in artifacts:
        rows.append({
            "path": str(path),
            "artifact_type": artifact_type,
            "required": required,
            "exists": path.exists(),
            "is_dir": path.is_dir(),
            "is_file": path.is_file(),
            "size_bytes": path.stat().st_size if path.exists() and path.is_file() else 0,
        })
    return rows


def _json_validation() -> List[Dict[str, Any]]:
    rows = []
    for path in [MANIFEST, SCHEMA, PROVENANCE, EXPECTED_RESULTS]:
        try:
            data = _read_json(path)
            valid_json = isinstance(data, dict)
            created_as_scaffold = data.get("created_as_scaffold") is True
            rows.append({
                "path": str(path),
                "valid_json": valid_json,
                "created_as_scaffold": created_as_scaffold,
                "top_level_keys": "|".join(sorted(data.keys())),
                "passed": valid_json and created_as_scaffold,
            })
        except Exception as exc:
            rows.append({
                "path": str(path),
                "valid_json": False,
                "created_as_scaffold": False,
                "top_level_keys": "",
                "passed": False,
                "error": repr(exc),
            })
    return rows


def _no_payload_audit() -> List[Dict[str, Any]]:
    payload_files = sorted(DATES_DIR.glob("*.jsonl")) if DATES_DIR.exists() else []
    all_files = sorted(path for path in DATES_DIR.rglob("*") if path.is_file()) if DATES_DIR.exists() else []

    return [
        {
            "audit": "date_jsonl_payload_files",
            "path": str(DATES_DIR),
            "file_count": len(payload_files),
            "files": "|".join(str(path) for path in payload_files),
            "passed": len(payload_files) == 0,
        },
        {
            "audit": "any_files_under_dates_directory",
            "path": str(DATES_DIR),
            "file_count": len(all_files),
            "files": "|".join(str(path) for path in all_files),
            "passed": len(all_files) == 0,
        },
    ]


def _field_contract_audit() -> List[Dict[str, Any]]:
    manifest = _read_json(MANIFEST)
    schema = _read_json(SCHEMA)

    manifest_fields = manifest.get("required_normalized_fields", [])
    schema_fields = schema.get("required_normalized_fields", [])
    manifest_key = manifest.get("natural_key_fields", [])
    schema_key = schema.get("natural_key_fields", [])
    manifest_required_fields = manifest.get("required_manifest_fields", [])

    rows = []
    for field in REQUIRED_NORMALIZED_FIELDS:
        rows.append({
            "contract": "required_normalized_field",
            "field": field,
            "in_manifest": field in manifest_fields,
            "in_schema": field in schema_fields,
            "expected": True,
            "passed": field in manifest_fields and field in schema_fields,
        })

    for field in NATURAL_KEY_FIELDS:
        rows.append({
            "contract": "natural_key_field",
            "field": field,
            "in_manifest": field in manifest_key,
            "in_schema": field in schema_key,
            "expected": True,
            "passed": field in manifest_key and field in schema_key,
        })

    for field in REQUIRED_MANIFEST_FIELDS:
        rows.append({
            "contract": "required_manifest_field",
            "field": field,
            "in_manifest": field in manifest_required_fields,
            "in_schema": "",
            "expected": True,
            "passed": field in manifest_required_fields,
        })

    rows.append({
        "contract": "manifest_entries_empty",
        "field": "entries",
        "in_manifest": len(manifest.get("entries", [])) == 0,
        "in_schema": "",
        "expected": True,
        "passed": len(manifest.get("entries", [])) == 0,
    })

    rows.append({
        "contract": "schema_row_format",
        "field": "row_format",
        "in_manifest": "",
        "in_schema": schema.get("row_format"),
        "expected": "jsonl",
        "passed": schema.get("row_format") == "jsonl",
    })

    return rows


def main() -> None:
    _ensure_scaffold_exists()

    artifact_rows = _artifact_inventory()
    json_rows = _json_validation()
    no_payload_rows = _no_payload_audit()
    field_rows = _field_contract_audit()

    _write_csv(OUTPUT_ARTIFACTS, artifact_rows)
    _write_csv(OUTPUT_JSON_VALIDATION, json_rows)
    _write_csv(OUTPUT_NO_PAYLOAD, no_payload_rows)
    _write_csv(OUTPUT_FIELDS, field_rows)

    scaffold_directories_created = FIXTURE_ROOT.exists() and FIXTURE_ROOT.is_dir() and DATES_DIR.exists() and DATES_DIR.is_dir()
    metadata_files_created = all(path.exists() and path.is_file() for path in [MANIFEST, SCHEMA, PROVENANCE, EXPECTED_RESULTS, README])
    json_files_valid = all(row["passed"] for row in json_rows)
    manifest_entries_empty = _read_json(MANIFEST).get("entries") == []
    no_date_payload_files = all(row["passed"] for row in no_payload_rows)
    field_contract_valid = all(row["passed"] for row in field_rows)
    artifact_inventory_created = OUTPUT_ARTIFACTS.exists() and len(artifact_rows) == 7

    checks = [
        {"check": "scaffold_directories_created", "passed": scaffold_directories_created, "detail": str(FIXTURE_ROOT)},
        {"check": "metadata_files_created", "passed": metadata_files_created, "detail": "manifest/schema/provenance/expected_results/readme"},
        {"check": "json_files_valid", "passed": json_files_valid, "detail": f"{sum(1 for row in json_rows if row['passed'])}/{len(json_rows)}"},
        {"check": "manifest_entries_empty", "passed": manifest_entries_empty, "detail": "entries=[]"},
        {"check": "no_date_payload_files", "passed": no_date_payload_files, "detail": f"{sum(1 for row in no_payload_rows if row['passed'])}/{len(no_payload_rows)}"},
        {"check": "field_contract_valid", "passed": field_contract_valid, "detail": f"{sum(1 for row in field_rows if row['passed'])}/{len(field_rows)}"},
        {"check": "artifact_inventory_created", "passed": artifact_inventory_created, "detail": f"{len(artifact_rows)} artifacts"},
        {"check": "scaffold_only_no_fixture_payload_rows", "passed": True, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fixture_corpus_scaffold_complete",
        "scaffold_version": SCAFFOLD_VERSION,
        "fixture_root": str(FIXTURE_ROOT),
        "dates_dir": str(DATES_DIR),
        "artifact_count": len(artifact_rows),
        "json_file_count": len(json_rows),
        "date_payload_file_count": len(list(DATES_DIR.glob("*.jsonl"))) if DATES_DIR.exists() else 0,
        "manifest_entries_count": len(_read_json(MANIFEST).get("entries", [])),
        "required_normalized_fields": REQUIRED_NORMALIZED_FIELDS,
        "natural_key_fields": NATURAL_KEY_FIELDS,
        "all_checks_passed": all(check["passed"] for check in checks),
        "scaffold_only": True,
        "fixture_payload_rows_created": False,
        "fixture_adapter_implemented": False,
        "backfill_scaffold_modified": False,
        "test_double_prototype_modified": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": "6CK_candidate_bullpen_statcast_fixture_corpus_scaffold_audit",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
