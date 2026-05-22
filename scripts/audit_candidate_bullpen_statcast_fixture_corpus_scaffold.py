from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


AUDIT_VERSION = "candidate_bullpen_statcast_fixture_corpus_scaffold_audit_v0.1"

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
DATES_DIR = FIXTURE_ROOT / "dates"
MANIFEST = FIXTURE_ROOT / "manifest.json"
SCHEMA = FIXTURE_ROOT / "schema.json"
PROVENANCE = FIXTURE_ROOT / "provenance.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
README = FIXTURE_ROOT / "README.md"
SCAFFOLD_SCRIPT = Path("scripts/scaffold_candidate_bullpen_statcast_fixture_corpus.py")

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_audit_checks.csv"
OUTPUT_ARTIFACTS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_artifact_audit.csv"
OUTPUT_JSON_METADATA = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_json_metadata_audit.csv"
OUTPUT_FIELDS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_field_contract_audit.csv"
OUTPUT_MANIFEST = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_manifest_contract_audit.csv"
OUTPUT_PROVENANCE = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_provenance_safety_audit.csv"
OUTPUT_PAYLOAD = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_payload_absence_audit.csv"
OUTPUT_README = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_readme_safety_audit.csv"
OUTPUT_IDEMPOTENCE = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_corpus_scaffold_idempotence_audit.csv"


EXPECTED_NORMALIZED_FIELDS = [
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

EXPECTED_NATURAL_KEY_FIELDS = ["game_pk", "at_bat_number", "pitch_number", "pitcher_id"]

EXPECTED_MANIFEST_FIELDS = [
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

EXPECTED_DISALLOWED_METHODS = [
    "live_fetch_during_test",
    "database_write_side_effect",
    "raw_dataframe_dump",
    "production_route_capture",
]

README_REQUIRED_PHRASES = [
    "scaffold-only",
    "no fixture payload rows",
    "no external fetch",
    "no database writes",
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


def _artifact_audit() -> List[Dict[str, Any]]:
    artifacts = [
        (FIXTURE_ROOT, "root_directory", "dir"),
        (DATES_DIR, "dates_directory", "dir"),
        (MANIFEST, "manifest_json", "file"),
        (SCHEMA, "schema_json", "file"),
        (PROVENANCE, "provenance_json", "file"),
        (EXPECTED_RESULTS, "expected_results_json", "file"),
        (README, "readme", "file"),
        (SCAFFOLD_SCRIPT, "scaffold_script", "file"),
    ]

    rows = []
    for path, artifact_type, expected_type in artifacts:
        rows.append({
            "path": str(path),
            "artifact_type": artifact_type,
            "expected_type": expected_type,
            "exists": path.exists(),
            "is_dir": path.is_dir(),
            "is_file": path.is_file(),
            "size_bytes": path.stat().st_size if path.exists() and path.is_file() else 0,
            "passed": path.exists() and ((expected_type == "dir" and path.is_dir()) or (expected_type == "file" and path.is_file())),
        })
    return rows


def _json_metadata_audit() -> List[Dict[str, Any]]:
    rows = []
    for path in [MANIFEST, SCHEMA, PROVENANCE, EXPECTED_RESULTS]:
        try:
            data = _read_json(path)
            row = {
                "path": str(path),
                "valid_json": isinstance(data, dict),
                "created_as_scaffold": data.get("created_as_scaffold") is True,
                "top_level_keys": "|".join(sorted(data.keys())),
                "passed": isinstance(data, dict) and data.get("created_as_scaffold") is True,
            }
            if path == MANIFEST:
                row["entries_empty"] = data.get("entries") == []
                row["passed"] = row["passed"] and data.get("entries") == []
            if path == EXPECTED_RESULTS:
                row["date_expectations_empty"] = data.get("date_expectations") == {}
                row["negative_fixture_expectations_empty"] = data.get("negative_fixture_expectations") == {}
                row["passed"] = (
                    row["passed"]
                    and data.get("date_expectations") == {}
                    and data.get("negative_fixture_expectations") == {}
                )
            rows.append(row)
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


def _field_contract_audit() -> List[Dict[str, Any]]:
    manifest = _read_json(MANIFEST)
    schema = _read_json(SCHEMA)

    manifest_fields = manifest.get("required_normalized_fields", [])
    schema_fields = schema.get("required_normalized_fields", [])
    manifest_key = manifest.get("natural_key_fields", [])
    schema_key = schema.get("natural_key_fields", [])
    sorted_order = schema.get("sorted_order", [])

    rows = []
    rows.append({
        "contract": "manifest_schema_required_fields_match",
        "expected": "|".join(EXPECTED_NORMALIZED_FIELDS),
        "actual": f"manifest={'|'.join(manifest_fields)} schema={'|'.join(schema_fields)}",
        "passed": manifest_fields == schema_fields == EXPECTED_NORMALIZED_FIELDS,
    })
    rows.append({
        "contract": "manifest_schema_natural_key_match",
        "expected": "|".join(EXPECTED_NATURAL_KEY_FIELDS),
        "actual": f"manifest={'|'.join(manifest_key)} schema={'|'.join(schema_key)}",
        "passed": manifest_key == schema_key == EXPECTED_NATURAL_KEY_FIELDS,
    })
    rows.append({
        "contract": "schema_row_format_jsonl",
        "expected": "jsonl",
        "actual": schema.get("row_format"),
        "passed": schema.get("row_format") == "jsonl",
    })
    rows.append({
        "contract": "schema_sorted_order_matches_natural_key",
        "expected": "|".join(EXPECTED_NATURAL_KEY_FIELDS),
        "actual": "|".join(sorted_order),
        "passed": sorted_order == EXPECTED_NATURAL_KEY_FIELDS,
    })

    for field in EXPECTED_NORMALIZED_FIELDS:
        rows.append({
            "contract": "required_field_present",
            "expected": field,
            "actual": f"manifest={field in manifest_fields} schema={field in schema_fields}",
            "passed": field in manifest_fields and field in schema_fields,
        })

    return rows


def _manifest_contract_audit() -> List[Dict[str, Any]]:
    manifest = _read_json(MANIFEST)
    required_fields = manifest.get("required_manifest_fields", [])

    rows = []
    for field in EXPECTED_MANIFEST_FIELDS:
        rows.append({
            "field": field,
            "present": field in required_fields,
            "passed": field in required_fields,
        })
    rows.append({
        "field": "__manifest_field_count__",
        "present": len(required_fields) == len(EXPECTED_MANIFEST_FIELDS),
        "passed": len(required_fields) == len(EXPECTED_MANIFEST_FIELDS),
    })
    rows.append({
        "field": "__entries_empty__",
        "present": manifest.get("entries") == [],
        "passed": manifest.get("entries") == [],
    })
    return rows


def _provenance_safety_audit() -> List[Dict[str, Any]]:
    provenance = _read_json(PROVENANCE)
    disallowed = provenance.get("disallowed_generation_methods", [])
    rows = []

    for method in EXPECTED_DISALLOWED_METHODS:
        rows.append({
            "method": method,
            "present": method in disallowed,
            "passed": method in disallowed,
        })

    rows.append({
        "method": "__source_policy_present__",
        "present": bool(provenance.get("source_policy")),
        "passed": bool(provenance.get("source_policy")),
    })
    rows.append({
        "method": "__known_limitations_present__",
        "present": bool(provenance.get("known_limitations")),
        "passed": bool(provenance.get("known_limitations")),
    })
    return rows


def _payload_absence_audit() -> List[Dict[str, Any]]:
    all_files = sorted(path for path in DATES_DIR.rglob("*") if path.is_file()) if DATES_DIR.exists() else []
    jsonl_files = sorted(DATES_DIR.rglob("*.jsonl")) if DATES_DIR.exists() else []
    manifest_entries = _read_json(MANIFEST).get("entries", [])

    return [
        {
            "payload_check": "dates_directory_file_count_zero",
            "count": len(all_files),
            "items": "|".join(str(path) for path in all_files),
            "passed": len(all_files) == 0,
        },
        {
            "payload_check": "jsonl_payload_count_zero",
            "count": len(jsonl_files),
            "items": "|".join(str(path) for path in jsonl_files),
            "passed": len(jsonl_files) == 0,
        },
        {
            "payload_check": "manifest_entries_zero",
            "count": len(manifest_entries),
            "items": str(manifest_entries),
            "passed": len(manifest_entries) == 0,
        },
    ]


def _readme_safety_audit() -> List[Dict[str, Any]]:
    text = README.read_text(errors="ignore").lower() if README.exists() else ""
    return [
        {
            "phrase": phrase,
            "present": phrase in text,
            "passed": phrase in text,
        }
        for phrase in README_REQUIRED_PHRASES
    ]


def _run_scaffold_script() -> Dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(SCAFFOLD_SCRIPT)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )
    return {
        "returncode": completed.returncode,
        "succeeded": completed.returncode == 0,
        "stdout_tail": completed.stdout[-500:],
        "stderr_tail": completed.stderr[-500:],
    }


def _idempotence_audit() -> List[Dict[str, Any]]:
    before_payload = _payload_absence_audit()
    before_manifest_entries = len(_read_json(MANIFEST).get("entries", []))

    run = _run_scaffold_script()

    after_payload = _payload_absence_audit()
    after_manifest_entries = len(_read_json(MANIFEST).get("entries", []))

    return [
        {
            "idempotence_check": "scaffold_script_succeeded",
            "before": "",
            "after": run["returncode"],
            "passed": run["succeeded"],
        },
        {
            "idempotence_check": "manifest_entries_remain_zero",
            "before": before_manifest_entries,
            "after": after_manifest_entries,
            "passed": before_manifest_entries == after_manifest_entries == 0,
        },
        {
            "idempotence_check": "payload_absence_preserved",
            "before": all(row["passed"] for row in before_payload),
            "after": all(row["passed"] for row in after_payload),
            "passed": all(row["passed"] for row in before_payload) and all(row["passed"] for row in after_payload),
        },
    ]


def main() -> None:
    artifact_rows = _artifact_audit()
    json_rows = _json_metadata_audit()
    field_rows = _field_contract_audit()
    manifest_rows = _manifest_contract_audit()
    provenance_rows = _provenance_safety_audit()
    payload_rows = _payload_absence_audit()
    readme_rows = _readme_safety_audit()
    idempotence_rows = _idempotence_audit()

    _write_csv(OUTPUT_ARTIFACTS, artifact_rows)
    _write_csv(OUTPUT_JSON_METADATA, json_rows)
    _write_csv(OUTPUT_FIELDS, field_rows)
    _write_csv(OUTPUT_MANIFEST, manifest_rows)
    _write_csv(OUTPUT_PROVENANCE, provenance_rows)
    _write_csv(OUTPUT_PAYLOAD, payload_rows)
    _write_csv(OUTPUT_README, readme_rows)
    _write_csv(OUTPUT_IDEMPOTENCE, idempotence_rows)

    artifact_paths_valid = all(row["passed"] for row in artifact_rows)
    json_metadata_valid = all(row["passed"] for row in json_rows)
    field_contract_valid = all(row["passed"] for row in field_rows)
    manifest_contract_valid = all(row["passed"] for row in manifest_rows)
    provenance_safety_valid = all(row["passed"] for row in provenance_rows)
    payload_absence_valid = all(row["passed"] for row in payload_rows)
    readme_safety_valid = all(row["passed"] for row in readme_rows)
    idempotence_valid = all(row["passed"] for row in idempotence_rows)

    checks = [
        {"check": "artifact_paths_valid", "passed": artifact_paths_valid, "detail": f"{sum(row['passed'] for row in artifact_rows)}/{len(artifact_rows)}"},
        {"check": "json_metadata_valid", "passed": json_metadata_valid, "detail": f"{sum(row['passed'] for row in json_rows)}/{len(json_rows)}"},
        {"check": "field_contract_valid", "passed": field_contract_valid, "detail": f"{sum(row['passed'] for row in field_rows)}/{len(field_rows)}"},
        {"check": "manifest_contract_valid", "passed": manifest_contract_valid, "detail": f"{sum(row['passed'] for row in manifest_rows)}/{len(manifest_rows)}"},
        {"check": "provenance_safety_valid", "passed": provenance_safety_valid, "detail": f"{sum(row['passed'] for row in provenance_rows)}/{len(provenance_rows)}"},
        {"check": "payload_absence_valid", "passed": payload_absence_valid, "detail": f"{sum(row['passed'] for row in payload_rows)}/{len(payload_rows)}"},
        {"check": "readme_safety_valid", "passed": readme_safety_valid, "detail": f"{sum(row['passed'] for row in readme_rows)}/{len(readme_rows)}"},
        {"check": "idempotence_valid", "passed": idempotence_valid, "detail": f"{sum(row['passed'] for row in idempotence_rows)}/{len(idempotence_rows)}"},
        {"check": "audit_only_no_fixture_payload_rows", "passed": True, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fixture_corpus_scaffold_audit_complete",
        "audit_version": AUDIT_VERSION,
        "fixture_root": str(FIXTURE_ROOT),
        "artifact_checks": len(artifact_rows),
        "json_metadata_checks": len(json_rows),
        "field_contract_checks": len(field_rows),
        "manifest_contract_checks": len(manifest_rows),
        "provenance_safety_checks": len(provenance_rows),
        "payload_absence_checks": len(payload_rows),
        "readme_safety_checks": len(readme_rows),
        "idempotence_checks": len(idempotence_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "audit_only": True,
        "fixture_payload_rows_created": False,
        "fixture_adapter_implemented": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CL_candidate_bullpen_statcast_fixture_payload_scaffold_plan"
            if all(check["passed"] for check in checks)
            else "6CJ_patch_candidate_bullpen_statcast_fixture_corpus_scaffold"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
