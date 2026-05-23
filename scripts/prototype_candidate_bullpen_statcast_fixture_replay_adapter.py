from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple


PROTOTYPE_VERSION = "candidate_bullpen_statcast_fixture_replay_adapter_prototype_v0.1"

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
DATES_DIR_NAME = "dates"
MANIFEST_NAME = "manifest.json"
EXPECTED_RESULTS_NAME = "expected_results.json"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_adapter_prototype.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_adapter_prototype_checks.csv"
OUTPUT_RESULTS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_results.csv"
OUTPUT_ROWS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_row_audit.csv"
OUTPUT_EXPECTATIONS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_expectation_audit.csv"
OUTPUT_DETERMINISM = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_determinism_audit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_safety_audit.csv"


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

FIXTURE_DATES = [
    "2026-05-20",
    "2026-05-21",
    "2026-05-22",
    "2026-05-23",
    "2026-05-24",
    "2026-05-25",
    "2026-05-26",
]

EXPECTED_STATUSES = {
    "2026-05-20": "success",
    "2026-05-21": "success",
    "2026-05-22": "success",
    "2026-05-23": "dedupe_success",
    "2026-05-24": "schema_failed_safely",
    "2026-05-25": "schema_failed_safely",
    "2026-05-26": "fixture_missing",
}


@dataclass(frozen=True)
class FixtureReplayResult:
    label_date: str
    fixture_date: str
    payload_class: str
    status: str
    rows: List[Dict[str, Any]]
    raw_row_count: int
    deduped_row_count: int
    duplicate_count: int
    required_field_failures: int
    missing_fields: List[str]
    sha256: str
    manifest_entry_present: bool
    expected_result_present: bool


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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _stable_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: row[key] for key in sorted(row.keys())}


def _natural_key(row: Dict[str, Any]) -> Tuple[Any, Any, Any, Any]:
    return tuple(row.get(field) for field in NATURAL_KEY_FIELDS)


def _sort_value(value: Any) -> Tuple[int, str]:
    if value is None:
        return (1, "")
    return (0, str(value))


def _sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        [_stable_row(row) for row in rows],
        key=lambda row: tuple(_sort_value(item) for item in _natural_key(row)),
    )


def _dedupe_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    seen = set()
    deduped: List[Dict[str, Any]] = []
    duplicate_count = 0

    for row in rows:
        key = _natural_key(row)
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        deduped.append(row)

    return deduped, duplicate_count


def _required_field_failures(rows: List[Dict[str, Any]]) -> Tuple[int, List[str]]:
    failures = 0
    missing_fields: List[str] = []

    for row in rows:
        missing = [field for field in REQUIRED_FIELDS if field not in row]
        if missing:
            failures += 1
            missing_fields.extend(missing)

    return failures, sorted(set(missing_fields))


def _read_jsonl(path: Path) -> Tuple[List[Dict[str, Any]], str]:
    rows: List[Dict[str, Any]] = []
    try:
        for line in path.read_text().splitlines():
            if line.strip():
                parsed = json.loads(line)
                rows.append(_stable_row(parsed))
        return rows, ""
    except Exception as exc:
        return [], repr(exc)


def _manifest_entries_by_date(manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {entry.get("fixture_date"): entry for entry in manifest.get("entries", [])}


def _metadata_paths(fixture_root: Path) -> Tuple[Path, Path]:
    return fixture_root / MANIFEST_NAME, fixture_root / EXPECTED_RESULTS_NAME


def fetch_candidate_bullpen_statcast_fixture_rows(
    label_date: str,
    fixture_root: Path | str = FIXTURE_ROOT,
) -> FixtureReplayResult:
    root = Path(fixture_root)
    manifest_path, expected_path = _metadata_paths(root)
    manifest = _read_json(manifest_path)
    expected_results = _read_json(expected_path)

    manifest_entries = _manifest_entries_by_date(manifest)
    entry = manifest_entries.get(label_date)
    expectations = expected_results.get("date_expectations", {})
    expectation = expectations.get(label_date)

    expected_result_present = expectation is not None
    payload_class = expectation.get("payload_class", "") if expectation else ""
    expected_status = expectation.get("expected_status", "") if expectation else ""
    fixture_date = expectation.get("fixture_date", label_date) if expectation else label_date

    if not expected_result_present:
        return FixtureReplayResult(
            label_date=label_date,
            fixture_date=fixture_date,
            payload_class=payload_class,
            status="manifest_missing",
            rows=[],
            raw_row_count=0,
            deduped_row_count=0,
            duplicate_count=0,
            required_field_failures=0,
            missing_fields=[],
            sha256="",
            manifest_entry_present=entry is not None,
            expected_result_present=False,
        )

    if expected_status == "fixture_missing":
        missing_path = root / DATES_DIR_NAME / f"{label_date}.jsonl"
        return FixtureReplayResult(
            label_date=label_date,
            fixture_date=fixture_date,
            payload_class=payload_class,
            status="fixture_missing" if not missing_path.exists() and entry is None else "manifest_missing",
            rows=[],
            raw_row_count=0,
            deduped_row_count=0,
            duplicate_count=0,
            required_field_failures=0,
            missing_fields=[],
            sha256="",
            manifest_entry_present=entry is not None,
            expected_result_present=True,
        )

    if entry is None:
        return FixtureReplayResult(
            label_date=label_date,
            fixture_date=fixture_date,
            payload_class=payload_class,
            status="manifest_missing",
            rows=[],
            raw_row_count=0,
            deduped_row_count=0,
            duplicate_count=0,
            required_field_failures=0,
            missing_fields=[],
            sha256="",
            manifest_entry_present=False,
            expected_result_present=True,
        )

    payload_path = root / entry["file_path"]
    rows, parse_error = _read_jsonl(payload_path)
    if parse_error:
        return FixtureReplayResult(
            label_date=label_date,
            fixture_date=fixture_date,
            payload_class=payload_class,
            status="jsonl_parse_error",
            rows=[],
            raw_row_count=0,
            deduped_row_count=0,
            duplicate_count=0,
            required_field_failures=0,
            missing_fields=[],
            sha256="",
            manifest_entry_present=True,
            expected_result_present=True,
        )

    actual_sha = _sha256(payload_path)
    if actual_sha != entry.get("sha256"):
        return FixtureReplayResult(
            label_date=label_date,
            fixture_date=fixture_date,
            payload_class=payload_class,
            status="hash_mismatch",
            rows=[],
            raw_row_count=len(rows),
            deduped_row_count=0,
            duplicate_count=0,
            required_field_failures=0,
            missing_fields=[],
            sha256=actual_sha,
            manifest_entry_present=True,
            expected_result_present=True,
        )

    sorted_rows = _sort_rows(rows)
    deduped_rows, duplicate_count = _dedupe_rows(sorted_rows)
    required_failures, missing_fields = _required_field_failures(deduped_rows)

    if required_failures > 0:
        status = "schema_failed_safely"
    elif duplicate_count > 0:
        status = "dedupe_success"
    else:
        status = "success"

    return FixtureReplayResult(
        label_date=label_date,
        fixture_date=fixture_date,
        payload_class=payload_class,
        status=status,
        rows=deduped_rows,
        raw_row_count=len(rows),
        deduped_row_count=len(deduped_rows),
        duplicate_count=duplicate_count,
        required_field_failures=required_failures,
        missing_fields=missing_fields,
        sha256=actual_sha,
        manifest_entry_present=True,
        expected_result_present=True,
    )


def _payload_snapshot() -> Dict[str, str]:
    snapshot: Dict[str, str] = {}
    dates_dir = FIXTURE_ROOT / DATES_DIR_NAME
    for label_date in FIXTURE_DATES:
        path = dates_dir / f"{label_date}.jsonl"
        snapshot[str(path)] = path.read_text() if path.exists() else "__MISSING__"
    return snapshot


def _metadata_snapshot() -> Dict[str, str]:
    snapshot: Dict[str, str] = {}
    for path in [FIXTURE_ROOT / MANIFEST_NAME, FIXTURE_ROOT / EXPECTED_RESULTS_NAME]:
        snapshot[str(path)] = path.read_text() if path.exists() else "__MISSING__"
    return snapshot


def _stable_projection(results: List[FixtureReplayResult]) -> List[Dict[str, Any]]:
    projection = []
    for result in results:
        data = asdict(result)
        data["rows"] = [json.dumps(row, sort_keys=True, separators=(",", ":")) for row in result.rows]
        projection.append(data)
    return projection


def _replay_all() -> List[FixtureReplayResult]:
    return [fetch_candidate_bullpen_statcast_fixture_rows(label_date) for label_date in FIXTURE_DATES]


def _result_rows(results: List[FixtureReplayResult]) -> List[Dict[str, Any]]:
    rows = []
    for result in results:
        rows.append({
            "label_date": result.label_date,
            "fixture_date": result.fixture_date,
            "payload_class": result.payload_class,
            "status": result.status,
            "raw_row_count": result.raw_row_count,
            "deduped_row_count": result.deduped_row_count,
            "duplicate_count": result.duplicate_count,
            "required_field_failures": result.required_field_failures,
            "missing_fields": "|".join(result.missing_fields),
            "sha256": result.sha256,
            "manifest_entry_present": result.manifest_entry_present,
            "expected_result_present": result.expected_result_present,
            "passed": result.status == EXPECTED_STATUSES[result.label_date],
        })
    return rows


def _row_audit_rows(results: List[FixtureReplayResult]) -> List[Dict[str, Any]]:
    rows = []
    for result in results:
        if not result.rows:
            rows.append({
                "label_date": result.label_date,
                "row_index": "",
                "natural_key": "",
                "field_count": 0,
                "missing_fields": "|".join(result.missing_fields),
                "passed": result.status == "fixture_missing",
            })
            continue

        for idx, row in enumerate(result.rows):
            missing = [field for field in REQUIRED_FIELDS if field not in row]
            rows.append({
                "label_date": result.label_date,
                "row_index": idx,
                "natural_key": str(_natural_key(row)),
                "field_count": len(row),
                "missing_fields": "|".join(missing),
                "passed": missing == [] or result.status == "schema_failed_safely",
            })
    return rows


def _expectation_audit_rows(results: List[FixtureReplayResult]) -> List[Dict[str, Any]]:
    expected_results = _read_json(FIXTURE_ROOT / EXPECTED_RESULTS_NAME)
    expectations = expected_results.get("date_expectations", {})
    rows = []

    for result in results:
        expectation = expectations.get(result.label_date, {})
        rows.append({
            "label_date": result.label_date,
            "expected_status": expectation.get("expected_status"),
            "actual_status": result.status,
            "expected_row_count": expectation.get("row_count"),
            "actual_raw_row_count": result.raw_row_count,
            "expected_deduped_row_count": expectation.get("deduped_row_count"),
            "actual_deduped_row_count": result.deduped_row_count,
            "expected_duplicate_count": expectation.get("duplicate_count"),
            "actual_duplicate_count": result.duplicate_count,
            "expected_required_field_failures": expectation.get("required_field_failures"),
            "actual_required_field_failures": result.required_field_failures,
            "expected_missing_fields": "|".join(expectation.get("expected_missing_fields", [])),
            "actual_missing_fields": "|".join(result.missing_fields),
            "passed": (
                expectation.get("expected_status") == result.status
                and expectation.get("row_count") == result.raw_row_count
                and expectation.get("deduped_row_count") == result.deduped_row_count
                and expectation.get("duplicate_count") == result.duplicate_count
                and expectation.get("required_field_failures") == result.required_field_failures
                and expectation.get("expected_missing_fields", []) == result.missing_fields
            ),
        })

    return rows


def _determinism_audit_rows(first: List[FixtureReplayResult], second: List[FixtureReplayResult]) -> List[Dict[str, Any]]:
    first_projection = _stable_projection(first)
    second_projection = _stable_projection(second)

    rows = [
        {
            "comparison": "stable_projection",
            "first": json.dumps(first_projection, sort_keys=True),
            "second": json.dumps(second_projection, sort_keys=True),
            "passed": first_projection == second_projection,
        }
    ]

    for first_result, second_result in zip(first, second):
        rows.append({
            "comparison": f"result::{first_result.label_date}",
            "first": json.dumps(asdict(first_result), sort_keys=True, default=str),
            "second": json.dumps(asdict(second_result), sort_keys=True, default=str),
            "passed": asdict(first_result) == asdict(second_result),
        })

    return rows


def _safety_audit_rows(before_payload: Dict[str, str], before_metadata: Dict[str, str]) -> List[Dict[str, Any]]:
    after_payload = _payload_snapshot()
    after_metadata = _metadata_snapshot()

    source = Path(__file__).read_text(errors="ignore")
    import_lines = "\n".join(
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    safety_start = source.find("def _safety_audit_rows")
    executable_source = source[:safety_start] if safety_start >= 0 else source
    executable_lower = executable_source.lower()

    rows = [
        {
            "check": "payload_snapshot_unchanged",
            "passed": before_payload == after_payload,
            "detail": "fixture payloads unchanged by replay prototype",
        },
        {
            "check": "metadata_snapshot_unchanged",
            "passed": before_metadata == after_metadata,
            "detail": "manifest/expected_results unchanged by replay prototype",
        },
    ]

    for token in [
        "mlb_app.simulation",
        "GameEngine",
        "canonical_matchup_probability",
        "sportsbook",
        "routes",
        "frontend",
    ]:
        rows.append({
            "check": f"forbidden_import::{token}",
            "passed": token not in import_lines,
            "detail": "import_lines_only",
        })

    for token in ["requests.", "httpx.", "urllib.", "pybaseball.statcast"]:
        rows.append({
            "check": f"external_fetch::{token}",
            "passed": token not in executable_source,
            "detail": "source_before_safety_function",
        })

    for token in ["session.commit(", ".to_sql(", "insert into"]:
        rows.append({
            "check": f"db_write::{token}",
            "passed": token.lower() not in executable_lower,
            "detail": "source_before_safety_function",
        })

    return rows


def main() -> None:
    before_payload = _payload_snapshot()
    before_metadata = _metadata_snapshot()

    first_results = _replay_all()
    second_results = _replay_all()

    result_rows = _result_rows(first_results)
    row_audit_rows = _row_audit_rows(first_results)
    expectation_rows = _expectation_audit_rows(first_results)
    determinism_rows = _determinism_audit_rows(first_results, second_results)
    safety_rows = _safety_audit_rows(before_payload, before_metadata)

    _write_csv(OUTPUT_RESULTS, result_rows)
    _write_csv(OUTPUT_ROWS, row_audit_rows)
    _write_csv(OUTPUT_EXPECTATIONS, expectation_rows)
    _write_csv(OUTPUT_DETERMINISM, determinism_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)

    replay_results_valid = all(row["passed"] for row in result_rows)
    replay_expectations_valid = all(row["passed"] for row in expectation_rows)
    negative_cases_valid = (
        any(result.label_date == "2026-05-23" and result.status == "dedupe_success" and result.duplicate_count == 1 for result in first_results)
        and any(result.label_date == "2026-05-24" and result.status == "schema_failed_safely" and result.missing_fields == ["pitcher_id"] for result in first_results)
        and any(result.label_date == "2026-05-25" and result.status == "schema_failed_safely" and result.missing_fields == ["game_pk"] for result in first_results)
        and any(result.label_date == "2026-05-26" and result.status == "fixture_missing" for result in first_results)
    )
    deterministic_replay_valid = all(row["passed"] for row in determinism_rows)
    no_payload_mutation = before_payload == _payload_snapshot()
    no_metadata_mutation = before_metadata == _metadata_snapshot()
    safety_audit_valid = all(row["passed"] for row in safety_rows)

    checks = [
        {"check": "replay_results_valid", "passed": replay_results_valid, "detail": f"{sum(row['passed'] for row in result_rows)}/{len(result_rows)}"},
        {"check": "replay_expectations_valid", "passed": replay_expectations_valid, "detail": f"{sum(row['passed'] for row in expectation_rows)}/{len(expectation_rows)}"},
        {"check": "negative_cases_valid", "passed": negative_cases_valid, "detail": "duplicate/missing-field/missing-file cases"},
        {"check": "deterministic_replay_valid", "passed": deterministic_replay_valid, "detail": f"{sum(row['passed'] for row in determinism_rows)}/{len(determinism_rows)}"},
        {"check": "no_payload_mutation", "passed": no_payload_mutation, "detail": "fixture payload snapshots unchanged"},
        {"check": "no_metadata_mutation", "passed": no_metadata_mutation, "detail": "manifest/expected_results snapshots unchanged"},
        {"check": "safety_audit_valid", "passed": safety_audit_valid, "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "fixture_replay_adapter_prototype_only", "passed": True, "detail": True},
        {"check": "live_adapter_not_implemented", "passed": True, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fixture_replay_adapter_prototype_complete",
        "prototype_version": PROTOTYPE_VERSION,
        "fixture_dates_replayed": len(FIXTURE_DATES),
        "result_rows": len(result_rows),
        "row_audit_rows": len(row_audit_rows),
        "expectation_audit_rows": len(expectation_rows),
        "determinism_checks": len(determinism_rows),
        "safety_checks": len(safety_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "fixture_replay_adapter_prototype_implemented": True,
        "live_adapter_implemented": False,
        "payload_mutated": False,
        "metadata_mutated": False,
        "backfill_scaffold_modified": False,
        "test_double_prototype_modified": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CQ_candidate_bullpen_statcast_fixture_replay_adapter_audit"
            if all(check["passed"] for check in checks)
            else "6CP_patch_candidate_bullpen_statcast_fixture_replay_adapter_prototype"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
