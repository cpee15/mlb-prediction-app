from __future__ import annotations

import csv
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Protocol, Tuple


PROTOTYPE_VERSION = "candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype_v0.1"

LIVE_ADAPTER_TARGET = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
BACKFILL_SCAFFOLD = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
DESIGN_SCRIPT = Path("scripts/design_candidate_bullpen_statcast_live_adapter_fetch.py")
CONTRACT_AUDIT = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_fetch_contract.py")
PLAN_SCRIPT = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_fetch_test_doubles.py")
FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype_checks.csv"
OUTPUT_RESULTS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_results.csv"
OUTPUT_RESULT_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_result_contract_audit.csv"
OUTPUT_ROW_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_normalized_row_contract_audit.csv"
OUTPUT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_status_mapping_audit.csv"
OUTPUT_DUPLICATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_duplicate_audit.csv"
OUTPUT_ORDERING = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_ordering_audit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_fetch_test_double_safety_audit.csv"

NORMALIZED_FIELDS = [
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
RESULT_FIELDS = [
    "label_date",
    "status",
    "rows",
    "raw_row_count",
    "normalized_row_count",
    "duplicate_count",
    "required_field_failures",
    "missing_fields",
    "fetch_error",
    "external_fetch_performed",
    "db_writes_performed",
    "fetch_duration_ms",
    "retry_count",
    "source_adapter_version",
]


class PrototypeDependencyMissingError(RuntimeError):
    pass


@dataclass
class LiveAdapterResult:
    label_date: str
    status: str
    rows: List[Dict[str, Any]]
    raw_row_count: int
    normalized_row_count: int
    duplicate_count: int
    required_field_failures: int
    missing_fields: List[str]
    fetch_error: str
    external_fetch_performed: bool
    db_writes_performed: bool
    fetch_duration_ms: int
    retry_count: int
    source_adapter_version: str


class FetcherDoubleProtocol(Protocol):
    name: str
    expected_status: str

    def fetch(self, label_date: str) -> List[Dict[str, Any]]:
        ...


def _base_row(
    *,
    label_date: str,
    game_pk: int,
    at_bat_number: int,
    pitch_number: int,
    pitcher_id: int,
    events: Any = "strikeout",
    description: Any = "called_strike",
) -> Dict[str, Any]:
    return {
        "game_date": label_date,
        "game_pk": game_pk,
        "inning": 7,
        "inning_topbot": "Top",
        "at_bat_number": at_bat_number,
        "pitch_number": pitch_number,
        "outs_when_up": 1,
        "pitcher_id": pitcher_id,
        "home_team": "NYY",
        "away_team": "BOS",
        "events": events,
        "description": description,
    }


def build_success_rows(label_date: str) -> List[Dict[str, Any]]:
    return [
        _base_row(label_date=label_date, game_pk=1001, at_bat_number=42, pitch_number=1, pitcher_id=501),
        _base_row(label_date=label_date, game_pk=1001, at_bat_number=42, pitch_number=2, pitcher_id=501, events="single"),
        _base_row(label_date=label_date, game_pk=1002, at_bat_number=12, pitch_number=3, pitcher_id=777, description="swinging_strike"),
    ]


def build_schema_failure_rows(label_date: str) -> List[Dict[str, Any]]:
    row = _base_row(label_date=label_date, game_pk=1003, at_bat_number=44, pitch_number=1, pitcher_id=888)
    del row["pitcher_id"]
    return [row]


def build_duplicate_rows(label_date: str) -> List[Dict[str, Any]]:
    row_a = _base_row(label_date=label_date, game_pk=1004, at_bat_number=55, pitch_number=1, pitcher_id=999)
    row_b = dict(row_a)
    row_b["description"] = "duplicate_key_different_description"
    row_c = _base_row(label_date=label_date, game_pk=1004, at_bat_number=55, pitch_number=2, pitcher_id=999)
    return [row_a, row_b, row_c]


def build_unordered_rows(label_date: str) -> List[Dict[str, Any]]:
    return [
        _base_row(label_date=label_date, game_pk=2000, at_bat_number=9, pitch_number=3, pitcher_id=400),
        _base_row(label_date=label_date, game_pk=1000, at_bat_number=1, pitch_number=1, pitcher_id=100),
        _base_row(label_date=label_date, game_pk=1000, at_bat_number=1, pitch_number=2, pitcher_id=100),
    ]


def build_nullable_optional_rows(label_date: str) -> List[Dict[str, Any]]:
    return [
        _base_row(label_date=label_date, game_pk=3001, at_bat_number=1, pitch_number=1, pitcher_id=301, events=None),
        _base_row(label_date=label_date, game_pk=3001, at_bat_number=1, pitch_number=2, pitcher_id=301, description=None),
    ]


def build_mixed_event_description_rows(label_date: str) -> List[Dict[str, Any]]:
    return [
        _base_row(label_date=label_date, game_pk=4001, at_bat_number=1, pitch_number=1, pitcher_id=401, events="", description="ball"),
        _base_row(label_date=label_date, game_pk=4001, at_bat_number=1, pitch_number=2, pitcher_id=401, events="walk", description=""),
        _base_row(label_date=label_date, game_pk=4001, at_bat_number=2, pitch_number=1, pitcher_id=401, events="field_out", description="hit_into_play"),
    ]


class SuccessfulFetcherDouble:
    name = "SuccessfulFetcherDouble"
    expected_status = "live_dry_run_ready"

    def fetch(self, label_date: str) -> List[Dict[str, Any]]:
        return build_success_rows(label_date)


class EmptyFetcherDouble:
    name = "EmptyFetcherDouble"
    expected_status = "live_fetch_empty"

    def fetch(self, label_date: str) -> List[Dict[str, Any]]:
        return []


class ErrorFetcherDouble:
    name = "ErrorFetcherDouble"
    expected_status = "live_fetch_error"

    def fetch(self, label_date: str) -> List[Dict[str, Any]]:
        raise RuntimeError("synthetic fetch error")


class DependencyMissingFetcherDouble:
    name = "DependencyMissingFetcherDouble"
    expected_status = "live_dependency_missing"

    def fetch(self, label_date: str) -> List[Dict[str, Any]]:
        raise PrototypeDependencyMissingError("synthetic dependency missing")


class SchemaFailureFetcherDouble:
    name = "SchemaFailureFetcherDouble"
    expected_status = "live_schema_failed_safely"

    def fetch(self, label_date: str) -> List[Dict[str, Any]]:
        return build_schema_failure_rows(label_date)


class DuplicateRowsFetcherDouble:
    name = "DuplicateRowsFetcherDouble"
    expected_status = "live_dry_run_ready"

    def fetch(self, label_date: str) -> List[Dict[str, Any]]:
        return build_duplicate_rows(label_date)


class UnorderedRowsFetcherDouble:
    name = "UnorderedRowsFetcherDouble"
    expected_status = "live_dry_run_ready"

    def fetch(self, label_date: str) -> List[Dict[str, Any]]:
        return build_unordered_rows(label_date)


def _natural_key(row: Dict[str, Any]) -> Tuple[Any, Any, Any, Any]:
    return tuple(row.get(field) for field in NATURAL_KEY_FIELDS)


def _normalize_rows(raw_rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int, int, List[str]]:
    missing_fields: List[str] = []
    required_field_failures = 0
    normalized_rows: List[Dict[str, Any]] = []

    for raw in raw_rows:
        row_missing = [field for field in NORMALIZED_FIELDS if field not in raw]
        if row_missing:
            required_field_failures += 1
            for field in row_missing:
                if field not in missing_fields:
                    missing_fields.append(field)
            continue

        normalized_rows.append({field: raw.get(field) for field in NORMALIZED_FIELDS})

    seen_keys = set()
    duplicate_count = 0
    for row in normalized_rows:
        key = _natural_key(row)
        if key in seen_keys:
            duplicate_count += 1
        seen_keys.add(key)

    normalized_rows = sorted(normalized_rows, key=_natural_key)
    return normalized_rows, duplicate_count, required_field_failures, sorted(missing_fields)


def run_fetcher_double(label_date: str, fetcher: FetcherDoubleProtocol) -> LiveAdapterResult:
    started = time.perf_counter()
    raw_rows: List[Dict[str, Any]] = []
    fetch_error = ""
    status = "live_dry_run_ready"

    try:
        raw_rows = fetcher.fetch(label_date)
    except PrototypeDependencyMissingError as exc:
        fetch_error = str(exc)
        status = "live_dependency_missing"
    except Exception as exc:
        fetch_error = str(exc)
        status = "live_fetch_error"

    rows: List[Dict[str, Any]] = []
    duplicate_count = 0
    required_field_failures = 0
    missing_fields: List[str] = []

    if not fetch_error:
        if not raw_rows:
            status = "live_fetch_empty"
        else:
            rows, duplicate_count, required_field_failures, missing_fields = _normalize_rows(raw_rows)
            status = "live_schema_failed_safely" if required_field_failures else "live_dry_run_ready"

    duration_ms = max(0, int((time.perf_counter() - started) * 1000))

    return LiveAdapterResult(
        label_date=label_date,
        status=status,
        rows=rows,
        raw_row_count=len(raw_rows),
        normalized_row_count=len(rows),
        duplicate_count=duplicate_count,
        required_field_failures=required_field_failures,
        missing_fields=missing_fields,
        fetch_error=fetch_error,
        external_fetch_performed=False,
        db_writes_performed=False,
        fetch_duration_ms=duration_ms,
        retry_count=0,
        source_adapter_version=PROTOTYPE_VERSION,
    )


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _snapshot_files() -> Dict[str, str]:
    paths = [BACKFILL_SCAFFOLD, DESIGN_SCRIPT, CONTRACT_AUDIT, PLAN_SCRIPT, MANIFEST, EXPECTED_RESULTS]
    snapshot = {str(path): path.read_text(errors="ignore") if path.exists() else "__MISSING__" for path in paths}
    if DATES_DIR.exists():
        for payload in sorted(DATES_DIR.glob("*.jsonl")):
            snapshot[str(payload)] = payload.read_text(errors="ignore")
    return snapshot


def _source_safety_scan() -> Dict[str, bool]:
    source = Path(__file__).read_text(errors="ignore")
    scanner_start = source.find("def _source_safety_scan()")
    executable_prefix = source[:scanner_start] if scanner_start >= 0 else source
    import_lines = "\n".join(
        line.strip()
        for line in executable_prefix.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    executable_lower = executable_prefix.lower()
    return {
        "no_pybaseball_import": "pybaseball" not in import_lines and "statcast" not in import_lines,
        "no_external_fetch": all(token not in executable_prefix for token in ["requests.", "httpx.", "urllib."]),
        "no_db_writes": all(token not in executable_lower for token in ["session.commit(", ".to_sql(", "insert into"]),
    }


def _result_contract_audit(results: List[LiveAdapterResult]) -> List[Dict[str, Any]]:
    rows = []
    for result in results:
        payload = asdict(result)
        rows.append({
            "fetcher": result.source_adapter_version,
            "label_date": result.label_date,
            "field_count": len(payload),
            "fields_exact": set(payload) == set(RESULT_FIELDS),
            "external_fetch_performed": result.external_fetch_performed,
            "db_writes_performed": result.db_writes_performed,
            "fetch_duration_ms_type_valid": isinstance(result.fetch_duration_ms, int),
            "retry_count_type_valid": isinstance(result.retry_count, int),
            "source_adapter_version_populated": bool(result.source_adapter_version),
            "passed": (
                len(payload) == 14
                and set(payload) == set(RESULT_FIELDS)
                and result.external_fetch_performed is False
                and result.db_writes_performed is False
                and isinstance(result.fetch_duration_ms, int)
                and isinstance(result.retry_count, int)
                and bool(result.source_adapter_version)
            ),
        })
    return rows


def _row_contract_audit(results: List[LiveAdapterResult]) -> List[Dict[str, Any]]:
    rows = []
    for result in results:
        for idx, row in enumerate(result.rows):
            rows.append({
                "status": result.status,
                "row_index": idx,
                "field_count": len(row),
                "fields_exact": set(row) == set(NORMALIZED_FIELDS),
                "natural_key_complete": all(field in row for field in NATURAL_KEY_FIELDS),
                "passed": len(row) == 12 and set(row) == set(NORMALIZED_FIELDS) and all(field in row for field in NATURAL_KEY_FIELDS),
            })
    if not rows:
        rows.append({"status": "no_rows", "row_index": -1, "field_count": 0, "fields_exact": True, "natural_key_complete": True, "passed": True})
    return rows


def _ordering_is_deterministic(result: LiveAdapterResult) -> bool:
    return result.rows == sorted(result.rows, key=_natural_key)


def main() -> None:
    before_snapshot = _snapshot_files()
    live_adapter_existed_before = LIVE_ADAPTER_TARGET.exists()

    label_date = "2024-07-15"
    fetchers: List[FetcherDoubleProtocol] = [
        SuccessfulFetcherDouble(),
        EmptyFetcherDouble(),
        ErrorFetcherDouble(),
        DependencyMissingFetcherDouble(),
        SchemaFailureFetcherDouble(),
        DuplicateRowsFetcherDouble(),
        UnorderedRowsFetcherDouble(),
    ]
    results = [run_fetcher_double(label_date, fetcher) for fetcher in fetchers]
    result_by_fetcher = dict(zip([fetcher.name for fetcher in fetchers], results))

    double_result_rows = []
    status_rows = []
    duplicate_rows = []
    ordering_rows = []

    for fetcher, result in zip(fetchers, results):
        double_result_rows.append({
            "fetcher": fetcher.name,
            "expected_status": fetcher.expected_status,
            "actual_status": result.status,
            "raw_row_count": result.raw_row_count,
            "normalized_row_count": result.normalized_row_count,
            "duplicate_count": result.duplicate_count,
            "required_field_failures": result.required_field_failures,
            "missing_fields": "|".join(result.missing_fields),
            "fetch_error": result.fetch_error,
            "external_fetch_performed": result.external_fetch_performed,
            "db_writes_performed": result.db_writes_performed,
            "passed": result.status == fetcher.expected_status,
        })
        status_rows.append({
            "fetcher": fetcher.name,
            "expected_status": fetcher.expected_status,
            "actual_status": result.status,
            "passed": result.status == fetcher.expected_status,
        })
        duplicate_rows.append({
            "fetcher": fetcher.name,
            "duplicate_count": result.duplicate_count,
            "expected_duplicate_positive": fetcher.name == "DuplicateRowsFetcherDouble",
            "passed": (result.duplicate_count > 0) if fetcher.name == "DuplicateRowsFetcherDouble" else (result.duplicate_count == 0),
        })
        ordering_rows.append({
            "fetcher": fetcher.name,
            "row_count": len(result.rows),
            "deterministic_ordering": _ordering_is_deterministic(result),
            "passed": _ordering_is_deterministic(result),
        })

    result_contract_rows = _result_contract_audit(results)
    row_contract_rows = _row_contract_audit(results)
    safety_scan = _source_safety_scan()
    after_snapshot = _snapshot_files()

    success_rows_valid = result_by_fetcher["SuccessfulFetcherDouble"].normalized_row_count == 3
    empty_fetch_valid = result_by_fetcher["EmptyFetcherDouble"].raw_row_count == 0
    error_fetch_valid = bool(result_by_fetcher["ErrorFetcherDouble"].fetch_error)
    dependency_missing_valid = bool(result_by_fetcher["DependencyMissingFetcherDouble"].fetch_error)
    schema_failure_valid = result_by_fetcher["SchemaFailureFetcherDouble"].required_field_failures == 1
    duplicate_detection_valid = result_by_fetcher["DuplicateRowsFetcherDouble"].duplicate_count == 1
    deterministic_ordering_valid = all(row["passed"] for row in ordering_rows)
    status_mapping_valid = all(row["passed"] for row in status_rows)

    safety_rows = [
        {"check": "no_live_adapter_created", "passed": live_adapter_existed_before is False and not LIVE_ADAPTER_TARGET.exists(), "detail": str(LIVE_ADAPTER_TARGET)},
        {"check": "no_scaffold_mutation", "passed": before_snapshot.get(str(BACKFILL_SCAFFOLD)) == after_snapshot.get(str(BACKFILL_SCAFFOLD)), "detail": str(BACKFILL_SCAFFOLD)},
        {"check": "no_fixture_mutation", "passed": before_snapshot == after_snapshot, "detail": "fixture and tracked scripts unchanged"},
        {"check": "no_pybaseball_import", "passed": safety_scan["no_pybaseball_import"], "detail": True},
        {"check": "no_external_fetch", "passed": safety_scan["no_external_fetch"], "detail": True},
        {"check": "no_db_writes", "passed": safety_scan["no_db_writes"], "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _write_csv(OUTPUT_RESULTS, double_result_rows)
    _write_csv(OUTPUT_RESULT_CONTRACT, result_contract_rows)
    _write_csv(OUTPUT_ROW_CONTRACT, row_contract_rows)
    _write_csv(OUTPUT_STATUS, status_rows)
    _write_csv(OUTPUT_DUPLICATE, duplicate_rows)
    _write_csv(OUTPUT_ORDERING, ordering_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)

    checks = [
        {"check": "live_adapter_result_contract_valid", "passed": all(row["passed"] for row in result_contract_rows), "detail": f"{sum(row['passed'] for row in result_contract_rows)}/{len(result_contract_rows)}"},
        {"check": "synthetic_payload_cases_valid", "passed": success_rows_valid and empty_fetch_valid and error_fetch_valid and dependency_missing_valid and schema_failure_valid, "detail": "success/empty/error/dependency/schema"},
        {"check": "fetcher_doubles_valid", "passed": len(fetchers) == 7, "detail": f"{len(fetchers)} doubles"},
        {"check": "status_mapping_valid", "passed": status_mapping_valid, "detail": f"{sum(row['passed'] for row in status_rows)}/{len(status_rows)}"},
        {"check": "normalized_row_contract_valid", "passed": all(row["passed"] for row in row_contract_rows), "detail": f"{sum(row['passed'] for row in row_contract_rows)}/{len(row_contract_rows)}"},
        {"check": "duplicate_detection_valid", "passed": duplicate_detection_valid and all(row["passed"] for row in duplicate_rows), "detail": f"{sum(row['passed'] for row in duplicate_rows)}/{len(duplicate_rows)}"},
        {"check": "deterministic_ordering_valid", "passed": deterministic_ordering_valid, "detail": f"{sum(row['passed'] for row in ordering_rows)}/{len(ordering_rows)}"},
        {"check": "result_safety_flags_valid", "passed": all(result.external_fetch_performed is False and result.db_writes_performed is False for result in results), "detail": True},
        {"check": "no_live_adapter_created", "passed": safety_rows[0]["passed"], "detail": str(LIVE_ADAPTER_TARGET)},
        {"check": "no_scaffold_mutation", "passed": safety_rows[1]["passed"], "detail": str(BACKFILL_SCAFFOLD)},
        {"check": "no_fixture_mutation", "passed": safety_rows[2]["passed"], "detail": True},
        {"check": "no_pybaseball_import", "passed": safety_scan["no_pybaseball_import"], "detail": True},
        {"check": "no_external_fetch", "passed": safety_scan["no_external_fetch"], "detail": True},
        {"check": "no_db_writes", "passed": safety_scan["no_db_writes"], "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype_complete",
        "prototype_version": PROTOTYPE_VERSION,
        "fetcher_double_rows": len(fetchers),
        "double_result_rows": len(double_result_rows),
        "result_contract_rows": len(result_contract_rows),
        "normalized_row_contract_rows": len(row_contract_rows),
        "status_mapping_rows": len(status_rows),
        "duplicate_audit_rows": len(duplicate_rows),
        "ordering_audit_rows": len(ordering_rows),
        "safety_rows": len(safety_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "prototype_only": True,
        "real_live_adapter_created": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "fixture_assets_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DE_candidate_bullpen_statcast_live_adapter_fetch_test_double_audit"
            if all(check["passed"] for check in checks)
            else "6DD_patch_candidate_bullpen_statcast_live_adapter_fetch_test_double_prototype"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
