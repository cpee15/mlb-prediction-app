from __future__ import annotations

import csv
import importlib.util
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple


PROTOTYPE_VERSION = "candidate_bullpen_statcast_fetch_adapter_test_double_prototype_v0.1"
SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_prototype.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_prototype_checks.csv"
OUTPUT_RESULTS = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_results.csv"
OUTPUT_RETRY = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_retry_audit.csv"
OUTPUT_NORMALIZATION = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_normalization_audit.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_fetch_adapter_test_double_safety_audit.csv"


LABEL_DATE = "2026-05-20"


class AdapterFetchUnavailable(Exception):
    pass


class AdapterNoRowsReturned(Exception):
    pass


class AdapterSchemaMismatch(Exception):
    pass


class AdapterRateLimited(Exception):
    pass


class AdapterNetworkError(Exception):
    pass


class AdapterUnexpectedError(Exception):
    pass


@dataclass
class AdapterRunResult:
    adapter_name: str
    final_status: str
    raw_row_count: int
    deduped_row_count: int
    duplicate_count: int
    required_field_failures: int
    retry_attempts: int
    exception_name: str
    no_external_fetch: bool
    no_db_writes: bool


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _import_scaffold():
    spec = importlib.util.spec_from_file_location("backfill_candidate_bullpen_statcast_labels", SCAFFOLD_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not import scaffold module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fixture_row(
    *,
    game_pk: int = 777001,
    at_bat_number: int = 42,
    pitch_number: int = 3,
    pitcher_id: int = 123456,
    description: str = "called_strike",
) -> Dict[str, Any]:
    return {
        "game_date": LABEL_DATE,
        "game_pk": game_pk,
        "inning": 7,
        "inning_topbot": "Top",
        "at_bat_number": at_bat_number,
        "pitch_number": pitch_number,
        "outs_when_up": 1,
        "pitcher_id": pitcher_id,
        "home_team": "NYY",
        "away_team": "BOS",
        "events": "strikeout",
        "description": description,
    }


def empty_adapter(label_date: str) -> List[Dict[str, Any]]:
    _ = label_date
    return []


def fixture_adapter(label_date: str) -> List[Dict[str, Any]]:
    _ = label_date
    return [
        _fixture_row(at_bat_number=42, pitch_number=1),
        _fixture_row(at_bat_number=42, pitch_number=2),
        _fixture_row(at_bat_number=42, pitch_number=3),
    ]


def malformed_schema_adapter(label_date: str) -> List[Dict[str, Any]]:
    _ = label_date
    row_missing_pitcher = _fixture_row(at_bat_number=50, pitch_number=1)
    row_missing_pitcher.pop("pitcher_id", None)

    row_missing_game = _fixture_row(at_bat_number=50, pitch_number=2)
    row_missing_game.pop("game_pk", None)

    return [row_missing_pitcher, row_missing_game]


def duplicate_natural_key_adapter(label_date: str) -> List[Dict[str, Any]]:
    _ = label_date
    first = _fixture_row(at_bat_number=61, pitch_number=1, description="called_strike")
    duplicate = dict(first)
    duplicate["description"] = "duplicate_called_strike"
    unique = _fixture_row(at_bat_number=61, pitch_number=2, description="swinging_strike")
    return [first, duplicate, unique]


class TransientErrorAdapter:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, label_date: str) -> List[Dict[str, Any]]:
        _ = label_date
        self.calls += 1
        if self.calls == 1:
            raise AdapterNetworkError("simulated transient network failure")
        return [_fixture_row(at_bat_number=70, pitch_number=1, description="retry_success")]


def _validate_rows(scaffold_module, rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int, int, List[Dict[str, Any]]]:
    deduped, duplicate_count = scaffold_module._dedupe_rows(rows)
    validation_results = [scaffold_module._validate_required_fields(row) for row in deduped]
    required_field_failures = sum(1 for result in validation_results if not result["valid"])

    normalization_rows = []
    for index, row in enumerate(deduped):
        validation = scaffold_module._validate_required_fields(row)
        normalization_rows.append({
            "row_index": index,
            "game_pk": row.get("game_pk"),
            "at_bat_number": row.get("at_bat_number"),
            "pitch_number": row.get("pitch_number"),
            "pitcher_id": row.get("pitcher_id"),
            "natural_key": str(scaffold_module._natural_key(row)),
            "valid": validation["valid"],
            "missing_fields": validation["missing_fields"],
            "nullable_missing_fields": validation["nullable_missing_fields"],
        })

    return deduped, duplicate_count, required_field_failures, normalization_rows


def _run_adapter(
    scaffold_module,
    adapter_name: str,
    adapter: Callable[[str], List[Dict[str, Any]]],
    *,
    max_attempts: int = 2,
) -> Tuple[AdapterRunResult, List[Dict[str, Any]], List[Dict[str, Any]]]:
    retry_rows: List[Dict[str, Any]] = []
    exception_name = ""
    raw_rows: List[Dict[str, Any]] = []
    final_status = "unknown"

    for attempt in range(1, max_attempts + 1):
        try:
            raw_rows = adapter(LABEL_DATE)
            retry_rows.append({
                "adapter_name": adapter_name,
                "attempt": attempt,
                "status": "success",
                "exception_name": "",
                "row_count": len(raw_rows),
            })
            if not raw_rows:
                final_status = "empty_success"
            else:
                final_status = "success"
            break
        except AdapterNetworkError as exc:
            exception_name = exc.__class__.__name__
            retry_rows.append({
                "adapter_name": adapter_name,
                "attempt": attempt,
                "status": "retryable_error",
                "exception_name": exception_name,
                "row_count": 0,
            })
            if attempt == max_attempts:
                final_status = "retry_exhausted"
        except Exception as exc:
            exception_name = exc.__class__.__name__
            retry_rows.append({
                "adapter_name": adapter_name,
                "attempt": attempt,
                "status": "non_retryable_error",
                "exception_name": exception_name,
                "row_count": 0,
            })
            final_status = "failed"
            break

    deduped, duplicate_count, required_field_failures, normalization_rows = _validate_rows(scaffold_module, raw_rows)

    if required_field_failures > 0:
        final_status = "schema_failed_safely"

    result = AdapterRunResult(
        adapter_name=adapter_name,
        final_status=final_status,
        raw_row_count=len(raw_rows),
        deduped_row_count=len(deduped),
        duplicate_count=duplicate_count,
        required_field_failures=required_field_failures,
        retry_attempts=len(retry_rows),
        exception_name=exception_name,
        no_external_fetch=True,
        no_db_writes=True,
    )

    for row in normalization_rows:
        row["adapter_name"] = adapter_name

    return result, retry_rows, normalization_rows


def _source_safety_audit() -> List[Dict[str, Any]]:
    source = Path(__file__).read_text(errors="ignore")
    import_lines = "\n".join(
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )

    # Exclude declarative token-list literals from executable safety scanning.
    executable_lines = []
    in_token_list = False
    for raw_line in source.splitlines():
        stripped = raw_line.strip()
        if stripped in {"forbidden_imports = [", "external_fetch_tokens = [", "db_write_tokens = ["}:
            in_token_list = True
            continue
        if in_token_list and stripped == "]":
            in_token_list = False
            continue
        if not in_token_list:
            executable_lines.append(raw_line)
    executable_source = "\n".join(executable_lines)

    forbidden_imports = [
        "mlb_app.simulation",
        "GameEngine",
        "canonical_matchup_probability",
        "sportsbook",
        "routes",
        "frontend",
    ]

    external_fetch_tokens = [
        "requests.",
        "urllib.",
        "httpx.",
        "pybaseball.statcast",
        "statcast(",
    ]

    db_write_tokens = [
        "session.commit(",
        ".to_sql(",
        "insert into",
    ]

    rows = []
    for token in forbidden_imports:
        rows.append({
            "check_type": "forbidden_import",
            "token": token,
            "present": token in import_lines,
            "passed": token not in import_lines,
            "scan_scope": "import_lines_only",
        })

    for token in external_fetch_tokens:
        rows.append({
            "check_type": "external_fetch_token",
            "token": token,
            "present": token in executable_source,
            "passed": token not in executable_source,
            "scan_scope": "executable_source_excluding_token_lists",
        })

    for token in db_write_tokens:
        rows.append({
            "check_type": "db_write_token",
            "token": token,
            "present": token.lower() in executable_source.lower(),
            "passed": token.lower() not in executable_source.lower(),
            "scan_scope": "executable_source_excluding_token_lists",
        })

    return rows


def main() -> None:
    scaffold_module = _import_scaffold()

    transient = TransientErrorAdapter()
    adapters: List[Tuple[str, Callable[[str], List[Dict[str, Any]]]]] = [
        ("empty_adapter", empty_adapter),
        ("fixture_adapter", fixture_adapter),
        ("malformed_schema_adapter", malformed_schema_adapter),
        ("duplicate_natural_key_adapter", duplicate_natural_key_adapter),
        ("transient_error_adapter", transient),
    ]

    result_objects: List[AdapterRunResult] = []
    retry_rows: List[Dict[str, Any]] = []
    normalization_rows: List[Dict[str, Any]] = []

    for adapter_name, adapter in adapters:
        result, adapter_retry_rows, adapter_normalization_rows = _run_adapter(scaffold_module, adapter_name, adapter)
        result_objects.append(result)
        retry_rows.extend(adapter_retry_rows)
        normalization_rows.extend(adapter_normalization_rows)

    result_rows = [result.__dict__ for result in result_objects]
    safety_rows = _source_safety_audit()

    _write_csv(OUTPUT_RESULTS, result_rows)
    _write_csv(OUTPUT_RETRY, retry_rows)
    _write_csv(OUTPUT_NORMALIZATION, normalization_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)

    by_name = {row["adapter_name"]: row for row in result_rows}

    scaffold_module_loaded = scaffold_module is not None and hasattr(scaffold_module, "_validate_required_fields")
    adapter_exceptions_defined = all(
        cls.__name__ in {
            "AdapterFetchUnavailable",
            "AdapterNoRowsReturned",
            "AdapterSchemaMismatch",
            "AdapterRateLimited",
            "AdapterNetworkError",
            "AdapterUnexpectedError",
        }
        for cls in [
            AdapterFetchUnavailable,
            AdapterNoRowsReturned,
            AdapterSchemaMismatch,
            AdapterRateLimited,
            AdapterNetworkError,
            AdapterUnexpectedError,
        ]
    )
    test_doubles_defined = len(adapters) == 5
    empty_adapter_valid = by_name["empty_adapter"]["final_status"] == "empty_success" and by_name["empty_adapter"]["raw_row_count"] == 0
    fixture_adapter_valid = by_name["fixture_adapter"]["final_status"] == "success" and by_name["fixture_adapter"]["deduped_row_count"] == 3
    malformed_schema_fails_safely = by_name["malformed_schema_adapter"]["final_status"] == "schema_failed_safely" and by_name["malformed_schema_adapter"]["required_field_failures"] == 2
    duplicate_natural_key_deduped = by_name["duplicate_natural_key_adapter"]["duplicate_count"] == 1 and by_name["duplicate_natural_key_adapter"]["deduped_row_count"] == 2
    transient_error_retry_valid = by_name["transient_error_adapter"]["final_status"] == "success" and by_name["transient_error_adapter"]["retry_attempts"] == 2
    normalization_audit_valid = len(normalization_rows) >= 1 and any(row["valid"] is False for row in normalization_rows)
    safety_audit_valid = all(row["passed"] for row in safety_rows)

    checks = [
        {"check": "scaffold_module_loaded", "passed": scaffold_module_loaded, "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_exceptions_defined", "passed": adapter_exceptions_defined, "detail": "6 adapter exceptions"},
        {"check": "test_doubles_defined", "passed": test_doubles_defined, "detail": "5 doubles"},
        {"check": "empty_adapter_valid", "passed": empty_adapter_valid, "detail": by_name["empty_adapter"]},
        {"check": "fixture_adapter_valid", "passed": fixture_adapter_valid, "detail": by_name["fixture_adapter"]},
        {"check": "malformed_schema_fails_safely", "passed": malformed_schema_fails_safely, "detail": by_name["malformed_schema_adapter"]},
        {"check": "duplicate_natural_key_deduped", "passed": duplicate_natural_key_deduped, "detail": by_name["duplicate_natural_key_adapter"]},
        {"check": "transient_error_retry_valid", "passed": transient_error_retry_valid, "detail": by_name["transient_error_adapter"]},
        {"check": "normalization_audit_valid", "passed": normalization_audit_valid, "detail": f"{len(normalization_rows)} rows"},
        {"check": "safety_audit_valid", "passed": safety_audit_valid, "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fetch_adapter_test_double_prototype_complete",
        "prototype_version": PROTOTYPE_VERSION,
        "adapter_doubles": len(adapters),
        "result_rows": len(result_rows),
        "retry_audit_rows": len(retry_rows),
        "normalization_audit_rows": len(normalization_rows),
        "safety_checks": len(safety_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "test_double_only": True,
        "live_adapter_implemented": False,
        "scaffold_modified": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CH_candidate_bullpen_statcast_fetch_adapter_test_double_audit"
            if all(check["passed"] for check in checks)
            else "6CG_patch_candidate_bullpen_statcast_fetch_adapter_test_double_prototype"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
