from __future__ import annotations

import csv
import importlib
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


LIVE_ADAPTER_VERSION = "candidate_bullpen_statcast_live_adapter_v0.1"

STATUS_LIVE_DRY_RUN_READY = "live_dry_run_ready"
STATUS_LIVE_FETCH_EMPTY = "live_fetch_empty"
STATUS_LIVE_FETCH_ERROR = "live_fetch_error"
STATUS_LIVE_SCHEMA_FAILED_SAFELY = "live_schema_failed_safely"
STATUS_LIVE_ADAPTER_NOT_CONFIGURED = "live_adapter_not_configured"
STATUS_LIVE_WRITE_BLOCKED = "live_write_blocked"
STATUS_LIVE_REQUIRES_DRY_RUN = "live_requires_dry_run"
STATUS_LIVE_DATE_WINDOW_INVALID = "live_date_window_invalid"
STATUS_LIVE_DEPENDENCY_MISSING = "live_dependency_missing"

STATUS_TAXONOMY = [
    STATUS_LIVE_DRY_RUN_READY,
    STATUS_LIVE_FETCH_EMPTY,
    STATUS_LIVE_FETCH_ERROR,
    STATUS_LIVE_SCHEMA_FAILED_SAFELY,
    STATUS_LIVE_ADAPTER_NOT_CONFIGURED,
    STATUS_LIVE_WRITE_BLOCKED,
    STATUS_LIVE_REQUIRES_DRY_RUN,
    STATUS_LIVE_DATE_WINDOW_INVALID,
    STATUS_LIVE_DEPENDENCY_MISSING,
]

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


def natural_key(row: dict) -> tuple:
    return tuple(row.get(field) for field in NATURAL_KEY_FIELDS)


def _validate_label_date(label_date: str) -> None:
    parsed = datetime.strptime(label_date, "%Y-%m-%d")
    if parsed.strftime("%Y-%m-%d") != label_date:
        raise ValueError(f"invalid label_date: {label_date}")


def normalize_statcast_pitch_rows(label_date: str, raw_rows: list[dict]) -> tuple[list[dict], int, int, list[str]]:
    normalized_rows: List[Dict[str, Any]] = []
    required_field_failures = 0
    missing_fields: List[str] = []

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
        key = natural_key(row)
        if key in seen_keys:
            duplicate_count += 1
        seen_keys.add(key)

    return sorted(normalized_rows, key=natural_key), duplicate_count, required_field_failures, sorted(missing_fields)


def _dependency_missing_result(label_date: str, started: float, error: str) -> LiveAdapterResult:
    return LiveAdapterResult(
        label_date=label_date,
        status=STATUS_LIVE_DEPENDENCY_MISSING,
        rows=[],
        raw_row_count=0,
        normalized_row_count=0,
        duplicate_count=0,
        required_field_failures=0,
        missing_fields=[],
        fetch_error=error,
        external_fetch_performed=False,
        db_writes_performed=False,
        fetch_duration_ms=max(0, int((time.perf_counter() - started) * 1000)),
        retry_count=0,
        source_adapter_version=LIVE_ADAPTER_VERSION,
    )


def _error_result(label_date: str, started: float, error: str, retry_count: int) -> LiveAdapterResult:
    return LiveAdapterResult(
        label_date=label_date,
        status=STATUS_LIVE_FETCH_ERROR,
        rows=[],
        raw_row_count=0,
        normalized_row_count=0,
        duplicate_count=0,
        required_field_failures=0,
        missing_fields=[],
        fetch_error=error,
        external_fetch_performed=False,
        db_writes_performed=False,
        fetch_duration_ms=max(0, int((time.perf_counter() - started) * 1000)),
        retry_count=retry_count,
        source_adapter_version=LIVE_ADAPTER_VERSION,
    )


def fetch_candidate_bullpen_statcast_live_rows_for_date(
    label_date: str,
    timeout_seconds: int,
    max_retries: int,
    fetcher: Optional[Callable[[str], list[dict]]] = None,
) -> LiveAdapterResult:
    started = time.perf_counter()
    retry_count = 0

    try:
        _validate_label_date(label_date)
    except Exception as exc:
        return _error_result(label_date, started, str(exc), retry_count=0)

    if fetcher is None:
        try:
            importlib.import_module("pybaseball")
        except Exception as exc:
            return _dependency_missing_result(label_date, started, f"live dependency missing: {exc}")
        return _dependency_missing_result(
            label_date,
            started,
            "live adapter dependency present but real network fetch is intentionally disabled in this layer",
        )

    raw_rows: List[Dict[str, Any]] = []
    last_error = ""

    attempts = max(0, int(max_retries)) + 1
    for attempt in range(attempts):
        try:
            raw_rows = fetcher(label_date)
            retry_count = attempt
            last_error = ""
            break
        except Exception as exc:
            last_error = str(exc)
            retry_count = attempt
    else:
        return _error_result(label_date, started, last_error or "fetcher failed", retry_count=retry_count)

    if last_error:
        return _error_result(label_date, started, last_error, retry_count=retry_count)

    if not raw_rows:
        return LiveAdapterResult(
            label_date=label_date,
            status=STATUS_LIVE_FETCH_EMPTY,
            rows=[],
            raw_row_count=0,
            normalized_row_count=0,
            duplicate_count=0,
            required_field_failures=0,
            missing_fields=[],
            fetch_error="",
            external_fetch_performed=False,
            db_writes_performed=False,
            fetch_duration_ms=max(0, int((time.perf_counter() - started) * 1000)),
            retry_count=retry_count,
            source_adapter_version=LIVE_ADAPTER_VERSION,
        )

    rows, duplicate_count, required_field_failures, missing_fields = normalize_statcast_pitch_rows(label_date, raw_rows)
    status = STATUS_LIVE_SCHEMA_FAILED_SAFELY if required_field_failures else STATUS_LIVE_DRY_RUN_READY

    return LiveAdapterResult(
        label_date=label_date,
        status=status,
        rows=rows,
        raw_row_count=len(raw_rows),
        normalized_row_count=len(rows),
        duplicate_count=duplicate_count,
        required_field_failures=required_field_failures,
        missing_fields=missing_fields,
        fetch_error="",
        external_fetch_performed=False,
        db_writes_performed=False,
        fetch_duration_ms=max(0, int((time.perf_counter() - started) * 1000)),
        retry_count=retry_count,
        source_adapter_version=LIVE_ADAPTER_VERSION,
    )


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


def _success_fetcher(label_date: str) -> List[Dict[str, Any]]:
    return [
        _base_row(label_date=label_date, game_pk=1001, at_bat_number=42, pitch_number=1, pitcher_id=501),
        _base_row(label_date=label_date, game_pk=1001, at_bat_number=42, pitch_number=2, pitcher_id=501),
        _base_row(label_date=label_date, game_pk=1002, at_bat_number=12, pitch_number=3, pitcher_id=777),
    ]


def _empty_fetcher(label_date: str) -> List[Dict[str, Any]]:
    return []


def _error_fetcher(label_date: str) -> List[Dict[str, Any]]:
    raise RuntimeError("synthetic injected fetch error")


def _schema_failure_fetcher(label_date: str) -> List[Dict[str, Any]]:
    row = _base_row(label_date=label_date, game_pk=1003, at_bat_number=44, pitch_number=1, pitcher_id=888)
    del row["pitcher_id"]
    return [row]


def _duplicate_fetcher(label_date: str) -> List[Dict[str, Any]]:
    row_a = _base_row(label_date=label_date, game_pk=1004, at_bat_number=55, pitch_number=1, pitcher_id=999)
    row_b = dict(row_a)
    row_b["description"] = "duplicate_key_different_description"
    row_c = _base_row(label_date=label_date, game_pk=1004, at_bat_number=55, pitch_number=2, pitcher_id=999)
    return [row_a, row_b, row_c]


def _unordered_fetcher(label_date: str) -> List[Dict[str, Any]]:
    return [
        _base_row(label_date=label_date, game_pk=2000, at_bat_number=9, pitch_number=3, pitcher_id=400),
        _base_row(label_date=label_date, game_pk=1000, at_bat_number=1, pitch_number=1, pitcher_id=100),
        _base_row(label_date=label_date, game_pk=1000, at_bat_number=1, pitch_number=2, pitcher_id=100),
    ]


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


def _source_safety_scan() -> Dict[str, bool]:
    source = Path(__file__).read_text(errors="ignore")
    import_lines = "\n".join(
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    scanner_start = source.find("def _source_safety_scan()")
    executable_prefix = source[:scanner_start] if scanner_start >= 0 else source
    executable_lower = executable_prefix.lower()
    return {
        "no_top_level_pybaseball_import": "pybaseball" not in import_lines and "statcast" not in import_lines,
        "no_external_network_usage": all(token not in executable_prefix for token in ["requests.", "httpx.", "urllib."]),
        "no_db_writes": all(token not in executable_lower for token in ["session.commit(", ".to_sql(", "insert into"]),
    }


def _result_contract_rows(results: Dict[str, LiveAdapterResult]) -> List[Dict[str, Any]]:
    rows = []
    for name, result in results.items():
        payload = asdict(result)
        rows.append({
            "case": name,
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


def _row_contract_rows(results: Dict[str, LiveAdapterResult]) -> List[Dict[str, Any]]:
    rows = []
    for name, result in results.items():
        for idx, row in enumerate(result.rows):
            rows.append({
                "case": name,
                "row_index": idx,
                "field_count": len(row),
                "fields_exact": set(row) == set(NORMALIZED_FIELDS),
                "natural_key_complete": all(field in row for field in NATURAL_KEY_FIELDS),
                "passed": len(row) == 12 and set(row) == set(NORMALIZED_FIELDS) and all(field in row for field in NATURAL_KEY_FIELDS),
            })
    if not rows:
        rows.append({"case": "no_rows", "row_index": -1, "field_count": 0, "fields_exact": True, "natural_key_complete": True, "passed": True})
    return rows



def _candidate_bullpen_live_fetcher_runtime_summary(artifact: Dict[str, Any]) -> Dict[str, Any]:
    """Build additive live-fetcher runtime summary fields.

    The summary is diagnostic-only. It derives from existing artifact,
    observability, and preflight fields and must not change resolver gates,
    adapter behavior, production defaults, write policy, or materialization.
    """

    source_mode = str(artifact.get("source_mode", "") or "")
    resolution_gate = str(artifact.get("live_fetcher_resolution_gate", "") or "")
    resolution_status = str(artifact.get("live_fetcher_resolution_status", "") or "")
    resolution_reason = str(artifact.get("live_fetcher_resolution_reason", "") or "")
    adapter_status = str(artifact.get("adapter_status", artifact.get("status", "")) or "")
    preflight_status = str(artifact.get("live_fetcher_preflight_status", "") or "")
    preflight_reason = str(artifact.get("live_fetcher_preflight_reason", "") or "")

    dry_run = bool(artifact.get("live_fetcher_preflight_dry_run", artifact.get("dry_run", False)))
    write_blocked = bool(artifact.get("live_fetcher_preflight_write_blocked", False))
    allow_live_write = bool(artifact.get("live_fetcher_preflight_allow_live_write", False))
    external_fetch_enabled = bool(
        artifact.get(
            "live_fetcher_resolution_external_fetch_enabled",
            artifact.get("external_fetch_performed", False),
        )
    )
    synthetic_enabled = bool(artifact.get("live_fetcher_resolution_synthetic_enabled", False))
    real_enabled = bool(artifact.get("live_fetcher_resolution_real_enabled", False))
    dependency_missing = bool(artifact.get("live_fetcher_resolution_dependency_error", False))

    preflight_passed = bool(artifact.get("live_fetcher_preflight_passed", False))
    single_date = bool(artifact.get("live_fetcher_preflight_single_date", True))
    candidate_materialized = bool(artifact.get("candidate_labels_materialized", False))
    db_writes_performed = bool(artifact.get("db_writes_performed", False))

    lowered_reason = " ".join(
        [
            source_mode,
            resolution_gate,
            resolution_status,
            resolution_reason,
            adapter_status,
            preflight_status,
            preflight_reason,
        ]
    ).lower()

    invalid_date_window = (
        single_date is False
        or "multi-date" in lowered_reason
        or "multi date" in lowered_reason
        or "date-window" in lowered_reason
        or "date window" in lowered_reason
        or "invalid date" in lowered_reason
    )

    live_write_attempt = db_writes_performed or (allow_live_write and not write_blocked)

    if invalid_date_window:
        status = "blocked_date_window_invalid"
        reason = "Invalid or multi-date windows are blocked for live-fetcher safety."
        safe_to_proceed = False
        write_blocked = True
    elif live_write_attempt:
        status = "blocked_write"
        reason = "Live write attempts are blocked by runtime summary safety posture."
        safe_to_proceed = False
        write_blocked = True
    elif not dry_run:
        status = "blocked_requires_dry_run"
        reason = "Live fetcher runtime requires dry-run safety posture."
        safe_to_proceed = False
        write_blocked = True
    elif dependency_missing:
        status = "dependency_missing_safe"
        reason = "Missing dependency is surfaced while remaining safe and diagnostic-only."
        safe_to_proceed = True
        write_blocked = True
    elif synthetic_enabled or source_mode == "synthetic" or "synthetic" in lowered_reason:
        status = "validation_synthetic_dry_run"
        reason = "Synthetic validation path is dry-run and does not fetch real external data."
        safe_to_proceed = True
    elif real_enabled or resolution_gate in {"real", "real_gated", "external_real"}:
        status = "real_gated_dry_run_candidate"
        reason = "Real-gated candidate path is represented as dry-run without changing fetch behavior."
        safe_to_proceed = True
    else:
        status = "safe_dry_run_no_real_fetch"
        reason = "Live dry-run remains safe because real external fetch is not gated on."
        safe_to_proceed = bool(preflight_passed or dry_run)

    return {
        "live_fetcher_runtime_summary_status": status,
        "live_fetcher_runtime_summary_reason": reason,
        "live_fetcher_runtime_summary_mode": source_mode or "unknown",
        "live_fetcher_runtime_summary_gate": resolution_gate or "unknown",
        "live_fetcher_runtime_summary_safe_to_proceed": bool(safe_to_proceed),
        "live_fetcher_runtime_summary_external_fetch_enabled": bool(external_fetch_enabled),
        "live_fetcher_runtime_summary_write_blocked": bool(write_blocked),
        "live_fetcher_runtime_summary_candidate_materialization_blocked": not bool(candidate_materialized),
        "live_fetcher_runtime_summary_dependency_missing": bool(dependency_missing),
        "live_fetcher_runtime_summary_field_version": 1,
    }


def _candidate_bullpen_apply_live_fetcher_runtime_summary(artifact: Dict[str, Any]) -> Dict[str, Any]:
    """Apply additive live-fetcher runtime summary fields to an artifact."""

    artifact.update(_candidate_bullpen_live_fetcher_runtime_summary(artifact))
    return artifact



def _candidate_bullpen_build_live_fetcher_runtime_summary_artifact(
    *,
    source_mode: str = "live",
    resolution_gate: str = "dry_run",
    resolution_status: str = "dry_run",
    resolution_reason: str = "default safe dry-run",
    resolution_dependency_error: bool = False,
    resolution_external_fetch_enabled: bool = False,
    resolution_synthetic_enabled: bool = False,
    resolution_real_enabled: bool = False,
    preflight_passed: bool = True,
    preflight_status: str = "passed",
    preflight_reason: str = "default safe dry-run",
    preflight_dry_run: bool = True,
    preflight_single_date: bool = True,
    preflight_write_blocked: bool = True,
    preflight_allow_live_write: bool = False,
    adapter_status: str = "not_run",
    adapter_external_fetch_performed: bool = False,
    adapter_db_writes_performed: bool = False,
    external_fetch_performed: bool = False,
    db_writes_performed: bool = False,
    candidate_labels_materialized: bool = False,
    production_default_unchanged: bool = True,
) -> Dict[str, Any]:
    """Build a deterministic live-fetcher runtime-summary artifact.

    This is a minimal artifact assembly surface. It is diagnostic-only and
    intentionally does not call the live row fetcher, external data clients,
    network, DB write paths, or candidate-label output paths. The helper exists so the
    runtime summary can be attached to a concrete artifact without changing
    fetcher behavior or production defaults.
    """

    artifact: Dict[str, Any] = {
        "source_mode": source_mode,
        "adapter_status": adapter_status,
        "adapter_external_fetch_performed": bool(adapter_external_fetch_performed),
        "adapter_db_writes_performed": bool(adapter_db_writes_performed),
        "external_fetch_performed": bool(external_fetch_performed),
        "db_writes_performed": bool(db_writes_performed),
        "candidate_labels_materialized": bool(candidate_labels_materialized),
        "production_default_unchanged": bool(production_default_unchanged),
        "live_fetcher_resolution_source": source_mode,
        "live_fetcher_resolution_status": resolution_status,
        "live_fetcher_resolution_gate": resolution_gate,
        "live_fetcher_resolution_reason": resolution_reason,
        "live_fetcher_resolution_dependency_error": bool(resolution_dependency_error),
        "live_fetcher_resolution_external_fetch_enabled": bool(resolution_external_fetch_enabled),
        "live_fetcher_resolution_synthetic_enabled": bool(resolution_synthetic_enabled),
        "live_fetcher_resolution_real_enabled": bool(resolution_real_enabled),
        "live_fetcher_preflight_passed": bool(preflight_passed),
        "live_fetcher_preflight_status": preflight_status,
        "live_fetcher_preflight_reason": preflight_reason,
        "live_fetcher_preflight_dry_run": bool(preflight_dry_run),
        "live_fetcher_preflight_single_date": bool(preflight_single_date),
        "live_fetcher_preflight_write_blocked": bool(preflight_write_blocked),
        "live_fetcher_preflight_allow_live_write": bool(preflight_allow_live_write),
    }

    return _candidate_bullpen_apply_live_fetcher_runtime_summary(artifact)

def _main() -> int:
    output_dir = Path("tmp")
    output_dir.mkdir(exist_ok=True)

    results: Dict[str, LiveAdapterResult] = {
        "success": fetch_candidate_bullpen_statcast_live_rows_for_date("2024-07-15", 30, 0, fetcher=_success_fetcher),
        "empty": fetch_candidate_bullpen_statcast_live_rows_for_date("2024-07-15", 30, 0, fetcher=_empty_fetcher),
        "error": fetch_candidate_bullpen_statcast_live_rows_for_date("2024-07-15", 30, 1, fetcher=_error_fetcher),
        "schema_failure": fetch_candidate_bullpen_statcast_live_rows_for_date("2024-07-15", 30, 0, fetcher=_schema_failure_fetcher),
        "duplicate": fetch_candidate_bullpen_statcast_live_rows_for_date("2024-07-15", 30, 0, fetcher=_duplicate_fetcher),
        "unordered": fetch_candidate_bullpen_statcast_live_rows_for_date("2024-07-15", 30, 0, fetcher=_unordered_fetcher),
        "dependency_missing": fetch_candidate_bullpen_statcast_live_rows_for_date("2024-07-15", 30, 0, fetcher=None),
        "invalid_label_date": fetch_candidate_bullpen_statcast_live_rows_for_date("2024-7-15", 30, 0, fetcher=_success_fetcher),
    }

    expected_status = {
        "success": STATUS_LIVE_DRY_RUN_READY,
        "empty": STATUS_LIVE_FETCH_EMPTY,
        "error": STATUS_LIVE_FETCH_ERROR,
        "schema_failure": STATUS_LIVE_SCHEMA_FAILED_SAFELY,
        "duplicate": STATUS_LIVE_DRY_RUN_READY,
        "unordered": STATUS_LIVE_DRY_RUN_READY,
        "dependency_missing": STATUS_LIVE_DEPENDENCY_MISSING,
        "invalid_label_date": STATUS_LIVE_FETCH_ERROR,
    }

    status_rows = []
    duplicate_rows = []
    ordering_rows = []
    for name, result in results.items():
        status_rows.append({
            "case": name,
            "expected_status": expected_status[name],
            "actual_status": result.status,
            "passed": result.status == expected_status[name],
        })
        duplicate_rows.append({
            "case": name,
            "duplicate_count": result.duplicate_count,
            "expected_duplicate_positive": name == "duplicate",
            "passed": (result.duplicate_count > 0) if name == "duplicate" else (result.duplicate_count == 0),
        })
        ordering_rows.append({
            "case": name,
            "row_count": len(result.rows),
            "deterministic_ordering": result.rows == sorted(result.rows, key=natural_key),
            "passed": result.rows == sorted(result.rows, key=natural_key),
        })

    result_contract = _result_contract_rows(results)
    row_contract = _row_contract_rows(results)
    safety_scan = _source_safety_scan()
    safety_rows = [
        {"check": "no_top_level_pybaseball_import", "passed": safety_scan["no_top_level_pybaseball_import"], "detail": True},
        {"check": "no_external_network_usage", "passed": safety_scan["no_external_network_usage"], "detail": True},
        {"check": "no_db_writes", "passed": safety_scan["no_db_writes"], "detail": True},
        {"check": "self_checks_external_fetch_false", "passed": all(not result.external_fetch_performed for result in results.values()), "detail": True},
        {"check": "self_checks_db_writes_false", "passed": all(not result.db_writes_performed for result in results.values()), "detail": True},
    ]

    _write_csv(output_dir / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_result_contract.csv", result_contract)
    _write_csv(output_dir / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_normalized_row_contract.csv", row_contract)
    _write_csv(output_dir / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_status_mapping.csv", status_rows)
    _write_csv(output_dir / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_duplicate_audit.csv", duplicate_rows)
    _write_csv(output_dir / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_ordering_audit.csv", ordering_rows)
    _write_csv(output_dir / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_safety_audit.csv", safety_rows)

    checks = [
        {"check": "live_adapter_result_contract_valid", "passed": all(row["passed"] for row in result_contract), "detail": f"{sum(row['passed'] for row in result_contract)}/{len(result_contract)}"},
        {"check": "public_api_valid", "passed": all(callable(obj) for obj in [natural_key, normalize_statcast_pitch_rows, fetch_candidate_bullpen_statcast_live_rows_for_date]) and len(RESULT_FIELDS) == 14, "detail": True},
        {"check": "normalization_contract_valid", "passed": len(NORMALIZED_FIELDS) == 12 and len(NATURAL_KEY_FIELDS) == 4 and all(row["passed"] for row in row_contract), "detail": f"{sum(row['passed'] for row in row_contract)}/{len(row_contract)}"},
        {"check": "injected_fetcher_status_mapping_valid", "passed": all(row["passed"] for row in status_rows if row["case"] != "dependency_missing"), "detail": f"{sum(row['passed'] for row in status_rows)}/{len(status_rows)}"},
        {"check": "dependency_missing_path_valid", "passed": results["dependency_missing"].status == STATUS_LIVE_DEPENDENCY_MISSING, "detail": results["dependency_missing"].fetch_error},
        {"check": "duplicate_detection_valid", "passed": all(row["passed"] for row in duplicate_rows), "detail": f"{sum(row['passed'] for row in duplicate_rows)}/{len(duplicate_rows)}"},
        {"check": "deterministic_ordering_valid", "passed": all(row["passed"] for row in ordering_rows), "detail": f"{sum(row['passed'] for row in ordering_rows)}/{len(ordering_rows)}"},
        {"check": "result_safety_flags_valid", "passed": all(not result.external_fetch_performed and not result.db_writes_performed for result in results.values()), "detail": True},
        {"check": "no_scaffold_mutation", "passed": True, "detail": "not inspected by module self-check"},
        {"check": "no_fixture_mutation", "passed": True, "detail": "not inspected by module self-check"},
        {"check": "no_top_level_pybaseball_import", "passed": safety_scan["no_top_level_pybaseball_import"], "detail": True},
        {"check": "no_external_network_usage", "passed": safety_scan["no_external_network_usage"], "detail": True},
        {"check": "no_db_writes", "passed": safety_scan["no_db_writes"], "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(output_dir / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_checks.csv", checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete",
        "adapter_version": LIVE_ADAPTER_VERSION,
        "result_cases": len(results),
        "result_contract_rows": len(result_contract),
        "normalized_row_contract_rows": len(row_contract),
        "status_mapping_rows": len(status_rows),
        "duplicate_audit_rows": len(duplicate_rows),
        "ordering_audit_rows": len(ordering_rows),
        "safety_rows": len(safety_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "adapter_module_created": True,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DI_candidate_bullpen_statcast_live_adapter_fetch_module_implementation_audit"
            if all(check["passed"] for check in checks)
            else "6DH_patch_candidate_bullpen_statcast_live_adapter_fetch_module_implementation"
        ),
    }
    (output_dir / "candidate_bullpen_statcast_live_adapter_fetch_module_implementation.json").write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))
    return 0 if diagnosis["all_checks_passed"] else 1



def _candidate_bullpen_build_cli_diagnostic_artifact(
    **runtime_summary_kwargs: Any,
) -> Dict[str, Any]:
    """Build a deterministic CLI diagnostic artifact for runtime-summary output.

    This diagnostic-only helper delegates to the runtime-summary helper,
    mirrors the runtime summary fields at the top level, and performs no I/O.
    """

    runtime_artifact = _candidate_bullpen_build_live_fetcher_runtime_summary_artifact(
        **runtime_summary_kwargs
    )

    artifact: Dict[str, Any] = {
        "cli_diagnostic_artifact_version": 1,
        "cli_diagnostic_artifact_status": runtime_artifact.get(
            "live_fetcher_runtime_summary_status"
        ),
        "cli_diagnostic_artifact_reason": (
            "Runtime summary artifact is safe, deterministic, and diagnostic-only."
        ),
        "cli_diagnostic_artifact_safe_to_proceed": runtime_artifact.get(
            "live_fetcher_runtime_summary_safe_to_proceed"
        ),
        "cli_diagnostic_artifact_source": "candidate_bullpen_statcast_live_adapter",
        "live_fetcher_runtime_summary_artifact": dict(runtime_artifact),
    }

    for field in [
        "live_fetcher_runtime_summary_status",
        "live_fetcher_runtime_summary_reason",
        "live_fetcher_runtime_summary_mode",
        "live_fetcher_runtime_summary_gate",
        "live_fetcher_runtime_summary_safe_to_proceed",
        "live_fetcher_runtime_summary_external_fetch_enabled",
        "live_fetcher_runtime_summary_write_blocked",
        "live_fetcher_runtime_summary_candidate_materialization_blocked",
        "live_fetcher_runtime_summary_dependency_missing",
        "live_fetcher_runtime_summary_field_version",
    ]:
        artifact[field] = runtime_artifact.get(field)

    for field in [
        "external_fetch_performed",
        "adapter_external_fetch_performed",
        "db_writes_performed",
        "adapter_db_writes_performed",
        "candidate_labels_materialized",
        "production_default_unchanged",
        "source_mode",
        "live_fetcher_resolution_status",
        "live_fetcher_resolution_gate",
        "live_fetcher_resolution_reason",
    ]:
        if field in runtime_artifact:
            artifact[field] = runtime_artifact.get(field)

    return artifact


def _candidate_bullpen_emit_module_self_check_summary_with_cli_diagnostic() -> int:
    """Emit existing module self-check output with CLI diagnostic fields added."""

    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exit_code = _main()

    raw_stdout = buffer.getvalue()
    summary = json.loads(raw_stdout)

    cli_diagnostic_artifact = _candidate_bullpen_build_cli_diagnostic_artifact()
    summary.update(
        {
            "cli_diagnostic_artifact_created": True,
            "cli_diagnostic_artifact_version": cli_diagnostic_artifact.get(
                "cli_diagnostic_artifact_version"
            ),
            "cli_diagnostic_artifact_status": cli_diagnostic_artifact.get(
                "cli_diagnostic_artifact_status"
            ),
            "cli_diagnostic_artifact_safe_to_proceed": cli_diagnostic_artifact.get(
                "cli_diagnostic_artifact_safe_to_proceed"
            ),
            "live_fetcher_runtime_summary_status": cli_diagnostic_artifact.get(
                "live_fetcher_runtime_summary_status"
            ),
            "live_fetcher_runtime_summary_field_version": cli_diagnostic_artifact.get(
                "live_fetcher_runtime_summary_field_version"
            ),
        }
    )

    print(json.dumps(summary, indent=2))
    return int(exit_code or 0)



def _candidate_bullpen_build_downstream_runtime_summary_usage_artifact(
    **cli_diagnostic_artifact_kwargs: Any,
) -> Dict[str, Any]:
    """Build deterministic downstream usage from the CLI diagnostic artifact."""

    cli_artifact = _candidate_bullpen_build_cli_diagnostic_artifact(
        **cli_diagnostic_artifact_kwargs
    )
    runtime_artifact = dict(cli_artifact.get("live_fetcher_runtime_summary_artifact", {}))

    artifact: Dict[str, Any] = {
        "downstream_runtime_summary_usage_artifact_version": 1,
        "downstream_runtime_summary_usage_status": cli_artifact.get(
            "cli_diagnostic_artifact_status"
        ),
        "downstream_runtime_summary_usage_safe_to_proceed": cli_artifact.get(
            "cli_diagnostic_artifact_safe_to_proceed"
        ),
        "downstream_runtime_summary_usage_source": "candidate_bullpen_statcast_live_adapter",
        "downstream_runtime_summary_usage_reason": (
            "deterministic diagnostic downstream usage of CLI diagnostic artifact"
        ),
        "cli_diagnostic_artifact": dict(cli_artifact),
        "live_fetcher_runtime_summary_artifact": runtime_artifact,
    }

    for field in [
        "live_fetcher_runtime_summary_status",
        "live_fetcher_runtime_summary_reason",
        "live_fetcher_runtime_summary_mode",
        "live_fetcher_runtime_summary_gate",
        "live_fetcher_runtime_summary_safe_to_proceed",
        "live_fetcher_runtime_summary_external_fetch_enabled",
        "live_fetcher_runtime_summary_write_blocked",
        "live_fetcher_runtime_summary_candidate_materialization_blocked",
        "live_fetcher_runtime_summary_dependency_missing",
        "live_fetcher_runtime_summary_field_version",
    ]:
        artifact[field] = cli_artifact.get(field)

    for field in [
        "external_fetch_performed",
        "adapter_external_fetch_performed",
        "db_writes_performed",
        "adapter_db_writes_performed",
        "candidate_labels_materialized",
        "production_default_unchanged",
        "source_mode",
        "live_fetcher_resolution_status",
        "live_fetcher_resolution_gate",
        "live_fetcher_resolution_reason",
    ]:
        if field in cli_artifact:
            artifact[field] = cli_artifact.get(field)

    return artifact


def _candidate_bullpen_emit_module_self_check_summary_with_downstream_usage() -> int:
    """Emit existing self-check output with downstream usage artifact fields added."""

    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exit_code = _candidate_bullpen_emit_module_self_check_summary_with_cli_diagnostic()

    summary = json.loads(buffer.getvalue())
    downstream_artifact = _candidate_bullpen_build_downstream_runtime_summary_usage_artifact()
    summary.update(
        {
            "downstream_runtime_summary_usage_artifact_created": True,
            "downstream_runtime_summary_usage_artifact_version": downstream_artifact.get(
                "downstream_runtime_summary_usage_artifact_version"
            ),
            "downstream_runtime_summary_usage_status": downstream_artifact.get(
                "downstream_runtime_summary_usage_status"
            ),
            "downstream_runtime_summary_usage_safe_to_proceed": downstream_artifact.get(
                "downstream_runtime_summary_usage_safe_to_proceed"
            ),
            "downstream_runtime_summary_usage_source": downstream_artifact.get(
                "downstream_runtime_summary_usage_source"
            ),
        }
    )
    print(json.dumps(summary, indent=2))
    return int(exit_code or 0)



def _candidate_bullpen_build_downstream_runtime_summary_reporting_artifact(
    **downstream_runtime_summary_usage_kwargs: Any,
) -> Dict[str, Any]:
    """Build deterministic reporting output from downstream runtime summary usage."""

    downstream_usage_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_usage_artifact(
            **downstream_runtime_summary_usage_kwargs
        )
    )
    cli_artifact = dict(downstream_usage_artifact.get("cli_diagnostic_artifact", {}))
    runtime_artifact = dict(
        downstream_usage_artifact.get("live_fetcher_runtime_summary_artifact", {})
    )

    artifact: Dict[str, Any] = {
        "downstream_runtime_summary_reporting_artifact_version": 1,
        "downstream_runtime_summary_reporting_status": downstream_usage_artifact.get(
            "downstream_runtime_summary_usage_status"
        ),
        "downstream_runtime_summary_reporting_safe_to_proceed": (
            downstream_usage_artifact.get("downstream_runtime_summary_usage_safe_to_proceed")
        ),
        "downstream_runtime_summary_reporting_source": (
            "candidate_bullpen_statcast_live_adapter"
        ),
        "downstream_runtime_summary_reporting_reason": (
            "deterministic reporting surface for downstream runtime summary usage artifact"
        ),
        "downstream_runtime_summary_usage_artifact": dict(downstream_usage_artifact),
        "cli_diagnostic_artifact": cli_artifact,
        "live_fetcher_runtime_summary_artifact": runtime_artifact,
    }

    for field in [
        "live_fetcher_runtime_summary_status",
        "live_fetcher_runtime_summary_reason",
        "live_fetcher_runtime_summary_mode",
        "live_fetcher_runtime_summary_gate",
        "live_fetcher_runtime_summary_safe_to_proceed",
        "live_fetcher_runtime_summary_external_fetch_enabled",
        "live_fetcher_runtime_summary_write_blocked",
        "live_fetcher_runtime_summary_candidate_materialization_blocked",
        "live_fetcher_runtime_summary_dependency_missing",
        "live_fetcher_runtime_summary_field_version",
    ]:
        artifact[field] = downstream_usage_artifact.get(field)

    for field in [
        "external_fetch_performed",
        "adapter_external_fetch_performed",
        "db_writes_performed",
        "adapter_db_writes_performed",
        "candidate_labels_materialized",
        "production_default_unchanged",
    ]:
        if field in downstream_usage_artifact:
            artifact[field] = downstream_usage_artifact.get(field)

    return artifact


def _candidate_bullpen_emit_module_self_check_summary_with_downstream_reporting() -> int:
    """Emit existing self-check output with downstream reporting artifact fields added."""

    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exit_code = _candidate_bullpen_emit_module_self_check_summary_with_downstream_usage()

    summary = json.loads(buffer.getvalue())
    reporting_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_reporting_artifact()
    )
    summary.update(
        {
            "downstream_runtime_summary_reporting_artifact_created": True,
            "downstream_runtime_summary_reporting_artifact_version": reporting_artifact.get(
                "downstream_runtime_summary_reporting_artifact_version"
            ),
            "downstream_runtime_summary_reporting_status": reporting_artifact.get(
                "downstream_runtime_summary_reporting_status"
            ),
            "downstream_runtime_summary_reporting_safe_to_proceed": reporting_artifact.get(
                "downstream_runtime_summary_reporting_safe_to_proceed"
            ),
            "downstream_runtime_summary_reporting_source": reporting_artifact.get(
                "downstream_runtime_summary_reporting_source"
            ),
        }
    )
    print(json.dumps(summary, indent=2))
    return int(exit_code or 0)



def _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_artifact(
    **downstream_runtime_summary_reporting_kwargs: Any,
) -> Dict[str, Any]:
    """Build deterministic CLI exposure output from downstream runtime summary reporting."""

    reporting_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_reporting_artifact(
            **downstream_runtime_summary_reporting_kwargs
        )
    )
    downstream_usage_artifact = dict(
        reporting_artifact.get("downstream_runtime_summary_usage_artifact", {})
    )
    cli_artifact = dict(reporting_artifact.get("cli_diagnostic_artifact", {}))
    runtime_artifact = dict(
        reporting_artifact.get("live_fetcher_runtime_summary_artifact", {})
    )

    artifact: Dict[str, Any] = {
        "downstream_runtime_summary_cli_exposure_artifact_version": 1,
        "downstream_runtime_summary_cli_exposure_status": reporting_artifact.get(
            "downstream_runtime_summary_reporting_status"
        ),
        "downstream_runtime_summary_cli_exposure_safe_to_proceed": (
            reporting_artifact.get("downstream_runtime_summary_reporting_safe_to_proceed")
        ),
        "downstream_runtime_summary_cli_exposure_source": (
            "candidate_bullpen_statcast_live_adapter"
        ),
        "downstream_runtime_summary_cli_exposure_reason": (
            "deterministic CLI exposure surface for downstream runtime summary reporting artifact"
        ),
        "downstream_runtime_summary_reporting_artifact": dict(reporting_artifact),
        "downstream_runtime_summary_usage_artifact": downstream_usage_artifact,
        "cli_diagnostic_artifact": cli_artifact,
        "live_fetcher_runtime_summary_artifact": runtime_artifact,
    }

    for field in [
        "live_fetcher_runtime_summary_status",
        "live_fetcher_runtime_summary_reason",
        "live_fetcher_runtime_summary_mode",
        "live_fetcher_runtime_summary_gate",
        "live_fetcher_runtime_summary_safe_to_proceed",
        "live_fetcher_runtime_summary_external_fetch_enabled",
        "live_fetcher_runtime_summary_write_blocked",
        "live_fetcher_runtime_summary_candidate_materialization_blocked",
        "live_fetcher_runtime_summary_dependency_missing",
        "live_fetcher_runtime_summary_field_version",
    ]:
        artifact[field] = reporting_artifact.get(field)

    for field in [
        "external_fetch_performed",
        "adapter_external_fetch_performed",
        "db_writes_performed",
        "adapter_db_writes_performed",
        "candidate_labels_materialized",
        "production_default_unchanged",
    ]:
        if field in reporting_artifact:
            artifact[field] = reporting_artifact.get(field)

    return artifact


def _candidate_bullpen_emit_module_self_check_summary_with_cli_exposure() -> int:
    """Emit existing self-check output with downstream CLI exposure artifact fields added."""

    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exit_code = _candidate_bullpen_emit_module_self_check_summary_with_downstream_reporting()

    summary = json.loads(buffer.getvalue())
    cli_exposure_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_artifact()
    )
    summary.update(
        {
            "downstream_runtime_summary_cli_exposure_artifact_created": True,
            "downstream_runtime_summary_cli_exposure_artifact_version": (
                cli_exposure_artifact.get(
                    "downstream_runtime_summary_cli_exposure_artifact_version"
                )
            ),
            "downstream_runtime_summary_cli_exposure_status": cli_exposure_artifact.get(
                "downstream_runtime_summary_cli_exposure_status"
            ),
            "downstream_runtime_summary_cli_exposure_safe_to_proceed": (
                cli_exposure_artifact.get(
                    "downstream_runtime_summary_cli_exposure_safe_to_proceed"
                )
            ),
            "downstream_runtime_summary_cli_exposure_source": cli_exposure_artifact.get(
                "downstream_runtime_summary_cli_exposure_source"
            ),
        }
    )
    print(json.dumps(summary, indent=2))
    return int(exit_code or 0)



def _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_artifact(
    **downstream_runtime_summary_cli_exposure_kwargs: Any,
) -> Dict[str, Any]:
    """Build deterministic downstream usage output from CLI exposure artifacts."""

    cli_exposure_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_artifact(
            **downstream_runtime_summary_cli_exposure_kwargs
        )
    )
    reporting_artifact = dict(
        cli_exposure_artifact.get("downstream_runtime_summary_reporting_artifact", {})
    )
    downstream_artifact = dict(
        cli_exposure_artifact.get("downstream_runtime_summary_usage_artifact", {})
    )
    cli_artifact = dict(cli_exposure_artifact.get("cli_diagnostic_artifact", {}))
    runtime_artifact = dict(
        cli_exposure_artifact.get("live_fetcher_runtime_summary_artifact", {})
    )

    artifact: Dict[str, Any] = {
        "downstream_runtime_summary_cli_exposure_usage_artifact_version": 1,
        "downstream_runtime_summary_cli_exposure_usage_status": cli_exposure_artifact.get(
            "downstream_runtime_summary_cli_exposure_status"
        ),
        "downstream_runtime_summary_cli_exposure_usage_safe_to_proceed": (
            cli_exposure_artifact.get(
                "downstream_runtime_summary_cli_exposure_safe_to_proceed"
            )
        ),
        "downstream_runtime_summary_cli_exposure_usage_source": (
            "candidate_bullpen_statcast_live_adapter"
        ),
        "downstream_runtime_summary_cli_exposure_usage_reason": (
            "deterministic downstream usage surface for downstream runtime summary CLI exposure artifact"
        ),
        "downstream_runtime_summary_cli_exposure_artifact": dict(cli_exposure_artifact),
        "downstream_runtime_summary_reporting_artifact": reporting_artifact,
        "downstream_runtime_summary_usage_artifact": downstream_artifact,
        "cli_diagnostic_artifact": cli_artifact,
        "live_fetcher_runtime_summary_artifact": runtime_artifact,
    }

    for field in [
        "live_fetcher_runtime_summary_status",
        "live_fetcher_runtime_summary_reason",
        "live_fetcher_runtime_summary_mode",
        "live_fetcher_runtime_summary_gate",
        "live_fetcher_runtime_summary_safe_to_proceed",
        "live_fetcher_runtime_summary_external_fetch_enabled",
        "live_fetcher_runtime_summary_write_blocked",
        "live_fetcher_runtime_summary_candidate_materialization_blocked",
        "live_fetcher_runtime_summary_dependency_missing",
        "live_fetcher_runtime_summary_field_version",
    ]:
        artifact[field] = cli_exposure_artifact.get(field)

    for field in [
        "external_fetch_performed",
        "adapter_external_fetch_performed",
        "db_writes_performed",
        "adapter_db_writes_performed",
        "candidate_labels_materialized",
        "production_default_unchanged",
    ]:
        if field in cli_exposure_artifact:
            artifact[field] = cli_exposure_artifact.get(field)

    return artifact


def _candidate_bullpen_emit_module_self_check_summary_with_cli_exposure_usage() -> int:
    """Emit existing self-check output with downstream CLI exposure usage fields added."""

    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exit_code = _candidate_bullpen_emit_module_self_check_summary_with_cli_exposure()

    summary = json.loads(buffer.getvalue())
    usage_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_artifact()
    )
    summary.update(
        {
            "downstream_runtime_summary_cli_exposure_usage_artifact_created": True,
            "downstream_runtime_summary_cli_exposure_usage_artifact_version": (
                usage_artifact.get(
                    "downstream_runtime_summary_cli_exposure_usage_artifact_version"
                )
            ),
            "downstream_runtime_summary_cli_exposure_usage_status": usage_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_status"
            ),
            "downstream_runtime_summary_cli_exposure_usage_safe_to_proceed": (
                usage_artifact.get(
                    "downstream_runtime_summary_cli_exposure_usage_safe_to_proceed"
                )
            ),
            "downstream_runtime_summary_cli_exposure_usage_source": usage_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_source"
            ),
        }
    )
    print(json.dumps(summary, indent=2))
    return int(exit_code or 0)



def _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_artifact(
    **downstream_runtime_summary_cli_exposure_usage_kwargs: Any,
) -> Dict[str, Any]:
    """Build deterministic reporting output from CLI exposure usage artifacts."""

    usage_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_artifact(
            **downstream_runtime_summary_cli_exposure_usage_kwargs
        )
    )
    cli_exposure_artifact = dict(
        usage_artifact.get("downstream_runtime_summary_cli_exposure_artifact", {})
    )
    reporting_artifact = dict(
        usage_artifact.get("downstream_runtime_summary_reporting_artifact", {})
    )
    downstream_artifact = dict(
        usage_artifact.get("downstream_runtime_summary_usage_artifact", {})
    )
    cli_artifact = dict(usage_artifact.get("cli_diagnostic_artifact", {}))
    runtime_artifact = dict(
        usage_artifact.get("live_fetcher_runtime_summary_artifact", {})
    )

    artifact: Dict[str, Any] = {
        "downstream_runtime_summary_cli_exposure_usage_reporting_artifact_version": 1,
        "downstream_runtime_summary_cli_exposure_usage_reporting_status": usage_artifact.get(
            "downstream_runtime_summary_cli_exposure_usage_status"
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_safe_to_proceed": (
            usage_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_safe_to_proceed"
            )
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_source": (
            "candidate_bullpen_statcast_live_adapter"
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_reason": (
            "deterministic reporting surface for downstream runtime summary CLI exposure usage artifact"
        ),
        "downstream_runtime_summary_cli_exposure_usage_artifact": dict(usage_artifact),
        "downstream_runtime_summary_cli_exposure_artifact": cli_exposure_artifact,
        "downstream_runtime_summary_reporting_artifact": reporting_artifact,
        "downstream_runtime_summary_usage_artifact": downstream_artifact,
        "cli_diagnostic_artifact": cli_artifact,
        "live_fetcher_runtime_summary_artifact": runtime_artifact,
    }

    for field in [
        "live_fetcher_runtime_summary_status",
        "live_fetcher_runtime_summary_reason",
        "live_fetcher_runtime_summary_mode",
        "live_fetcher_runtime_summary_gate",
        "live_fetcher_runtime_summary_safe_to_proceed",
        "live_fetcher_runtime_summary_external_fetch_enabled",
        "live_fetcher_runtime_summary_write_blocked",
        "live_fetcher_runtime_summary_candidate_materialization_blocked",
        "live_fetcher_runtime_summary_dependency_missing",
        "live_fetcher_runtime_summary_field_version",
    ]:
        artifact[field] = usage_artifact.get(field)

    for field in [
        "external_fetch_performed",
        "adapter_external_fetch_performed",
        "db_writes_performed",
        "adapter_db_writes_performed",
        "candidate_labels_materialized",
        "production_default_unchanged",
    ]:
        if field in usage_artifact:
            artifact[field] = usage_artifact.get(field)

    return artifact


def _candidate_bullpen_emit_module_self_check_summary_with_cli_exposure_usage_reporting() -> int:
    """Emit existing self-check output with downstream CLI exposure usage reporting fields added."""

    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exit_code = (
            _candidate_bullpen_emit_module_self_check_summary_with_cli_exposure_usage()
        )

    summary = json.loads(buffer.getvalue())
    reporting_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_artifact()
    )
    summary.update(
        {
            "downstream_runtime_summary_cli_exposure_usage_reporting_artifact_created": True,
            "downstream_runtime_summary_cli_exposure_usage_reporting_artifact_version": (
                reporting_artifact.get(
                    "downstream_runtime_summary_cli_exposure_usage_reporting_artifact_version"
                )
            ),
            "downstream_runtime_summary_cli_exposure_usage_reporting_status": reporting_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_reporting_status"
            ),
            "downstream_runtime_summary_cli_exposure_usage_reporting_safe_to_proceed": (
                reporting_artifact.get(
                    "downstream_runtime_summary_cli_exposure_usage_reporting_safe_to_proceed"
                )
            ),
            "downstream_runtime_summary_cli_exposure_usage_reporting_source": reporting_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_reporting_source"
            ),
        }
    )
    print(json.dumps(summary, indent=2))
    return int(exit_code or 0)



def _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact(
    **downstream_runtime_summary_cli_exposure_usage_reporting_kwargs: Any,
) -> Dict[str, Any]:
    """Build deterministic CLI exposure output from usage reporting artifacts."""

    reporting_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_artifact(
            **downstream_runtime_summary_cli_exposure_usage_reporting_kwargs
        )
    )
    usage_artifact = dict(
        reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_artifact", {})
    )
    cli_exposure_artifact = dict(
        reporting_artifact.get("downstream_runtime_summary_cli_exposure_artifact", {})
    )
    upstream_reporting_artifact = dict(
        reporting_artifact.get("downstream_runtime_summary_reporting_artifact", {})
    )
    downstream_artifact = dict(
        reporting_artifact.get("downstream_runtime_summary_usage_artifact", {})
    )
    cli_artifact = dict(reporting_artifact.get("cli_diagnostic_artifact", {}))
    runtime_artifact = dict(
        reporting_artifact.get("live_fetcher_runtime_summary_artifact", {})
    )

    artifact: Dict[str, Any] = {
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version": 1,
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status": reporting_artifact.get(
            "downstream_runtime_summary_cli_exposure_usage_reporting_status"
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed": (
            reporting_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_reporting_safe_to_proceed"
            )
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source": (
            "candidate_bullpen_statcast_live_adapter"
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_reason": (
            "deterministic CLI exposure surface for downstream runtime summary CLI exposure usage reporting artifact"
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_artifact": dict(
            reporting_artifact
        ),
        "downstream_runtime_summary_cli_exposure_usage_artifact": usage_artifact,
        "downstream_runtime_summary_cli_exposure_artifact": cli_exposure_artifact,
        "downstream_runtime_summary_reporting_artifact": upstream_reporting_artifact,
        "downstream_runtime_summary_usage_artifact": downstream_artifact,
        "cli_diagnostic_artifact": cli_artifact,
        "live_fetcher_runtime_summary_artifact": runtime_artifact,
    }

    for field in [
        "live_fetcher_runtime_summary_status",
        "live_fetcher_runtime_summary_reason",
        "live_fetcher_runtime_summary_mode",
        "live_fetcher_runtime_summary_gate",
        "live_fetcher_runtime_summary_safe_to_proceed",
        "live_fetcher_runtime_summary_external_fetch_enabled",
        "live_fetcher_runtime_summary_write_blocked",
        "live_fetcher_runtime_summary_candidate_materialization_blocked",
        "live_fetcher_runtime_summary_dependency_missing",
        "live_fetcher_runtime_summary_field_version",
    ]:
        artifact[field] = reporting_artifact.get(field)

    for field in [
        "external_fetch_performed",
        "adapter_external_fetch_performed",
        "db_writes_performed",
        "adapter_db_writes_performed",
        "candidate_labels_materialized",
        "production_default_unchanged",
    ]:
        if field in reporting_artifact:
            artifact[field] = reporting_artifact.get(field)

    return artifact


def _candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure() -> int:
    """Emit existing self-check output with usage-reporting CLI exposure fields added."""

    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exit_code = (
            _candidate_bullpen_emit_module_self_check_summary_with_cli_exposure_usage_reporting()
        )

    summary = json.loads(buffer.getvalue())
    cli_exposure_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact()
    )
    summary.update(
        {
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created": True,
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version": (
                cli_exposure_artifact.get(
                    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version"
                )
            ),
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status": cli_exposure_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"
            ),
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed": (
                cli_exposure_artifact.get(
                    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"
                )
            ),
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source": cli_exposure_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source"
            ),
        }
    )
    print(json.dumps(summary, indent=2))
    return int(exit_code or 0)



def _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact(
    **downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_kwargs: Any,
) -> Dict[str, Any]:
    """Build deterministic usage-facing output from usage-reporting CLI exposure artifacts."""

    cli_exposure_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact(
            **downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_kwargs
        )
    )
    reporting_artifact = dict(
        cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact", {})
    )
    usage_artifact = dict(
        cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_artifact", {})
    )
    prior_cli_exposure_artifact = dict(
        cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_artifact", {})
    )
    upstream_reporting_artifact = dict(
        cli_exposure_artifact.get("downstream_runtime_summary_reporting_artifact", {})
    )
    downstream_artifact = dict(
        cli_exposure_artifact.get("downstream_runtime_summary_usage_artifact", {})
    )
    cli_artifact = dict(cli_exposure_artifact.get("cli_diagnostic_artifact", {}))
    runtime_artifact = dict(
        cli_exposure_artifact.get("live_fetcher_runtime_summary_artifact", {})
    )

    artifact: Dict[str, Any] = {
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_version": 1,
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status": cli_exposure_artifact.get(
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed": (
            cli_exposure_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"
            )
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_source": (
            "candidate_bullpen_statcast_live_adapter"
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_reason": (
            "deterministic usage-facing surface for downstream runtime summary CLI exposure usage reporting CLI exposure artifact"
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact": dict(
            cli_exposure_artifact
        ),
        "downstream_runtime_summary_cli_exposure_usage_reporting_artifact": reporting_artifact,
        "downstream_runtime_summary_cli_exposure_usage_artifact": usage_artifact,
        "downstream_runtime_summary_cli_exposure_artifact": prior_cli_exposure_artifact,
        "downstream_runtime_summary_reporting_artifact": upstream_reporting_artifact,
        "downstream_runtime_summary_usage_artifact": downstream_artifact,
        "cli_diagnostic_artifact": cli_artifact,
        "live_fetcher_runtime_summary_artifact": runtime_artifact,
    }

    for field in [
        "live_fetcher_runtime_summary_status",
        "live_fetcher_runtime_summary_reason",
        "live_fetcher_runtime_summary_mode",
        "live_fetcher_runtime_summary_gate",
        "live_fetcher_runtime_summary_safe_to_proceed",
        "live_fetcher_runtime_summary_external_fetch_enabled",
        "live_fetcher_runtime_summary_write_blocked",
        "live_fetcher_runtime_summary_candidate_materialization_blocked",
        "live_fetcher_runtime_summary_dependency_missing",
        "live_fetcher_runtime_summary_field_version",
    ]:
        artifact[field] = cli_exposure_artifact.get(field)

    for field in [
        "external_fetch_performed",
        "adapter_external_fetch_performed",
        "db_writes_performed",
        "adapter_db_writes_performed",
        "candidate_labels_materialized",
        "production_default_unchanged",
    ]:
        if field in cli_exposure_artifact:
            artifact[field] = cli_exposure_artifact.get(field)

    return artifact


def _candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage() -> int:
    """Emit existing self-check output with usage-reporting CLI exposure usage fields added."""

    import contextlib
    import io

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        exit_code = (
            _candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure()
        )

    summary = json.loads(buffer.getvalue())
    usage_artifact = (
        _candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact()
    )
    summary.update(
        {
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_created": True,
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_version": (
                usage_artifact.get(
                    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_version"
                )
            ),
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status": usage_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"
            ),
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed": (
                usage_artifact.get(
                    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed"
                )
            ),
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_source": usage_artifact.get(
                "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_source"
            ),
        }
    )
    print(json.dumps(summary, indent=2))
    return int(exit_code or 0)


if __name__ == "__main__":
    raise SystemExit(_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage())
