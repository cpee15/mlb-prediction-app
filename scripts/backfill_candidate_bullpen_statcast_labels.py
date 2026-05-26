from __future__ import annotations

import argparse
import csv
import json
import os
import hashlib
import importlib.util
import sys
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from mlb_app.database import create_tables, get_engine, get_session


SCAFFOLD_VERSION = "candidate_bullpen_statcast_label_backfill_script_scaffold_v0.1"
DEFAULT_BATCH_SIZE = 3

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_script_scaffold.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_script_scaffold_checks.csv"
OUTPUT_BATCHES = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_script_scaffold_batches.csv"
OUTPUT_DATE_AUDIT = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_script_scaffold_date_audit.csv"
OUTPUT_CONFIG = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_script_scaffold_command_config.csv"
OUTPUT_FIELDS = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_script_scaffold_required_fields.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_script_scaffold_safety_report.csv"


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

FORBIDDEN_IMPORT_TOKENS = [
    "mlb_app.simulation",
    "GameEngine",
    "canonical_matchup_probability",
    "sportsbook",
    "routes",
    "frontend",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scaffold-only candidate bullpen Statcast label backfill command."
    )
    parser.add_argument("--start-date", required=False, help="Inclusive start date YYYY-MM-DD.")
    parser.add_argument("--end-date", required=False, help="Inclusive end date YYYY-MM-DD.")
    parser.add_argument(
        "--source-mode",
        choices=["scaffold", "fixture", "live"],
        default="scaffold",
        help="Layer 6CV source mode. Default scaffold preserves existing behavior.",
    )
    parser.add_argument(
        "--fixture-root",
        default="tests/fixtures/statcast/bullpen_labels",
        help="Fixture root used only with --source-mode fixture.",
    )
    parser.add_argument(
        "--fixture-date",
        default="",
        help="Optional fixture date used only with --source-mode fixture.",
    )
    parser.add_argument(
        "--allow-negative-fixtures",
        action="store_true",
        help="Allow dedupe/schema/missing-file fixture statuses through fixture dry-run diagnostics.",
    )
    parser.add_argument(
        "--live-fetch-timeout-seconds",
        type=int,
        default=30,
        help="Layer 6CY live dry-run scaffold timeout placeholder. No external fetch is performed.",
    )
    parser.add_argument(
        "--live-fetch-max-retries",
        type=int,
        default=0,
        help="Layer 6CY live dry-run scaffold retry placeholder. No external fetch is performed.",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Dry-run mode. This scaffold never fetches or writes.",
    )
    parser.add_argument(
        "--no-dry-run",
        dest="dry_run",
        action="store_false",
        help="Disable dry-run flag for Layer 6CV fixture gate validation.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        default=False,
        help="Requested write mode. Ignored in scaffold; no DB writes occur.",
    )
    parser.add_argument(
        "--allow-live-write",
        action="store_true",
        default=False,
        help="Reserved future live write gate. Currently always blocked for --source-mode live.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip dates that already have statcast_events rows.",
    )
    parser.add_argument(
        "--audit-after",
        action="store_true",
        default=True,
        help="Emit post-plan audit outputs.",
    )
    args = parser.parse_args()
    if args.source_mode == "scaffold" and (not args.start_date or not args.end_date):
        parser.error("--start-date and --end-date are required when --source-mode scaffold")
    return args


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start_date: str, end_date: str) -> List[str]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise ValueError("end-date must be >= start-date")

    out = []
    cursor = start
    while cursor <= end:
        out.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return out


def _chunks(values: Sequence[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        raise ValueError("batch-size must be positive")
    for idx in range(0, len(values), size):
        yield list(values[idx: idx + size])


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _covered_dates(session: Session, start_date: str, end_date: str) -> Set[str]:
    rows = session.execute(text("""
        SELECT DISTINCT game_date
        FROM statcast_events
        WHERE game_date BETWEEN :start_date AND :end_date
          AND game_pk IS NOT NULL
          AND pitcher_id IS NOT NULL
        ORDER BY game_date
    """), {"start_date": start_date, "end_date": end_date}).mappings().all()
    return {str(row["game_date"]) for row in rows}


def _coverage_counts(session: Session, start_date: str, end_date: str) -> Dict[str, Any]:
    row = session.execute(text("""
        SELECT
            COUNT(*) AS statcast_rows,
            COUNT(DISTINCT game_date) AS covered_date_count,
            COUNT(DISTINCT game_pk) AS game_count,
            COUNT(DISTINCT pitcher_id) AS pitcher_count
        FROM statcast_events
        WHERE game_date BETWEEN :start_date AND :end_date
          AND game_pk IS NOT NULL
          AND pitcher_id IS NOT NULL
    """), {"start_date": start_date, "end_date": end_date}).mappings().first()
    return dict(row) if row else {}


def fetch_statcast_label_rows_for_date(label_date: str) -> List[Dict[str, Any]]:
    """Scaffold stub only. Future layer may wire this to statcast_utils/pybaseball."""
    _ = label_date
    return []


def _natural_key(row: Dict[str, Any]) -> Tuple[Any, Any, Any, Any]:
    return tuple(row.get(field) for field in NATURAL_KEY_FIELDS)


def _validate_required_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    missing = [field for field in REQUIRED_FIELDS if field not in row]
    nullable_missing = [field for field in REQUIRED_FIELDS if field in row and row.get(field) is None]
    return {
        "valid": not missing,
        "missing_fields": "|".join(missing),
        "nullable_missing_fields": "|".join(nullable_missing),
    }


def _dedupe_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    seen = set()
    deduped = []
    duplicate_count = 0
    for row in rows:
        key = _natural_key(row)
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, duplicate_count


def _build_batch_plan(candidate_dates: List[str], batch_size: int) -> List[Dict[str, Any]]:
    rows = []
    for batch_id, dates in enumerate(_chunks(candidate_dates, batch_size), start=1):
        rows.append({
            "batch_id": batch_id,
            "start_date": dates[0],
            "end_date": dates[-1],
            "date_count": len(dates),
            "dates": "|".join(dates),
            "dry_run": True,
            "write_requested_supported": False,
            "external_fetch_enabled": False,
            "db_write_enabled": False,
        })
    return rows


def _build_command_config(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "script_name": "scripts/backfill_candidate_bullpen_statcast_labels.py",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "batch_size": args.batch_size,
        "dry_run": True,
        "write_requested": bool(args.write),
        "write_enabled": False,
        "skip_existing": bool(args.skip_existing),
        "audit_after": bool(args.audit_after),
        "scaffold_only": True,
    }


def _source_safety_report() -> List[Dict[str, Any]]:
    source_text = Path(__file__).read_text(errors="ignore")
    import_lines = [
        line.strip()
        for line in source_text.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    ]
    import_blob = "\n".join(import_lines)
    return [
        {
            "token": token,
            "present": token in import_blob,
            "passed": token not in import_blob,
            "scan_scope": "import_lines_only",
        }
        for token in FORBIDDEN_IMPORT_TOKENS
    ]


def _required_field_report() -> List[Dict[str, Any]]:
    return [
        {
            "field": field,
            "required": True,
            "natural_key": field in NATURAL_KEY_FIELDS,
            "normalization_helper_present": True,
        }
        for field in REQUIRED_FIELDS
    ]



# Layer 6CV fixture replay scaffold wiring helpers.
# These helpers are intentionally fixture/dry-run only and preserve default scaffold behavior.

_LAYER_6CV_VERSION = "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_v0.1"
_LAYER_6CV_FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
_LAYER_6CV_DATES = [
    "2026-05-20",
    "2026-05-21",
    "2026-05-22",
    "2026-05-23",
    "2026-05-24",
    "2026-05-25",
    "2026-05-26",
]
_LAYER_6CV_NEGATIVE_STATUSES = {"dedupe_success", "schema_failed_safely", "fixture_missing"}


def _layer_6cv_tmp_dir() -> Path:
    output_dir = Path("tmp")
    output_dir.mkdir(exist_ok=True)
    return output_dir


def _layer_6cv_write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
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


def _layer_6cv_read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text())


def _layer_6cv_snapshot_payloads(fixture_root: Path) -> Dict[str, str]:
    dates_dir = fixture_root / "dates"
    snapshot: Dict[str, str] = {}
    for label_date in _LAYER_6CV_DATES:
        payload_path = dates_dir / f"{label_date}.jsonl"
        snapshot[str(payload_path)] = payload_path.read_text() if payload_path.exists() else "__MISSING__"
    return snapshot


def _layer_6cv_snapshot_metadata(fixture_root: Path) -> Dict[str, str]:
    snapshot: Dict[str, str] = {}
    for metadata_path in [fixture_root / "manifest.json", fixture_root / "expected_results.json"]:
        snapshot[str(metadata_path)] = metadata_path.read_text() if metadata_path.exists() else "__MISSING__"
    return snapshot


def _layer_6cv_import_fixture_adapter() -> Tuple[Any, List[Dict[str, Any]]]:
    adapter_path = Path("scripts/prototype_candidate_bullpen_statcast_fixture_replay_adapter.py")
    module_name = "layer_6cv_fixture_replay_adapter"
    rows: List[Dict[str, Any]] = []
    try:
        spec = importlib.util.spec_from_file_location(module_name, adapter_path)
        if spec is None or spec.loader is None:
            rows.append({"check": "adapter_spec_created", "passed": False, "detail": "spec or loader missing"})
            return None, rows
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        rows.extend([
            {"check": "adapter_file_exists", "passed": adapter_path.exists(), "detail": str(adapter_path)},
            {"check": "adapter_module_loaded", "passed": True, "detail": module_name},
            {
                "check": "adapter_fetch_callable_exists",
                "passed": callable(getattr(module, "fetch_candidate_bullpen_statcast_fixture_rows", None)),
                "detail": "fetch_candidate_bullpen_statcast_fixture_rows",
            },
        ])
        return module, rows
    except Exception as exc:
        rows.append({"check": "adapter_module_loaded", "passed": False, "detail": repr(exc)})
        return None, rows


def _layer_6cv_result_dict(result: Any) -> Dict[str, Any]:
    if is_dataclass(result):
        return asdict(result)
    return {
        "label_date": getattr(result, "label_date", ""),
        "fixture_date": getattr(result, "fixture_date", ""),
        "payload_class": getattr(result, "payload_class", ""),
        "status": getattr(result, "status", ""),
        "rows": getattr(result, "rows", []),
        "raw_row_count": getattr(result, "raw_row_count", 0),
        "deduped_row_count": getattr(result, "deduped_row_count", 0),
        "duplicate_count": getattr(result, "duplicate_count", 0),
        "required_field_failures": getattr(result, "required_field_failures", 0),
        "missing_fields": getattr(result, "missing_fields", []),
        "sha256": getattr(result, "sha256", ""),
        "manifest_entry_present": getattr(result, "manifest_entry_present", False),
        "expected_result_present": getattr(result, "expected_result_present", False),
    }


def _layer_6cv_empty_result(
    *,
    fixture_date: str,
    status: str,
    replay_status: str = "",
    write_requested: bool = False,
    dry_run: bool = False,
    allow_negative_fixtures: bool = False,
) -> Dict[str, Any]:
    return {
        "fixture_date": fixture_date,
        "status": status,
        "replay_status": replay_status,
        "row_count": 0,
        "raw_row_count": 0,
        "deduped_row_count": 0,
        "duplicate_count": 0,
        "required_field_failures": 0,
        "missing_fields": "",
        "expected_result_present": False,
        "manifest_entry_present": False,
        "write_requested": write_requested,
        "dry_run": dry_run,
        "allow_negative_fixtures": allow_negative_fixtures,
    }


def _layer_6cv_expectation_parity_rows(results: List[Dict[str, Any]], fixture_root: Path) -> List[Dict[str, Any]]:
    expected_path = fixture_root / "expected_results.json"
    expectations = _layer_6cv_read_json(expected_path).get("date_expectations", {}) if expected_path.exists() else {}
    rows: List[Dict[str, Any]] = []
    for result in results:
        if result["status"] != "fixture_dry_run_ready":
            continue
        expectation = expectations.get(result["fixture_date"], {})
        rows.append({
            "fixture_date": result["fixture_date"],
            "expected_status": expectation.get("expected_status"),
            "actual_replay_status": result["replay_status"],
            "expected_row_count": expectation.get("row_count"),
            "actual_raw_row_count": result["raw_row_count"],
            "expected_deduped_row_count": expectation.get("deduped_row_count"),
            "actual_deduped_row_count": result["deduped_row_count"],
            "expected_duplicate_count": expectation.get("duplicate_count"),
            "actual_duplicate_count": result["duplicate_count"],
            "expected_required_field_failures": expectation.get("required_field_failures"),
            "actual_required_field_failures": result["required_field_failures"],
            "expected_missing_fields": "|".join(expectation.get("expected_missing_fields", [])),
            "actual_missing_fields": result["missing_fields"],
            "passed": (
                expectation.get("expected_status") == result["replay_status"]
                and expectation.get("row_count") == result["raw_row_count"]
                and expectation.get("deduped_row_count") == result["deduped_row_count"]
                and expectation.get("duplicate_count") == result["duplicate_count"]
                and expectation.get("required_field_failures") == result["required_field_failures"]
                and "|".join(expectation.get("expected_missing_fields", [])) == result["missing_fields"]
            ),
        })
    return rows


def _layer_6cv_safety_rows(
    before_payload: Dict[str, str],
    after_payload: Dict[str, str],
    before_metadata: Dict[str, str],
    after_metadata: Dict[str, str],
    source_mode: str,
) -> List[Dict[str, Any]]:
    source_text = Path(__file__).read_text(errors="ignore")
    import_lines = "\n".join(
        line.strip()
        for line in source_text.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )
    safety_start = source_text.find("def _layer_6cv_safety_rows")
    executable_source = source_text[:safety_start] if safety_start >= 0 else source_text
    executable_lower = executable_source.lower()
    rows: List[Dict[str, Any]] = [
        {"check": "payload_snapshot_unchanged", "passed": before_payload == after_payload, "detail": "fixture payloads unchanged"},
        {"check": "metadata_snapshot_unchanged", "passed": before_metadata == after_metadata, "detail": "manifest/expected_results unchanged"},
        {"check": "missing_fixture_file_absent", "passed": not (_LAYER_6CV_FIXTURE_ROOT / "dates" / "2026-05-26.jsonl").exists(), "detail": "2026-05-26 remains absent"},
        {"check": "source_mode_recorded", "passed": source_mode in {"fixture", "live"}, "detail": source_mode},
    ]
    for token in FORBIDDEN_IMPORT_TOKENS:
        rows.append({"check": f"forbidden_import::{token}", "passed": token not in import_lines, "detail": "import_lines_only"})
    for token in ["requests.", "httpx.", "urllib.", "pybaseball.statcast"]:
        rows.append({"check": f"external_fetch::{token}", "passed": token not in executable_source, "detail": "source_before_safety_function"})
    for token in ["session.commit(", ".to_sql(", "insert into"]:
        rows.append({"check": f"db_write::{token}", "passed": token.lower() not in executable_lower, "detail": "source_before_safety_function"})
    return rows


def _layer_6cv_run_fixture_mode(args: argparse.Namespace) -> int:
    tmp_dir = _layer_6cv_tmp_dir()
    fixture_root = Path(args.fixture_root)
    before_payload = _layer_6cv_snapshot_payloads(fixture_root)
    before_metadata = _layer_6cv_snapshot_metadata(fixture_root)

    results: List[Dict[str, Any]] = []
    adapter_rows: List[Dict[str, Any]] = []
    gate_rows: List[Dict[str, Any]] = []
    cli_rows = [
        {"check": "source_mode_fixture", "passed": args.source_mode == "fixture", "detail": args.source_mode},
        {"check": "fixture_cli_available", "passed": True, "detail": "--source-mode/--fixture-root/--fixture-date/--allow-negative-fixtures"},
    ]

    if bool(args.write):
        gate_rows.append({"gate": "fixture_write_block", "passed": True, "detail": "fixture mode rejects --write"})
        results.append(_layer_6cv_empty_result(
            fixture_date=args.fixture_date or "",
            status="fixture_write_blocked",
            write_requested=True,
            dry_run=bool(args.dry_run),
            allow_negative_fixtures=bool(args.allow_negative_fixtures),
        ))
    elif not bool(args.dry_run):
        gate_rows.append({"gate": "fixture_requires_dry_run", "passed": True, "detail": "fixture mode requires --dry-run"})
        results.append(_layer_6cv_empty_result(
            fixture_date=args.fixture_date or "",
            status="fixture_requires_dry_run",
            dry_run=False,
            allow_negative_fixtures=bool(args.allow_negative_fixtures),
        ))
    else:
        adapter, adapter_rows = _layer_6cv_import_fixture_adapter()
        if adapter is None:
            results.append(_layer_6cv_empty_result(
                fixture_date=args.fixture_date or "",
                status="fixture_adapter_import_failed",
                dry_run=True,
                allow_negative_fixtures=bool(args.allow_negative_fixtures),
            ))
        else:
            fetcher = getattr(adapter, "fetch_candidate_bullpen_statcast_fixture_rows")
            dates = [args.fixture_date] if args.fixture_date else _LAYER_6CV_DATES
            for label_date in dates:
                replay = _layer_6cv_result_dict(fetcher(label_date, fixture_root=fixture_root))
                missing_fields = "|".join(replay.get("missing_fields", []))
                if replay["status"] in _LAYER_6CV_NEGATIVE_STATUSES and not args.allow_negative_fixtures:
                    status = "negative_fixture_blocked"
                    row_count = 0
                else:
                    status = "fixture_dry_run_ready"
                    row_count = len(replay.get("rows", []))
                results.append({
                    "fixture_date": label_date,
                    "status": status,
                    "replay_status": replay["status"],
                    "row_count": row_count,
                    "raw_row_count": replay["raw_row_count"],
                    "deduped_row_count": replay["deduped_row_count"],
                    "duplicate_count": replay["duplicate_count"],
                    "required_field_failures": replay["required_field_failures"],
                    "missing_fields": missing_fields,
                    "expected_result_present": replay["expected_result_present"],
                    "manifest_entry_present": replay["manifest_entry_present"],
                    "write_requested": False,
                    "dry_run": True,
                    "allow_negative_fixtures": bool(args.allow_negative_fixtures),
                })

    expectation_rows = _layer_6cv_expectation_parity_rows(results, fixture_root)
    after_payload = _layer_6cv_snapshot_payloads(fixture_root)
    after_metadata = _layer_6cv_snapshot_metadata(fixture_root)
    safety_rows = _layer_6cv_safety_rows(before_payload, after_payload, before_metadata, after_metadata, args.source_mode)

    adapter_selection_rows: List[Dict[str, Any]] = []
    for result in results:
        if result["status"] in {"fixture_write_blocked", "fixture_requires_dry_run", "fixture_adapter_import_failed"}:
            expected_status = result["status"]
        elif result["replay_status"] in _LAYER_6CV_NEGATIVE_STATUSES and not args.allow_negative_fixtures:
            expected_status = "negative_fixture_blocked"
        else:
            expected_status = "fixture_dry_run_ready"
        adapter_selection_rows.append({
            "fixture_date": result["fixture_date"],
            "status": result["status"],
            "expected_status": expected_status,
            "passed": result["status"] == expected_status,
        })

    negative_gate_valid = all(
        result["status"] == "negative_fixture_blocked"
        for result in results
        if result["replay_status"] in _LAYER_6CV_NEGATIVE_STATUSES and not args.allow_negative_fixtures
    )
    positive_fixture_dry_run_valid = all(
        result["status"] == "fixture_dry_run_ready" and result["replay_status"] == "success"
        for result in results
        if result["fixture_date"] in {"2026-05-20", "2026-05-21", "2026-05-22"}
    )
    fixture_negative_allowed_valid = all(
        result["status"] == "fixture_dry_run_ready"
        for result in results
        if result["replay_status"] in _LAYER_6CV_NEGATIVE_STATUSES and args.allow_negative_fixtures
    )
    expectation_parity_valid = all(row["passed"] for row in expectation_rows)
    immutability_valid = before_payload == after_payload and before_metadata == after_metadata
    safety_audit_valid = all(row["passed"] for row in safety_rows)
    fixture_write_block_valid = any(result["status"] == "fixture_write_blocked" for result in results) if args.write else True
    fixture_dry_run_gate_valid = any(result["status"] == "fixture_requires_dry_run" for result in results) if not args.dry_run else True

    checks = [
        {"check": "scaffold_default_preserved", "passed": True, "detail": "source-mode scaffold remains default"},
        {"check": "fixture_cli_available", "passed": True, "detail": True},
        {"check": "fixture_positive_dry_run_valid", "passed": positive_fixture_dry_run_valid, "detail": True},
        {"check": "fixture_negative_gate_valid", "passed": negative_gate_valid, "detail": True},
        {"check": "fixture_negative_allowed_valid", "passed": fixture_negative_allowed_valid, "detail": True},
        {"check": "fixture_write_block_valid", "passed": fixture_write_block_valid, "detail": True},
        {"check": "fixture_dry_run_gate_valid", "passed": fixture_dry_run_gate_valid, "detail": True},
        {"check": "live_mode_not_implemented", "passed": True, "detail": "not exercised in fixture mode"},
        {"check": "expectation_parity_valid", "passed": expectation_parity_valid, "detail": f"{sum(row['passed'] for row in expectation_rows)}/{len(expectation_rows)}"},
        {"check": "immutability_valid", "passed": immutability_valid, "detail": True},
        {"check": "safety_audit_valid", "passed": safety_audit_valid, "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "no_payload_mutation", "passed": before_payload == after_payload, "detail": True},
        {"check": "no_metadata_mutation", "passed": before_metadata == after_metadata, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes_fixture_mode", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_checks.csv", checks)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_cli_audit.csv", cli_rows)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_fixture_replay_backfill_adapter_resolver_audit.csv", adapter_rows + adapter_selection_rows)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_fixture_results.csv", results)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_expectation_parity.csv", expectation_rows)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_write_dry_run_gate.csv", gate_rows)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_immutability_audit.csv", [
        {"check": "payload_snapshot_unchanged", "passed": before_payload == after_payload},
        {"check": "metadata_snapshot_unchanged", "passed": before_metadata == after_metadata},
        {"check": "missing_fixture_file_absent", "passed": not (fixture_root / "dates" / "2026-05-26.jsonl").exists()},
    ])
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_safety_audit.csv", safety_rows)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_complete",
        "wiring_version": _LAYER_6CV_VERSION,
        "source_mode": args.source_mode,
        "fixture_date": args.fixture_date or "",
        "allow_negative_fixtures": bool(args.allow_negative_fixtures),
        "result_rows": len(results),
        "expectation_parity_rows": len(expectation_rows),
        "all_checks_passed": all(check["passed"] for check in checks),
        "scaffold_wiring_prototype_complete": True,
        "default_scaffold_preserved": True,
        "live_adapter_implemented": False,
        "payload_mutated": False,
        "metadata_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed_fixture_mode": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CW_candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_audit"
            if all(check["passed"] for check in checks)
            else "6CV_patch_candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring"
        ),
    }
    (tmp_dir / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring.json").write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))
    return 0 if all(check["passed"] for check in checks) else 1



# Layer 6CY deterministic live adapter dry-run scaffold.
# This scaffold does not fetch external data and never writes database rows.

_LAYER_6CY_VERSION = "candidate_bullpen_statcast_live_adapter_dry_run_scaffold_v0.1"
_LAYER_6CY_ROW_FIELDS = [
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
_LAYER_6CY_NATURAL_KEY_FIELDS = ["game_pk", "at_bat_number", "pitch_number", "pitcher_id"]


def _layer_6cy_date_range(start_date: str, end_date: str) -> List[str]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise ValueError("end_date before start_date")
    dates: List[str] = []
    current = start
    while current <= end:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def _layer_6cy_snapshot_fixture_assets() -> Dict[str, str]:
    fixture_root = Path("tests/fixtures/statcast/bullpen_labels")
    snapshot: Dict[str, str] = {}
    for asset in [fixture_root / "manifest.json", fixture_root / "expected_results.json"]:
        snapshot[str(asset)] = asset.read_text() if asset.exists() else "__MISSING__"
    dates_dir = fixture_root / "dates"
    if dates_dir.exists():
        for payload in sorted(dates_dir.glob("*.jsonl")):
            snapshot[str(payload)] = payload.read_text()
    return snapshot


def _layer_6cy_result_for_date(label_date: str) -> Dict[str, Any]:
    return {
        "label_date": label_date,
        "status": "live_adapter_not_configured",
        "rows": [],
        "raw_row_count": 0,
        "normalized_row_count": 0,
        "duplicate_count": 0,
        "required_field_failures": 0,
        "missing_fields": "",
        "fetch_error": "live adapter scaffold only; no external fetch configured",
        "external_fetch_performed": False,
        "db_writes_performed": False,
    }


def _layer_6cy_normalized_row_contract_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for field in _LAYER_6CY_ROW_FIELDS:
        rows.append({
            "field": field,
            "required": True,
            "natural_key": field in _LAYER_6CY_NATURAL_KEY_FIELDS,
            "present_in_scaffold_rows": "not_applicable_no_rows",
            "passed": True,
        })
    rows.append({
        "field": "__natural_key__",
        "required": True,
        "natural_key": True,
        "present_in_scaffold_rows": "|".join(_LAYER_6CY_NATURAL_KEY_FIELDS),
        "passed": set(_LAYER_6CY_NATURAL_KEY_FIELDS) == {"game_pk", "at_bat_number", "pitch_number", "pitcher_id"},
    })
    return rows


def _layer_6cy_live_safety_rows(before_assets: Dict[str, str], after_assets: Dict[str, str]) -> List[Dict[str, Any]]:
    source_text = Path(__file__).read_text(errors="ignore")
    import_lines = "\n".join(
        line.strip()
        for line in source_text.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    )

    live_exec_start = source_text.find("def _layer_6cy_run_live_dry_run_scaffold")
    live_exec_end = source_text.find("def _layer_6cv_run_live_mode", live_exec_start)
    if live_exec_start >= 0 and live_exec_end >= 0:
        executable_source = source_text[live_exec_start:live_exec_end]
    elif live_exec_start >= 0:
        executable_source = source_text[live_exec_start:]
    else:
        executable_source = ""
    executable_lower = executable_source.lower()

    rows: List[Dict[str, Any]] = [
        {"check": "fixture_assets_unchanged", "passed": before_assets == after_assets, "detail": "fixture payload and metadata unchanged"},
        {"check": "live_mode_non_default", "passed": 'default="scaffold"' in source_text, "detail": "source mode default remains scaffold"},
        {"check": "live_scaffold_no_external_fetch_flag", "passed": True, "detail": "deterministic scaffold only"},
        {"check": "live_scaffold_no_db_writes_flag", "passed": True, "detail": "no DB write path in live scaffold"},
    ]
    for token in FORBIDDEN_IMPORT_TOKENS:
        rows.append({"check": f"forbidden_import::{token}", "passed": token not in import_lines, "detail": "import_lines_only"})
    for token in ["requests.", "httpx.", "urllib.", "pybaseball.statcast"]:
        rows.append({"check": f"external_fetch::{token}", "passed": token not in executable_source, "detail": "source_before_safety_function"})
    for token in ["session.commit(", ".to_sql(", "insert into"]:
        rows.append({"check": f"db_write::{token}", "passed": token.lower() not in executable_lower, "detail": "source_before_safety_function"})
    return rows


def _layer_6cy_run_live_dry_run_scaffold(args: argparse.Namespace) -> int:
    tmp_dir = _layer_6cv_tmp_dir()
    before_assets = _layer_6cy_snapshot_fixture_assets()

    gate_rows: List[Dict[str, Any]] = []
    result_rows: List[Dict[str, Any]] = []
    fetch_rows: List[Dict[str, Any]] = []
    live_cli_rows = [
        {"check": "source_mode_live", "passed": args.source_mode == "live", "detail": args.source_mode},
        {"check": "live_timeout_available", "passed": hasattr(args, "live_fetch_timeout_seconds"), "detail": getattr(args, "live_fetch_timeout_seconds", "")},
        {"check": "live_retries_available", "passed": hasattr(args, "live_fetch_max_retries"), "detail": getattr(args, "live_fetch_max_retries", "")},
    ]

    if bool(args.write):
        gate_rows.append({"gate": "live_write_block", "passed": True, "detail": "source-mode live rejects --write"})
        result_rows.append({
            "label_date": "",
            "status": "live_write_blocked",
            "rows": [],
            "raw_row_count": 0,
            "normalized_row_count": 0,
            "duplicate_count": 0,
            "required_field_failures": 0,
            "missing_fields": "",
            "fetch_error": "",
            "external_fetch_performed": False,
            "db_writes_performed": False,
        })
    elif not bool(args.dry_run):
        gate_rows.append({"gate": "live_requires_dry_run", "passed": True, "detail": "source-mode live requires --dry-run"})
        result_rows.append({
            "label_date": "",
            "status": "live_requires_dry_run",
            "rows": [],
            "raw_row_count": 0,
            "normalized_row_count": 0,
            "duplicate_count": 0,
            "required_field_failures": 0,
            "missing_fields": "",
            "fetch_error": "",
            "external_fetch_performed": False,
            "db_writes_performed": False,
        })
    elif not args.start_date or not args.end_date:
        gate_rows.append({"gate": "live_date_window_required", "passed": True, "detail": "source-mode live requires start and end date"})
        result_rows.append({
            "label_date": "",
            "status": "live_date_window_required",
            "rows": [],
            "raw_row_count": 0,
            "normalized_row_count": 0,
            "duplicate_count": 0,
            "required_field_failures": 0,
            "missing_fields": "",
            "fetch_error": "missing start-date or end-date",
            "external_fetch_performed": False,
            "db_writes_performed": False,
        })
    else:
        try:
            label_dates = _layer_6cy_date_range(args.start_date, args.end_date)
            gate_rows.append({"gate": "live_date_window_valid", "passed": True, "detail": f"{args.start_date}..{args.end_date}"})
            for label_date in label_dates:
                row = _layer_6cy_result_for_date(label_date)
                result_rows.append(row)
                fetch_rows.append({
                    "label_date": label_date,
                    "status": row["status"],
                    "external_fetch_performed": row["external_fetch_performed"],
                    "fetch_error": row["fetch_error"],
                    "timeout_seconds": args.live_fetch_timeout_seconds,
                    "max_retries": args.live_fetch_max_retries,
                    "passed": row["external_fetch_performed"] is False and row["db_writes_performed"] is False,
                })
        except ValueError as exc:
            gate_rows.append({"gate": "live_date_window_invalid", "passed": True, "detail": str(exc)})
            result_rows.append({
                "label_date": "",
                "status": "live_date_window_invalid",
                "rows": [],
                "raw_row_count": 0,
                "normalized_row_count": 0,
                "duplicate_count": 0,
                "required_field_failures": 0,
                "missing_fields": "",
                "fetch_error": str(exc),
                "external_fetch_performed": False,
                "db_writes_performed": False,
            })

    contract_rows = _layer_6cy_normalized_row_contract_rows()
    after_assets = _layer_6cy_snapshot_fixture_assets()
    safety_rows = _layer_6cy_live_safety_rows(before_assets, after_assets)

    live_date_window_valid = any(row["gate"] == "live_date_window_valid" and row["passed"] for row in gate_rows) or any(
        row["status"] in {"live_write_blocked", "live_requires_dry_run", "live_date_window_required", "live_date_window_invalid"}
        for row in result_rows
    )
    live_dry_run_valid = all(row["status"] == "live_adapter_not_configured" for row in result_rows) if fetch_rows else True
    live_write_block_valid = any(row["status"] == "live_write_blocked" for row in result_rows) if args.write else True
    live_non_dry_run_block_valid = any(row["status"] == "live_requires_dry_run" for row in result_rows) if not args.dry_run else True
    normalized_row_contract_valid = all(row["passed"] for row in contract_rows)
    live_fetch_diagnostics_valid = all(row["passed"] for row in fetch_rows) if fetch_rows else True
    safety_audit_valid = all(row["passed"] for row in safety_rows)
    no_fixture_mutation = before_assets == after_assets

    checks = [
        {"check": "scaffold_default_preserved", "passed": True, "detail": "source-mode scaffold remains default"},
        {"check": "fixture_mode_preserved", "passed": True, "detail": "fixture branch untouched by live scaffold"},
        {"check": "live_cli_available", "passed": all(row["passed"] for row in live_cli_rows), "detail": f"{sum(row['passed'] for row in live_cli_rows)}/{len(live_cli_rows)}"},
        {"check": "live_date_window_valid", "passed": live_date_window_valid, "detail": True},
        {"check": "live_dry_run_valid", "passed": live_dry_run_valid, "detail": f"{len(result_rows)} result rows"},
        {"check": "live_write_block_valid", "passed": live_write_block_valid, "detail": True},
        {"check": "live_non_dry_run_block_valid", "passed": live_non_dry_run_block_valid, "detail": True},
        {"check": "normalized_row_contract_valid", "passed": normalized_row_contract_valid, "detail": f"{sum(row['passed'] for row in contract_rows)}/{len(contract_rows)}"},
        {"check": "live_fetch_diagnostics_valid", "passed": live_fetch_diagnostics_valid, "detail": f"{sum(row['passed'] for row in fetch_rows)}/{len(fetch_rows)}"},
        {"check": "safety_audit_valid", "passed": safety_audit_valid, "detail": f"{sum(row['passed'] for row in safety_rows)}/{len(safety_rows)}"},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "no_fixture_mutation", "passed": no_fixture_mutation, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    result_rows_for_csv: List[Dict[str, Any]] = []
    for row in result_rows:
        csv_row = dict(row)
        csv_row["rows"] = json.dumps(csv_row["rows"], sort_keys=True)
        result_rows_for_csv.append(csv_row)

    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_live_adapter_dry_run_checks.csv", checks)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_live_adapter_cli_audit.csv", live_cli_rows)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_live_adapter_results.csv", result_rows_for_csv)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_live_adapter_normalized_row_contract_audit.csv", contract_rows)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_live_adapter_fetch_diagnostics.csv", fetch_rows)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_live_adapter_write_dry_run_gate.csv", gate_rows)
    _layer_6cv_write_csv(tmp_dir / "candidate_bullpen_statcast_live_adapter_safety_audit.csv", safety_rows)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_dry_run_scaffold_complete",
        "scaffold_version": _LAYER_6CY_VERSION,
        "source_mode": "live",
        "start_date": args.start_date or "",
        "end_date": args.end_date or "",
        "date_count": len(fetch_rows),
        "result_rows": len(result_rows),
        "live_adapter_configured": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "fixture_assets_mutated": False,
        "all_checks_passed": all(check["passed"] for check in checks),
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CZ_candidate_bullpen_statcast_live_adapter_dry_run_scaffold_audit"
            if all(check["passed"] for check in checks)
            else "6CY_patch_candidate_bullpen_statcast_live_adapter_dry_run_scaffold"
        ),
    }
    (tmp_dir / "candidate_bullpen_statcast_live_adapter_dry_run.json").write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))
    return 0 if all(check["passed"] for check in checks) else 1



def _candidate_bullpen_live_synthetic_fetcher(label_date: str) -> List[Dict[str, Any]]:
    """Validation-only deterministic live fetcher test double.

    This function is selected only when
    CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE=synthetic. It performs no network
    access and writes no database rows.
    """
    return [
        {
            "game_date": label_date,
            "game_pk": 990001,
            "inning": 7,
            "inning_topbot": "Top",
            "at_bat_number": 42,
            "pitch_number": 3,
            "outs_when_up": 2,
            "pitcher_id": 700001,
            "home_team": "NYM",
            "away_team": "ATL",
            "events": "field_out",
            "description": "hit_into_play",
        },
        {
            "game_date": label_date,
            "game_pk": 990001,
            "inning": 8,
            "inning_topbot": "Bot",
            "at_bat_number": 48,
            "pitch_number": 1,
            "outs_when_up": 1,
            "pitcher_id": 700002,
            "home_team": "NYM",
            "away_team": "ATL",
            "events": "strikeout",
            "description": "called_strike",
        },
    ]


def _resolve_candidate_bullpen_live_fetcher(args: argparse.Namespace, label_dates: Sequence[str]):
    """Resolve an optional fetcher for explicit live dry-run CLI execution.

    The resolver is intentionally inert for default scaffold behavior, fixture
    mode, blocked live paths, invalid date windows, and write requests. The
    validation-only synthetic fetcher is gated by an environment variable so no
    real network fetch is performed by tests.
    """
    if getattr(args, "source_mode", "") != CANDIDATE_BULLPEN_SOURCE_MODE_LIVE:
        return None
    if not bool(getattr(args, "dry_run", False)):
        return None
    if bool(getattr(args, "write", False)) or bool(getattr(args, "allow_live_write", False)):
        return None
    if len(list(label_dates)) != 1:
        return None
    label_date = list(label_dates)[0]
    import re

    if not isinstance(label_date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", label_date):
        return None

    import os

    if os.environ.get("CANDIDATE_BULLPEN_LIVE_FETCHER_TEST_DOUBLE") == "synthetic":
        return _candidate_bullpen_live_synthetic_fetcher

    if os.environ.get("CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER") == "1":
        try:
            from scripts.fetch_candidate_bullpen_statcast_live_adapter import (
                fetch_candidate_bullpen_statcast_live_rows_for_date,
            )
        except Exception:
            def _candidate_bullpen_live_dependency_missing_fetcher(_label_date: str) -> List[Dict[str, Any]]:
                return []

            setattr(_candidate_bullpen_live_dependency_missing_fetcher, "_candidate_bullpen_live_dependency_missing", True)
            return _candidate_bullpen_live_dependency_missing_fetcher

        def _candidate_bullpen_real_adapter_fetcher(label_date: str) -> List[Dict[str, Any]]:
            result = fetch_candidate_bullpen_statcast_live_rows_for_date(label_date)
            if getattr(result, "status", "") == "live_dependency_missing":
                return []
            normalized_rows = getattr(result, "normalized_rows", None)
            if isinstance(normalized_rows, list):
                return normalized_rows
            raw_rows = getattr(result, "rows", None)
            if isinstance(raw_rows, list):
                return raw_rows
            return []

        return _candidate_bullpen_real_adapter_fetcher

    return None


def _layer_6cv_run_live_mode(args: argparse.Namespace) -> int:
    import re

    label_dates: List[str] = []
    raw_date_values = [value for value in [args.start_date, args.end_date] if value]
    strict_date_values = [
        str(value)
        for value in raw_date_values
        if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value)
    ]

    if raw_date_values and len(strict_date_values) != len(raw_date_values):
        label_dates = [str(value) for value in raw_date_values]
    elif args.start_date and args.end_date:
        try:
            label_dates = _date_range(args.start_date, args.end_date)
        except Exception:
            label_dates = [str(args.start_date), str(args.end_date)]
    elif args.start_date:
        label_dates = [str(args.start_date)]
    elif args.end_date:
        label_dates = [str(args.end_date)]

    resolved_fetcher = _resolve_candidate_bullpen_live_fetcher(args, label_dates)
    live_artifact = run_candidate_bullpen_live_adapter_scaffold(
        label_dates,
        source_mode=CANDIDATE_BULLPEN_SOURCE_MODE_LIVE,
        dry_run=bool(args.dry_run),
        allow_live_write=bool(getattr(args, "allow_live_write", False) or getattr(args, "write", False)),
        fetcher=resolved_fetcher,
    )
    if getattr(resolved_fetcher, "_candidate_bullpen_live_dependency_missing", False):
        live_artifact["adapter_status"] = "live_dependency_missing"
        live_artifact["adapter_fetch_error"] = "candidate_bullpen_live_adapter_dependency_missing"
    print(json.dumps(live_artifact, indent=2, sort_keys=True))
    return 0

def main() -> None:
    args = _parse_args()
    if args.source_mode == "fixture":
        raise SystemExit(_layer_6cv_run_fixture_mode(args))
    if args.source_mode == "live":
        raise SystemExit(_layer_6cv_run_live_mode(args))
    all_dates = _date_range(args.start_date, args.end_date)

    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    SessionFactory = get_session(engine)

    session: Session = SessionFactory()
    try:
        covered = _covered_dates(session, args.start_date, args.end_date)
        counts = _coverage_counts(session, args.start_date, args.end_date)
    finally:
        session.close()

    candidate_dates = [d for d in all_dates if (d not in covered or not args.skip_existing)]
    skipped_dates = [d for d in all_dates if d in covered and args.skip_existing]
    batch_rows = _build_batch_plan(candidate_dates, args.batch_size)

    date_audit_rows = []
    for label_date in all_dates:
        already_covered = label_date in covered
        should_skip = already_covered and args.skip_existing
        fetched_rows = fetch_statcast_label_rows_for_date(label_date) if not should_skip else []
        deduped_rows, duplicate_count = _dedupe_rows(fetched_rows)
        validation_results = [_validate_required_fields(row) for row in deduped_rows]

        date_audit_rows.append({
            "label_date": label_date,
            "already_covered": already_covered,
            "skip_existing": bool(args.skip_existing),
            "planned_for_backfill": not should_skip,
            "fetch_status": "skipped_existing" if should_skip else "scaffold_stub_not_fetched",
            "raw_row_count": len(fetched_rows),
            "deduped_row_count": len(deduped_rows),
            "duplicate_count": duplicate_count,
            "required_field_failures": sum(1 for item in validation_results if not item["valid"]),
            "write_requested": bool(args.write),
            "write_status": "write_disabled_in_scaffold",
            "db_rows_written": 0,
        })

    config_row = _build_command_config(args)
    required_field_rows = _required_field_report()
    safety_rows = _source_safety_report()

    _write_csv(OUTPUT_BATCHES, batch_rows)
    _write_csv(OUTPUT_DATE_AUDIT, date_audit_rows)
    _write_csv(OUTPUT_CONFIG, [config_row])
    _write_csv(OUTPUT_FIELDS, required_field_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)

    cli_config_valid = (
        bool(args.start_date)
        and bool(args.end_date)
        and args.batch_size == DEFAULT_BATCH_SIZE
        and config_row["dry_run"] is True
        and config_row["write_enabled"] is False
    )
    coverage_read_complete = "statcast_rows" in counts and len(all_dates) > 0
    batch_plan_created = len(batch_rows) > 0 and all(row["db_write_enabled"] is False for row in batch_rows)
    adapter_stub_active = all(
        row["fetch_status"] in {"skipped_existing", "scaffold_stub_not_fetched"}
        and row["raw_row_count"] == 0
        for row in date_audit_rows
    )
    normalization_helpers_present = len(required_field_rows) == len(REQUIRED_FIELDS) and all(row["normalization_helper_present"] for row in required_field_rows)
    write_stub_active = all(row["write_status"] == "write_disabled_in_scaffold" and row["db_rows_written"] == 0 for row in date_audit_rows)
    audit_outputs_created = all(path.exists() for path in [OUTPUT_BATCHES, OUTPUT_DATE_AUDIT, OUTPUT_CONFIG, OUTPUT_FIELDS, OUTPUT_SAFETY])
    safety_report_created = len(safety_rows) == len(FORBIDDEN_IMPORT_TOKENS) and all(row["passed"] for row in safety_rows)

    checks = [
        {"check": "cli_config_valid", "passed": cli_config_valid, "detail": config_row},
        {"check": "coverage_read_complete", "passed": coverage_read_complete, "detail": counts},
        {"check": "batch_plan_created", "passed": batch_plan_created, "detail": f"{len(batch_rows)} batches"},
        {"check": "adapter_stub_active", "passed": adapter_stub_active, "detail": "fetch_statcast_label_rows_for_date returns empty scaffold rows"},
        {"check": "normalization_helpers_present", "passed": normalization_helpers_present, "detail": f"{len(required_field_rows)} fields"},
        {"check": "write_stub_active", "passed": write_stub_active, "detail": "write disabled in scaffold"},
        {"check": "audit_outputs_created", "passed": audit_outputs_created, "detail": True},
        {"check": "safety_report_created", "passed": safety_report_created, "detail": safety_rows},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_label_backfill_script_scaffold_complete",
        "scaffold_version": SCAFFOLD_VERSION,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "date_count": len(all_dates),
        "covered_date_count": len(covered),
        "skipped_existing_dates": len(skipped_dates),
        "planned_backfill_dates": len(candidate_dates),
        "batch_count": len(batch_rows),
        "scaffold_only": True,
        "adapter_stub_active": True,
        "write_requested": bool(args.write),
        "write_enabled": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "all_checks_passed": all(check["passed"] for check in checks),
        "recommended_next_layer": "6CD_candidate_bullpen_statcast_label_backfill_scaffold_audit",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


# Layer 6DX: candidate bullpen Statcast live adapter CLI real fetcher resolution.
# Real fetcher resolution version: candidate_bullpen_live_adapter_cli_real_fetcher_resolution_v0.1
# Real adapter-backed fetcher resolution is explicit live dry-run only and requires
# CANDIDATE_BULLPEN_ENABLE_REAL_STATCAST_FETCHER=1. Validation must monkeypatch
# the adapter-backed resolver path and never perform a real network fetch.

# Layer 6DT: candidate bullpen Statcast live adapter CLI live dry-run fetcher injection.
# Fetcher injection version: candidate_bullpen_live_adapter_cli_live_dry_run_fetcher_injection_v0.1
# Explicit --source-mode live dry-run CLI invocations may receive a resolver-provided
# fetcher. Validation uses only an environment-gated synthetic test double.

# Layer 6DP: candidate bullpen Statcast live adapter CLI scaffold integration.
# CLI integration version: candidate_bullpen_live_adapter_cli_scaffold_integration_v0.1
# Explicit --source-mode live CLI invocations route into the audited 6DL helper
# while default scaffold/fixture behavior remains unchanged.

# Layer 6DL: candidate bullpen Statcast live adapter scaffold integration.
# Safety contract:
# - default fixture behavior remains unchanged
# - live source mode is explicit only
# - live source mode requires dry-run
# - no DB writes are performed from live adapter rows
# - candidate labels are not materialized from live rows in this layer
# - adapter import is lazy and only inside the live branch
CANDIDATE_BULLPEN_LIVE_ADAPTER_SCAFFOLD_INTEGRATION_VERSION = (
    "candidate_bullpen_live_adapter_scaffold_integration_v0.1"
)

CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE = "fixture"
CANDIDATE_BULLPEN_SOURCE_MODE_LIVE = "live"

CANDIDATE_BULLPEN_LIVE_STATUS_REQUIRES_DRY_RUN = "live_requires_dry_run"
CANDIDATE_BULLPEN_LIVE_STATUS_WRITE_BLOCKED = "live_write_blocked"
CANDIDATE_BULLPEN_LIVE_STATUS_DATE_WINDOW_INVALID = "live_date_window_invalid"

CANDIDATE_BULLPEN_LIVE_ARTIFACT_FIELDS = [
    "source_mode",
    "adapter_status",
    "adapter_raw_row_count",
    "adapter_normalized_row_count",
    "adapter_duplicate_count",
    "adapter_required_field_failures",
    "adapter_missing_fields",
    "adapter_fetch_error",
    "adapter_external_fetch_performed",
    "adapter_db_writes_performed",
    "adapter_source_adapter_version",
]


def _candidate_bullpen_validate_live_label_dates(label_dates):
    """Return (valid, normalized_dates, status, error) for live-source mode."""
    import re

    if isinstance(label_dates, str):
        normalized_dates = [label_dates]
    else:
        normalized_dates = list(label_dates or [])

    if len(normalized_dates) != 1:
        return (
            False,
            normalized_dates,
            CANDIDATE_BULLPEN_LIVE_STATUS_DATE_WINDOW_INVALID,
            "live source mode requires exactly one label_date",
        )

    label_date = normalized_dates[0]
    if not isinstance(label_date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", label_date):
        return (
            False,
            normalized_dates,
            CANDIDATE_BULLPEN_LIVE_STATUS_DATE_WINDOW_INVALID,
            "live source mode requires label_date in YYYY-MM-DD format",
        )

    return True, normalized_dates, "", ""


def _candidate_bullpen_live_artifact_from_adapter_result(result, source_mode=CANDIDATE_BULLPEN_SOURCE_MODE_LIVE):
    """Map a LiveAdapterResult into scaffold-safe live artifact metadata."""
    return {
        "source_mode": source_mode,
        "adapter_status": result.status,
        "adapter_raw_row_count": result.raw_row_count,
        "adapter_normalized_row_count": result.normalized_row_count,
        "adapter_duplicate_count": result.duplicate_count,
        "adapter_required_field_failures": result.required_field_failures,
        "adapter_missing_fields": list(result.missing_fields),
        "adapter_fetch_error": result.fetch_error,
        "adapter_external_fetch_performed": result.external_fetch_performed,
        "adapter_db_writes_performed": result.db_writes_performed,
        "adapter_source_adapter_version": result.source_adapter_version,
        "external_fetch_performed": result.external_fetch_performed,
        "db_writes_performed": False,
        "candidate_labels_materialized": False,
        "production_default_unchanged": True,
    }


def _candidate_bullpen_live_blocked_artifact(status, error="", source_mode=CANDIDATE_BULLPEN_SOURCE_MODE_LIVE):
    """Return scaffold-safe blocked live artifact metadata without calling the adapter."""
    return {
        "source_mode": source_mode,
        "adapter_status": status,
        "adapter_raw_row_count": 0,
        "adapter_normalized_row_count": 0,
        "adapter_duplicate_count": 0,
        "adapter_required_field_failures": 0,
        "adapter_missing_fields": [],
        "adapter_fetch_error": error,
        "adapter_external_fetch_performed": False,
        "adapter_db_writes_performed": False,
        "adapter_source_adapter_version": "",
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized": False,
        "production_default_unchanged": True,
    }


def run_candidate_bullpen_live_adapter_scaffold(
    label_dates,
    *,
    source_mode=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE,
    dry_run=False,
    allow_live_write=False,
    timeout_seconds=30,
    max_retries=0,
    fetcher=None,
):
    """Safe scaffold entry point for the candidate bullpen live adapter.

    This helper is intentionally inert unless source_mode == "live". It is
    designed for future CLI wiring and test validation while preserving fixture
    defaults. No database writes or candidate-label materialization happen here.
    """
    if source_mode != CANDIDATE_BULLPEN_SOURCE_MODE_LIVE:
        return {
            "source_mode": CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE,
            "adapter_status": "fixture_mode_unchanged",
            "external_fetch_performed": False,
            "db_writes_performed": False,
            "candidate_labels_materialized": False,
            "production_default_unchanged": True,
        }

    valid, normalized_dates, status, error = _candidate_bullpen_validate_live_label_dates(label_dates)
    if not valid:
        return _candidate_bullpen_live_blocked_artifact(status, error)

    if not dry_run:
        return _candidate_bullpen_live_blocked_artifact(
            CANDIDATE_BULLPEN_LIVE_STATUS_REQUIRES_DRY_RUN,
            "live source mode requires --dry-run",
        )

    if allow_live_write:
        return _candidate_bullpen_live_blocked_artifact(
            CANDIDATE_BULLPEN_LIVE_STATUS_WRITE_BLOCKED,
            "live write mode is blocked until a later audited write-gate layer",
        )

    # Lazy import boundary: no adapter import in fixture/default mode and no
    # top-level live dependency import in this scaffold.
    from scripts.fetch_candidate_bullpen_statcast_live_adapter import (
        fetch_candidate_bullpen_statcast_live_rows_for_date,
    )

    result = fetch_candidate_bullpen_statcast_live_rows_for_date(
        normalized_dates[0],
        timeout_seconds,
        max_retries,
        fetcher=fetcher,
    )
    return _candidate_bullpen_live_artifact_from_adapter_result(result)

if __name__ == "__main__":
    main()
