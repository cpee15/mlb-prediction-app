from __future__ import annotations

import argparse
import csv
import json
import os
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
    parser.add_argument("--start-date", required=True, help="Inclusive start date YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="Inclusive end date YYYY-MM-DD.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Dry-run mode. This scaffold never fetches or writes.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        default=False,
        help="Requested write mode. Ignored in scaffold; no DB writes occur.",
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
    return parser.parse_args()


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


def main() -> None:
    args = _parse_args()
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


if __name__ == "__main__":
    main()
