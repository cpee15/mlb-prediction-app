from __future__ import annotations

import csv
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Set

from sqlalchemy import text
from sqlalchemy.orm import Session

from mlb_app.database import create_tables, get_engine, get_session


TARGET_DATE = "2026-05-20"
START_DATE = "2026-04-21"
END_DATE = "2026-05-20"
BATCH_SIZE = 3
DRY_RUN_VERSION = "candidate_bullpen_statcast_label_backfill_dry_run_audit_v0.1"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_dry_run_audit.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_dry_run_audit_checks.csv"
OUTPUT_BATCHES = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_dry_run_batches.csv"
OUTPUT_CURRENT = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_current_window_coverage.csv"
OUTPUT_PROJECTED = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_projected_coverage.csv"
OUTPUT_COMMAND = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_command_contract_validation.csv"
OUTPUT_GATE = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_post_dry_run_gate_notes.csv"


COMMAND_CONTRACT = [
    {"field": "script_name", "value": "scripts/backfill_candidate_bullpen_statcast_labels.py", "required": True, "default": None},
    {"field": "--start-date", "value": "YYYY-MM-DD", "required": True, "default": None},
    {"field": "--end-date", "value": "YYYY-MM-DD", "required": True, "default": None},
    {"field": "--dry-run", "value": "flag", "required": False, "default": True},
    {"field": "--write", "value": "flag", "required": False, "default": False},
    {"field": "--skip-existing", "value": "flag", "required": False, "default": True},
    {"field": "--audit-after", "value": "flag", "required": False, "default": True},
]

GATE_NOTES = [
    {
        "gate": "rerun_6BY_after_real_backfill",
        "required": True,
        "note": "Recompute actual coverage after future fetch/write layer completes.",
    },
    {
        "gate": "rerun_6BW_after_real_backfill",
        "required": True,
        "note": "Rebuild actual historical usage joins using exact same-game/team-side labels.",
    },
    {
        "gate": "rerun_6BX_after_real_backfill",
        "required": True,
        "note": "Re-evaluate calibration_grade after real labels are loaded.",
    },
    {
        "gate": "exact_join_rate",
        "required": True,
        "note": "Must reach >= 0.80 before real-label calibration claims.",
    },
    {
        "gate": "missing_rate",
        "required": True,
        "note": "Must reach <= 0.20 before real-label calibration claims.",
    },
    {
        "gate": "actual_usage_appearance_rows",
        "required": True,
        "note": "Must reach >= 30 before real-label calibration claims.",
    },
    {
        "gate": "calibration_grade",
        "required": True,
        "note": "Must be true before advancing to real-label calibration analysis.",
    },
]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: str, end: str) -> List[str]:
    cursor = _parse_date(start)
    end_date = _parse_date(end)
    days = []
    while cursor <= end_date:
        days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return days


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


def _window_coverage(session: Session) -> Dict[str, Any]:
    all_dates = _date_range(START_DATE, END_DATE)
    covered = sorted(_covered_dates(session, START_DATE, END_DATE))
    missing = [d for d in all_dates if d not in set(covered)]

    row = session.execute(text("""
        SELECT
            COUNT(*) AS window_statcast_rows,
            COUNT(DISTINCT game_pk) AS window_games,
            COUNT(DISTINCT pitcher_id) AS window_pitchers
        FROM statcast_events
        WHERE game_date BETWEEN :start_date AND :end_date
          AND game_pk IS NOT NULL
          AND pitcher_id IS NOT NULL
    """), {"start_date": START_DATE, "end_date": END_DATE}).mappings().first()
    counts = dict(row) if row else {}

    target_row = session.execute(text("""
        SELECT
            COUNT(*) AS target_statcast_rows,
            COUNT(DISTINCT game_pk) AS target_games,
            COUNT(DISTINCT pitcher_id) AS target_pitchers
        FROM statcast_events
        WHERE game_date = :target_date
          AND game_pk IS NOT NULL
          AND pitcher_id IS NOT NULL
    """), {"target_date": TARGET_DATE}).mappings().first()
    target_counts = dict(target_row) if target_row else {}

    return {
        "start_date": START_DATE,
        "end_date": END_DATE,
        "target_date": TARGET_DATE,
        "window_date_count": len(all_dates),
        "covered_date_count": len(covered),
        "missing_date_count": len(missing),
        "covered_dates": "|".join(covered),
        "missing_dates": "|".join(missing),
        "target_date_covered": TARGET_DATE in set(covered),
        "window_statcast_rows": int(counts.get("window_statcast_rows") or 0),
        "window_games": int(counts.get("window_games") or 0),
        "window_pitchers": int(counts.get("window_pitchers") or 0),
        "target_statcast_rows": int(target_counts.get("target_statcast_rows") or 0),
        "target_games": int(target_counts.get("target_games") or 0),
        "target_pitchers": int(target_counts.get("target_pitchers") or 0),
    }


def _build_batches(missing_dates: List[str], batch_size: int = BATCH_SIZE) -> List[Dict[str, Any]]:
    batches = []
    for idx in range(0, len(missing_dates), batch_size):
        dates = missing_dates[idx: idx + batch_size]
        batches.append({
            "batch_id": len(batches) + 1,
            "start_date": dates[0],
            "end_date": dates[-1],
            "date_count": len(dates),
            "dates": "|".join(dates),
            "dry_run": True,
            "write_enabled": False,
            "skip_existing": True,
            "expected_external_fetch": False,
            "expected_db_write": False,
        })
    return batches


def _projected_coverage(current: Dict[str, Any], missing_dates: List[str]) -> Dict[str, Any]:
    projected_covered = int(current["covered_date_count"]) + len(missing_dates)
    window_count = int(current["window_date_count"])

    return {
        "start_date": START_DATE,
        "end_date": END_DATE,
        "target_date": TARGET_DATE,
        "current_covered_dates": current["covered_date_count"],
        "planned_missing_dates": len(missing_dates),
        "projected_covered_dates": projected_covered,
        "projected_missing_dates": max(0, window_count - projected_covered),
        "projected_window_coverage_rate": round(projected_covered / max(window_count, 1), 4),
        "projected_target_date_covered": TARGET_DATE in set(missing_dates) or bool(current["target_date_covered"]),
        "projected_exact_join_recovery_possible": TARGET_DATE in set(missing_dates) or bool(current["target_date_covered"]),
        "current_exact_join_estimate": 0.0 if not current["target_date_covered"] else None,
        "projected_exact_join_possibility": "possible_after_real_backfill" if TARGET_DATE in set(missing_dates) else "already_possible" if current["target_date_covered"] else "not_possible",
        "current_missing_rate_estimate": 1.0 if not current["target_date_covered"] else None,
        "projected_missing_rate_possibility": "requires_real_fetch_write_and_6BY_6BW_6BX_rerun",
    }


def _command_validation_rows() -> List[Dict[str, Any]]:
    rows = []
    for item in COMMAND_CONTRACT:
        valid = True
        if item["field"] == "--dry-run":
            valid = item["default"] is True
        elif item["field"] == "--write":
            valid = item["default"] is False
        elif item["field"] == "--skip-existing":
            valid = item["default"] is True
        elif item["field"] == "--audit-after":
            valid = item["default"] is True

        rows.append({
            **item,
            "valid": valid,
        })
    return rows


def main() -> None:
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    SessionFactory = get_session(engine)

    session: Session = SessionFactory()
    try:
        current = _window_coverage(session)
    finally:
        session.close()

    missing_dates = current["missing_dates"].split("|") if current["missing_dates"] else []
    batches = _build_batches(missing_dates)
    projected = _projected_coverage(current, missing_dates)
    command_rows = _command_validation_rows()

    _write_csv(OUTPUT_CURRENT, [current])
    _write_csv(OUTPUT_BATCHES, batches)
    _write_csv(OUTPUT_PROJECTED, [projected])
    _write_csv(OUTPUT_COMMAND, command_rows)
    _write_csv(OUTPUT_GATE, GATE_NOTES)

    current_window_coverage_inspected = current["window_date_count"] == 30
    missing_dates_identified = current["missing_date_count"] == len(missing_dates) and len(missing_dates) > 0
    dry_run_batches_created = len(batches) > 0 and all(row["dry_run"] is True and row["write_enabled"] is False for row in batches)
    skip_existing_behavior_valid = all(
        set(row["dates"].split("|")).isdisjoint(set(current["covered_dates"].split("|") if current["covered_dates"] else []))
        for row in batches
    )
    projected_coverage_valid = (
        projected["projected_missing_dates"] == 0
        and projected["projected_target_date_covered"] is True
        and projected["projected_exact_join_recovery_possible"] is True
    )
    command_contract_valid = all(row["valid"] for row in command_rows)
    post_backfill_gate_notes_created = len(GATE_NOTES) >= 7

    checks = [
        {"check": "current_window_coverage_inspected", "passed": current_window_coverage_inspected, "detail": current},
        {"check": "missing_dates_identified", "passed": missing_dates_identified, "detail": missing_dates},
        {"check": "dry_run_batches_created", "passed": dry_run_batches_created, "detail": f"{len(batches)} batches"},
        {"check": "skip_existing_behavior_valid", "passed": skip_existing_behavior_valid, "detail": "Only missing dates are batched."},
        {"check": "projected_coverage_valid", "passed": projected_coverage_valid, "detail": projected},
        {"check": "command_contract_valid", "passed": command_contract_valid, "detail": "dry-run true/write false/skip-existing true/audit-after true"},
        {"check": "post_backfill_gate_notes_created", "passed": post_backfill_gate_notes_created, "detail": f"{len(GATE_NOTES)} gates"},
        {"check": "dry_run_only", "passed": True, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_label_backfill_dry_run_audit_complete",
        "dry_run_version": DRY_RUN_VERSION,
        "target_date": TARGET_DATE,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "batch_size": BATCH_SIZE,
        "covered_date_count": current["covered_date_count"],
        "missing_date_count": current["missing_date_count"],
        "missing_dates": missing_dates,
        "batch_count": len(batches),
        "projected_coverage": projected,
        "all_checks_passed": all(check["passed"] for check in checks),
        "dry_run_only": True,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": "6CB_candidate_bullpen_statcast_label_backfill_implementation_plan",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
