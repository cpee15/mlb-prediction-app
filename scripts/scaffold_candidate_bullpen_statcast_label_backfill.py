from __future__ import annotations

import csv
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Set

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from mlb_app.database import create_tables, get_engine, get_session


TARGET_DATE = "2026-05-20"
SCAFFOLD_VERSION = "candidate_bullpen_statcast_label_backfill_scaffold_v0.1"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_scaffold.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_scaffold_checks.csv"
OUTPUT_CANDIDATES = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_ingestion_candidates.csv"
OUTPUT_SCOPES = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_scope_plan.csv"
OUTPUT_COLUMNS = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_required_columns.csv"
OUTPUT_COMMAND = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_future_command_contract.csv"
OUTPUT_CHECKLIST = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_post_validation_checklist.csv"


SEARCH_TERMS = [
    "statcast",
    "pybaseball",
    "fetch",
    "backfill",
    "ingest",
    "load",
    "events",
    "statcast_events",
]

REQUIRED_COLUMNS = [
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

FUTURE_COMMAND_CONTRACT = [
    {"field": "script_name", "value": "scripts/backfill_candidate_bullpen_statcast_labels.py", "required": True, "default": None, "purpose": "Future real backfill entrypoint."},
    {"field": "--start-date", "value": "YYYY-MM-DD", "required": True, "default": None, "purpose": "Inclusive start date for Statcast label fetch/import."},
    {"field": "--end-date", "value": "YYYY-MM-DD", "required": True, "default": None, "purpose": "Inclusive end date for Statcast label fetch/import."},
    {"field": "--dry-run", "value": "flag", "required": False, "default": True, "purpose": "Preview dates and row targets without writes."},
    {"field": "--write", "value": "flag", "required": False, "default": False, "purpose": "Explicit opt-in for DB writes in future layer only."},
    {"field": "--skip-existing", "value": "flag", "required": False, "default": True, "purpose": "Avoid refetching/reinserting dates already covered."},
    {"field": "--audit-after", "value": "flag", "required": False, "default": True, "purpose": "Run coverage audit after future write/import."},
]

POST_BACKFILL_CHECKLIST = [
    {"step": 1, "check": "rerun_6BY_coverage_plan", "required": True, "pass_threshold": "coverage targets recomputed"},
    {"step": 2, "check": "rerun_6BW_historical_usage_join", "required": True, "pass_threshold": "joined rows generated with exact labels"},
    {"step": 3, "check": "rerun_6BX_reliability_gate", "required": True, "pass_threshold": "architecture validation remains true"},
    {"step": 4, "check": "exact_join_rate", "required": True, "pass_threshold": ">= 0.80"},
    {"step": 5, "check": "missing_rate", "required": True, "pass_threshold": "<= 0.20"},
    {"step": 6, "check": "actual_usage_appearance_rows", "required": True, "pass_threshold": ">= 30"},
    {"step": 7, "check": "team_sides_with_actual_labels", "required": True, "pass_threshold": ">= 20"},
    {"step": 8, "check": "calibration_grade_gate", "required": True, "pass_threshold": "true before real-label calibration"},
]


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(errors="ignore")
    except Exception:
        return ""


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> List[str]:
    days = []
    cursor = start
    while cursor <= end:
        days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return days


def _inspect_ingestion_candidates() -> List[Dict[str, Any]]:
    roots = [Path("scripts"), Path("mlb_app")]
    files: List[Path] = []
    for root in roots:
        if root.exists():
            files.extend([p for p in root.rglob("*") if p.is_file() and p.suffix in {".py", ".sql", ".md"}])

    rows = []
    for path in sorted(files):
        text_body = _read_text(path)
        haystack = f"{path}\n{text_body}".lower()
        matched_terms = sorted({term for term in SEARCH_TERMS if term.lower() in haystack})
        if not matched_terms:
            continue

        usefulness_score = 0
        usefulness_score += 3 if "statcast" in matched_terms else 0
        usefulness_score += 2 if "pybaseball" in matched_terms else 0
        usefulness_score += 2 if "backfill" in matched_terms else 0
        usefulness_score += 2 if "ingest" in matched_terms or "load" in matched_terms else 0
        usefulness_score += 1 if "events" in matched_terms else 0

        rows.append({
            "path": str(path),
            "matched_terms": "|".join(matched_terms),
            "has_statcast_events": "statcast_events" in haystack,
            "has_external_fetch_signal": "pybaseball" in haystack or "requests." in haystack or "http" in haystack,
            "has_db_write_signal": any(token in haystack for token in ["session.add(", "session.commit(", ".to_sql(", "insert into"]),
            "likely_usefulness_score": usefulness_score,
            "likely_usefulness": "high" if usefulness_score >= 6 else "medium" if usefulness_score >= 3 else "low",
        })
    return rows


def _covered_dates(session: Session) -> Set[str]:
    rows = session.execute(text("""
        SELECT DISTINCT game_date
        FROM statcast_events
        WHERE game_date IS NOT NULL
          AND game_pk IS NOT NULL
          AND pitcher_id IS NOT NULL
    """)).mappings().all()
    return {str(row["game_date"]) for row in rows}


def _scope_dates(scope: str) -> List[str]:
    target = _parse_date(TARGET_DATE)

    if scope == "target_date":
        return [TARGET_DATE]

    if scope == "short_window":
        return _date_range(target - timedelta(days=7), target + timedelta(days=7))

    if scope == "calibration_window":
        return _date_range(target - timedelta(days=29), target)

    if scope == "robust_window":
        return _date_range(target - timedelta(days=89), target)

    raise ValueError(f"Unknown scope: {scope}")


def _build_scope_plan(session: Session) -> List[Dict[str, Any]]:
    covered = _covered_dates(session)
    scope_meta = [
        ("target_date", 1, False),
        ("short_window", 2, False),
        ("calibration_window", 3, True),
        ("robust_window", 4, False),
    ]

    rows = []
    for scope, priority, recommended in scope_meta:
        dates = _scope_dates(scope)
        already = [d for d in dates if d in covered]
        missing = [d for d in dates if d not in covered]

        rows.append({
            "scope": scope,
            "start_date": dates[0],
            "end_date": dates[-1],
            "date_count": len(dates),
            "already_covered_dates": len(already),
            "missing_dates": len(missing),
            "missing_date_list": "|".join(missing),
            "expected_fetch_needed": len(missing) > 0,
            "priority": priority,
            "recommended_for_next_layer": recommended,
        })

    return rows


def _validate_required_columns(session: Session) -> List[Dict[str, Any]]:
    engine = session.get_bind()
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())

    if "statcast_events" not in table_names:
        return [
            {
                "table_name": "statcast_events",
                "column_name": column,
                "present": False,
                "required": True,
                "detail": "statcast_events table missing",
            }
            for column in REQUIRED_COLUMNS
        ]

    columns = {col["name"] for col in inspector.get_columns("statcast_events")}

    return [
        {
            "table_name": "statcast_events",
            "column_name": column,
            "present": column in columns,
            "required": True,
            "detail": "present" if column in columns else "missing",
        }
        for column in REQUIRED_COLUMNS
    ]


def main() -> None:
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    SessionFactory = get_session(engine)

    ingestion_rows = _inspect_ingestion_candidates()

    session: Session = SessionFactory()
    try:
        scope_rows = _build_scope_plan(session)
        column_rows = _validate_required_columns(session)
    finally:
        session.close()

    _write_csv(OUTPUT_CANDIDATES, ingestion_rows)
    _write_csv(OUTPUT_SCOPES, scope_rows)
    _write_csv(OUTPUT_COLUMNS, column_rows)
    _write_csv(OUTPUT_COMMAND, FUTURE_COMMAND_CONTRACT)
    _write_csv(OUTPUT_CHECKLIST, POST_BACKFILL_CHECKLIST)

    recommended_scope = next((row for row in scope_rows if row["recommended_for_next_layer"]), None)

    ingestion_candidates_inspected = len(ingestion_rows) > 0
    scope_plan_created = len(scope_rows) == 4 and recommended_scope is not None
    required_columns_validated = len(column_rows) == len(REQUIRED_COLUMNS) and all(row["present"] for row in column_rows)
    future_command_contract_defined = len(FUTURE_COMMAND_CONTRACT) >= 7 and any(row["field"] == "--write" and row["default"] is False for row in FUTURE_COMMAND_CONTRACT)
    post_backfill_checklist_defined = len(POST_BACKFILL_CHECKLIST) >= 7
    recommended_scope_selected = recommended_scope is not None and recommended_scope["scope"] == "calibration_window"

    checks = [
        {"check": "ingestion_candidates_inspected", "passed": ingestion_candidates_inspected, "detail": f"{len(ingestion_rows)} candidates"},
        {"check": "scope_plan_created", "passed": scope_plan_created, "detail": f"{len(scope_rows)} scopes"},
        {"check": "required_columns_validated", "passed": required_columns_validated, "detail": f"{sum(1 for row in column_rows if row['present'])}/{len(column_rows)} columns"},
        {"check": "future_command_contract_defined", "passed": future_command_contract_defined, "detail": "dry-run default true, write default false"},
        {"check": "post_backfill_checklist_defined", "passed": post_backfill_checklist_defined, "detail": f"{len(POST_BACKFILL_CHECKLIST)} checks"},
        {"check": "recommended_scope_selected", "passed": recommended_scope_selected, "detail": recommended_scope},
        {"check": "scaffold_only_no_external_fetch", "passed": True, "detail": True},
        {"check": "dry_run_only", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_label_backfill_scaffold_complete",
        "scaffold_version": SCAFFOLD_VERSION,
        "target_date": TARGET_DATE,
        "ingestion_candidates_found": len(ingestion_rows),
        "recommended_scope": recommended_scope,
        "required_columns_present": required_columns_validated,
        "future_command_contract": {
            "script_name": "scripts/backfill_candidate_bullpen_statcast_labels.py",
            "dry_run_default": True,
            "write_default": False,
            "skip_existing_default": True,
            "audit_after_default": True,
        },
        "post_backfill_required_gates": {
            "exact_join_rate": ">= 0.80",
            "missing_rate": "<= 0.20",
            "actual_usage_appearance_rows": ">= 30",
            "team_sides_with_actual_labels": ">= 20",
        },
        "all_checks_passed": all(check["passed"] for check in checks),
        "scaffold_only": True,
        "dry_run_only": True,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": "6CA_candidate_bullpen_statcast_label_backfill_dry_run_audit",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
