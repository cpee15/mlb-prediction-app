from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_label_backfill_implementation_plan_v0.1"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_implementation_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_implementation_plan_checks.csv"
OUTPUT_INTEGRATION = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_integration_points.csv"
OUTPUT_COMMAND = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_command_architecture.csv"
OUTPUT_ADAPTER = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_fetch_adapter_contract.csv"
OUTPUT_NORMALIZATION = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_normalization_contract.csv"
OUTPUT_WRITE = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_write_semantics.csv"
OUTPUT_FAILURE = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_failure_semantics.csv"
OUTPUT_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_label_backfill_validation_sequence.csv"


INTEGRATION_CANDIDATES = [
    {
        "path": "mlb_app/statcast_utils.py",
        "intended_role": "preferred_fetch_adapter_source",
        "priority": 1,
        "reason": "Likely existing Statcast/pybaseball utility boundary; safest place to reuse external fetch behavior.",
        "must_not_mutate": True,
    },
    {
        "path": "mlb_app/etl.py",
        "intended_role": "existing_ingestion_reference",
        "priority": 2,
        "reason": "High-signal ETL location with existing load/write patterns that future implementation can mirror.",
        "must_not_mutate": True,
    },
    {
        "path": "scripts/backfill_hitter_statcast.py",
        "intended_role": "backfill_cli_reference",
        "priority": 3,
        "reason": "Existing backfill script likely shows CLI/date-window/write semantics.",
        "must_not_mutate": True,
    },
    {
        "path": "scripts/run_hitting_matchups_refresh.py",
        "intended_role": "refresh_orchestration_reference",
        "priority": 4,
        "reason": "Existing refresh command may show safe orchestration and skip-existing logic.",
        "must_not_mutate": True,
    },
    {
        "path": "scripts/nightly_statcast_refresh.py",
        "intended_role": "scheduled_refresh_reference",
        "priority": 5,
        "reason": "Existing nightly Statcast flow can inform audit-after and idempotent operation design.",
        "must_not_mutate": True,
    },
    {
        "path": "mlb_app/database.py",
        "intended_role": "session_factory_and_engine_source",
        "priority": 6,
        "reason": "Future script must use existing engine/session factory patterns.",
        "must_not_mutate": True,
    },
]


COMMAND_ARCHITECTURE = [
    {"component": "script_name", "value": "scripts/backfill_candidate_bullpen_statcast_labels.py", "required": True, "default": None, "safety_note": "Future script only; not created in this layer."},
    {"component": "argparse_cli", "value": "required", "required": True, "default": None, "safety_note": "Use explicit CLI args rather than hidden constants."},
    {"component": "--start-date", "value": "YYYY-MM-DD", "required": True, "default": None, "safety_note": "Inclusive lower bound."},
    {"component": "--end-date", "value": "YYYY-MM-DD", "required": True, "default": None, "safety_note": "Inclusive upper bound."},
    {"component": "--dry-run", "value": "bool flag", "required": False, "default": True, "safety_note": "Default mode must not fetch/write."},
    {"component": "--write", "value": "bool flag", "required": False, "default": False, "safety_note": "DB writes require explicit opt-in."},
    {"component": "--skip-existing", "value": "bool flag", "required": False, "default": True, "safety_note": "Avoid reloading covered dates by default."},
    {"component": "--audit-after", "value": "bool flag", "required": False, "default": True, "safety_note": "Post-write coverage audit should run by default."},
    {"component": "--batch-size", "value": "integer", "required": False, "default": 3, "safety_note": "Commit and rollback are batch-scoped."},
]


FETCH_ADAPTER_CONTRACT = [
    {"contract_item": "adapter_name", "requirement": "fetch_statcast_label_rows_for_date(date)", "required": True},
    {"contract_item": "isolation", "requirement": "Adapter performs fetch/import only and returns normalized row dicts.", "required": True},
    {"contract_item": "no_direct_db_write", "requirement": "Adapter cannot create sessions, commit, insert, or mutate database state.", "required": True},
    {"contract_item": "reuse_existing_utils", "requirement": "Prefer mlb_app.statcast_utils if usable; otherwise isolate pybaseball calls in the script adapter.", "required": True},
    {"contract_item": "error_boundary", "requirement": "Network/data errors are raised or returned per date, not swallowed globally.", "required": True},
    {"contract_item": "row_shape", "requirement": "Adapter returns list[dict] with the normalization contract fields.", "required": True},
]


NORMALIZATION_CONTRACT = [
    {"field": "game_date", "required": True, "nullable": False, "natural_key": False},
    {"field": "game_pk", "required": True, "nullable": False, "natural_key": True},
    {"field": "inning", "required": True, "nullable": False, "natural_key": False},
    {"field": "inning_topbot", "required": True, "nullable": False, "natural_key": False},
    {"field": "at_bat_number", "required": True, "nullable": False, "natural_key": True},
    {"field": "pitch_number", "required": True, "nullable": False, "natural_key": True},
    {"field": "outs_when_up", "required": True, "nullable": True, "natural_key": False},
    {"field": "pitcher_id", "required": True, "nullable": False, "natural_key": True},
    {"field": "home_team", "required": True, "nullable": False, "natural_key": False},
    {"field": "away_team", "required": True, "nullable": False, "natural_key": False},
    {"field": "events", "required": True, "nullable": True, "natural_key": False},
    {"field": "description", "required": True, "nullable": True, "natural_key": False},
]


WRITE_SEMANTICS = [
    {"semantic": "default_no_write", "requirement": "No database write unless --write is explicitly passed.", "required": True},
    {"semantic": "dry_run_precedence", "requirement": "If --dry-run is true, writes are disabled even if dates are missing.", "required": True},
    {"semantic": "skip_existing", "requirement": "When --skip-existing is true, dates with existing statcast_events rows are skipped.", "required": True},
    {"semantic": "session_factory", "requirement": "Use get_engine/create_tables/get_session from mlb_app.database.", "required": True},
    {"semantic": "dedupe_natural_key", "requirement": "Dedupe by game_pk + at_bat_number + pitch_number + pitcher_id before insert.", "required": True},
    {"semantic": "batch_commit", "requirement": "Commit only after all dates in a batch normalize and stage successfully.", "required": True},
    {"semantic": "batch_rollback", "requirement": "Rollback the entire batch on any batch failure.", "required": True},
    {"semantic": "audit_rows", "requirement": "Emit per-date inserted/skipped/failed/fetched counts to tmp outputs.", "required": True},
]


FAILURE_SEMANTICS = [
    {"failure_mode": "fetch_error", "handling": "Capture per date, mark date failed, continue only if outside active batch write or rollback active batch.", "required": True},
    {"failure_mode": "schema_error", "handling": "Fail the batch if required normalized fields are absent.", "required": True},
    {"failure_mode": "duplicate_rows", "handling": "Dedupe natural keys before staging and report duplicate count.", "required": True},
    {"failure_mode": "db_write_error", "handling": "Rollback batch and report failed dates/counts.", "required": True},
    {"failure_mode": "post_write_audit_failure", "handling": "Do not advance calibration gates; require 6BY/6BW/6BX rerun.", "required": True},
    {"failure_mode": "production_coupling_detected", "handling": "Fail immediately if script imports game engine, routes, sportsbook, frontend, or canonical probability modules.", "required": True},
]


VALIDATION_SEQUENCE = [
    {"step": 1, "action": "Run future script in dry-run mode", "command": "python scripts/backfill_candidate_bullpen_statcast_labels.py --start-date 2026-04-21 --end-date 2026-05-20 --dry-run --skip-existing --audit-after", "required": True},
    {"step": 2, "action": "Inspect dry-run batch plan", "command": "cat tmp/candidate_bullpen_statcast_label_backfill_dry_run_batches.csv", "required": True},
    {"step": 3, "action": "Run future write mode only after dry-run approval", "command": "python scripts/backfill_candidate_bullpen_statcast_labels.py --start-date 2026-04-21 --end-date 2026-05-20 --write --skip-existing --audit-after", "required": True},
    {"step": 4, "action": "Rerun 6BY coverage plan", "command": "python scripts/plan_candidate_bullpen_statcast_label_coverage_expansion.py", "required": True},
    {"step": 5, "action": "Rerun 6BW historical usage join", "command": "python scripts/prototype_candidate_bullpen_historical_usage_join.py", "required": True},
    {"step": 6, "action": "Rerun 6BX reliability gate", "command": "python scripts/analyze_candidate_bullpen_historical_usage_join.py", "required": True},
    {"step": 7, "action": "Advance only if calibration_grade true", "command": "inspect tmp/candidate_bullpen_historical_usage_join_analysis.json", "required": True},
]


def _read_text(path: Path) -> str:
    try:
        return path.read_text(errors="ignore")
    except Exception:
        return ""


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _integration_inventory() -> List[Dict[str, Any]]:
    rows = []
    for item in INTEGRATION_CANDIDATES:
        path = Path(item["path"])
        text = _read_text(path)
        lowered = text.lower()

        rows.append({
            **item,
            "exists": path.exists(),
            "has_statcast_signal": "statcast" in lowered or "statcast" in item["path"].lower(),
            "has_fetch_signal": "pybaseball" in lowered or "requests" in lowered or "fetch" in lowered,
            "has_write_signal": any(token in lowered for token in ["session.add(", "session.commit(", ".to_sql(", "insert into"]),
            "has_cli_signal": "argparse" in lowered or "if __name__" in lowered,
            "integration_score": (
                (3 if path.exists() else 0)
                + (2 if ("statcast" in lowered or "statcast" in item["path"].lower()) else 0)
                + (1 if ("pybaseball" in lowered or "fetch" in lowered) else 0)
                + (1 if ("argparse" in lowered or "if __name__" in lowered) else 0)
            ),
        })
    return rows


def main() -> None:
    integration_rows = _integration_inventory()

    _write_csv(OUTPUT_INTEGRATION, integration_rows)
    _write_csv(OUTPUT_COMMAND, COMMAND_ARCHITECTURE)
    _write_csv(OUTPUT_ADAPTER, FETCH_ADAPTER_CONTRACT)
    _write_csv(OUTPUT_NORMALIZATION, NORMALIZATION_CONTRACT)
    _write_csv(OUTPUT_WRITE, WRITE_SEMANTICS)
    _write_csv(OUTPUT_FAILURE, FAILURE_SEMANTICS)
    _write_csv(OUTPUT_VALIDATION, VALIDATION_SEQUENCE)

    integration_points_inventoried = len(integration_rows) == len(INTEGRATION_CANDIDATES) and any(row["exists"] for row in integration_rows)
    command_architecture_defined = len(COMMAND_ARCHITECTURE) >= 9 and any(row["component"] == "--write" and row["default"] is False for row in COMMAND_ARCHITECTURE)
    fetch_adapter_contract_defined = len(FETCH_ADAPTER_CONTRACT) >= 6 and all(row["required"] for row in FETCH_ADAPTER_CONTRACT)
    normalization_contract_defined = len(NORMALIZATION_CONTRACT) == 12 and sum(1 for row in NORMALIZATION_CONTRACT if row["natural_key"]) == 4
    idempotent_write_contract_defined = len(WRITE_SEMANTICS) >= 8 and any(row["semantic"] == "dedupe_natural_key" for row in WRITE_SEMANTICS)
    failure_semantics_defined = len(FAILURE_SEMANTICS) >= 6
    validation_sequence_defined = len(VALIDATION_SEQUENCE) >= 7

    checks = [
        {"check": "integration_points_inventoried", "passed": integration_points_inventoried, "detail": f"{len(integration_rows)} candidates"},
        {"check": "command_architecture_defined", "passed": command_architecture_defined, "detail": "argparse + dry-run/write/skip-existing/audit-after/batch-size"},
        {"check": "fetch_adapter_contract_defined", "passed": fetch_adapter_contract_defined, "detail": f"{len(FETCH_ADAPTER_CONTRACT)} adapter requirements"},
        {"check": "normalization_contract_defined", "passed": normalization_contract_defined, "detail": f"{len(NORMALIZATION_CONTRACT)} fields"},
        {"check": "idempotent_write_contract_defined", "passed": idempotent_write_contract_defined, "detail": f"{len(WRITE_SEMANTICS)} write semantics"},
        {"check": "failure_semantics_defined", "passed": failure_semantics_defined, "detail": f"{len(FAILURE_SEMANTICS)} failure modes"},
        {"check": "validation_sequence_defined", "passed": validation_sequence_defined, "detail": f"{len(VALIDATION_SEQUENCE)} steps"},
        {"check": "implementation_plan_only", "passed": True, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]

    with OUTPUT_CHECKS.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["check", "passed", "detail"])
        writer.writeheader()
        writer.writerows(checks)

    best_integration = sorted(
        integration_rows,
        key=lambda row: (row["integration_score"], -row["priority"]),
        reverse=True,
    )[0] if integration_rows else None

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_label_backfill_implementation_plan_complete",
        "plan_version": PLAN_VERSION,
        "integration_candidates": len(integration_rows),
        "best_integration_point": best_integration,
        "future_script_name": "scripts/backfill_candidate_bullpen_statcast_labels.py",
        "command_defaults": {
            "dry_run": True,
            "write": False,
            "skip_existing": True,
            "audit_after": True,
            "batch_size": 3,
        },
        "natural_key": ["game_pk", "at_bat_number", "pitch_number", "pitcher_id"],
        "required_normalized_fields": [row["field"] for row in NORMALIZATION_CONTRACT],
        "all_checks_passed": all(check["passed"] for check in checks),
        "implementation_plan_only": True,
        "actual_backfill_script_created": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": "6CC_candidate_bullpen_statcast_label_backfill_script_scaffold",
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
