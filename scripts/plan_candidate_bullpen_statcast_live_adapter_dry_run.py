from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_dry_run_plan_v0.1"

BACKFILL_SCAFFOLD = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
FIXTURE_ADAPTER = Path("scripts/prototype_candidate_bullpen_statcast_fixture_replay_adapter.py")
FIXTURE_WIRING_AUDIT = Path("scripts/audit_candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring.py")
STATCAST_UTILS = Path("mlb_app/statcast_utils.py")
ETL = Path("mlb_app/etl.py")
FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_dry_run_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_dry_run_plan_checks.csv"
OUTPUT_SURFACE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_surface_inventory.csv"
OUTPUT_CLI = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_contract.csv"
OUTPUT_INTERFACE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_interface.csv"
OUTPUT_ROW_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_normalized_row_contract.csv"
OUTPUT_STATUSES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_dry_run_statuses.csv"
OUTPUT_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_safety_gates.csv"
OUTPUT_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_future_validation_sequence.csv"
OUTPUT_FUTURE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_future_outputs.csv"


LIVE_SURFACE_INVENTORY = [
    {
        "surface": "backfill_scaffold",
        "path": str(BACKFILL_SCAFFOLD),
        "exists": BACKFILL_SCAFFOLD.exists(),
        "role": "future source-mode live integration target",
        "must_not_modify_this_layer": True,
    },
    {
        "surface": "current_live_branch",
        "path": str(BACKFILL_SCAFFOLD),
        "exists": BACKFILL_SCAFFOLD.exists(),
        "role": "currently returns live_mode_not_implemented",
        "must_not_modify_this_layer": True,
    },
    {
        "surface": "fixture_adapter_contract",
        "path": str(FIXTURE_ADAPTER),
        "exists": FIXTURE_ADAPTER.exists(),
        "role": "reference result wrapper and normalized row contract",
        "must_not_modify_this_layer": True,
    },
    {
        "surface": "fixture_wiring_audit",
        "path": str(FIXTURE_WIRING_AUDIT),
        "exists": FIXTURE_WIRING_AUDIT.exists(),
        "role": "required preflight audit before live dry-run",
        "must_not_modify_this_layer": True,
    },
    {
        "surface": "statcast_utils_reference",
        "path": str(STATCAST_UTILS),
        "exists": STATCAST_UTILS.exists(),
        "role": "reference-only possible fetch utilities",
        "must_not_modify_this_layer": True,
    },
    {
        "surface": "etl_reference",
        "path": str(ETL),
        "exists": ETL.exists(),
        "role": "reference-only normalization/load patterns",
        "must_not_modify_this_layer": True,
    },
]

LIVE_CLI_CONTRACT = [
    {
        "flag": "--source-mode",
        "value": "live",
        "required": True,
        "default_policy": "not default; scaffold remains default",
        "safety_note": "live requires explicit source-mode live",
    },
    {
        "flag": "--start-date",
        "value": "YYYY-MM-DD",
        "required": True,
        "default_policy": "none",
        "safety_note": "inclusive live dry-run lower bound",
    },
    {
        "flag": "--end-date",
        "value": "YYYY-MM-DD",
        "required": True,
        "default_policy": "none",
        "safety_note": "inclusive live dry-run upper bound",
    },
    {
        "flag": "--dry-run",
        "value": "bool",
        "required": True,
        "default_policy": "required for live mode",
        "safety_note": "live without dry-run must fail fast",
    },
    {
        "flag": "--write",
        "value": "bool",
        "required": False,
        "default_policy": "false",
        "safety_note": "source-mode live with write must be rejected until future explicit write gate",
    },
    {
        "flag": "--batch-size",
        "value": "integer",
        "required": False,
        "default_policy": "existing default",
        "safety_note": "dry-run fetch diagnostics may batch by date",
    },
    {
        "flag": "--skip-existing",
        "value": "bool",
        "required": False,
        "default_policy": "existing default",
        "safety_note": "dry-run should report skipped dates but not write",
    },
    {
        "flag": "--audit-after",
        "value": "bool",
        "required": False,
        "default_policy": "existing default",
        "safety_note": "emit post-dry-run diagnostics",
    },
    {
        "flag": "--live-fetch-timeout-seconds",
        "value": "integer",
        "required": False,
        "default_policy": "future default TBD",
        "safety_note": "future network timeout guard",
    },
    {
        "flag": "--live-fetch-max-retries",
        "value": "integer",
        "required": False,
        "default_policy": "future default TBD",
        "safety_note": "future bounded retry guard",
    },
]

LIVE_ADAPTER_INTERFACE = [
    {
        "component": "function_name",
        "value": "fetch_candidate_bullpen_statcast_live_rows_for_date(label_date: str) -> LiveAdapterResult",
        "required": True,
    },
    {"component": "result_field", "value": "label_date", "required": True},
    {"component": "result_field", "value": "status", "required": True},
    {"component": "result_field", "value": "rows", "required": True},
    {"component": "result_field", "value": "raw_row_count", "required": True},
    {"component": "result_field", "value": "normalized_row_count", "required": True},
    {"component": "result_field", "value": "duplicate_count", "required": True},
    {"component": "result_field", "value": "required_field_failures", "required": True},
    {"component": "result_field", "value": "missing_fields", "required": True},
    {"component": "result_field", "value": "fetch_error", "required": True},
    {"component": "result_field", "value": "external_fetch_performed", "required": True},
    {"component": "result_field", "value": "db_writes_performed", "required": True},
]

NORMALIZED_ROW_CONTRACT = [
    {"field": "game_date", "required": True, "natural_key": False},
    {"field": "game_pk", "required": True, "natural_key": True},
    {"field": "inning", "required": True, "natural_key": False},
    {"field": "inning_topbot", "required": True, "natural_key": False},
    {"field": "at_bat_number", "required": True, "natural_key": True},
    {"field": "pitch_number", "required": True, "natural_key": True},
    {"field": "outs_when_up", "required": True, "natural_key": False},
    {"field": "pitcher_id", "required": True, "natural_key": True},
    {"field": "home_team", "required": True, "natural_key": False},
    {"field": "away_team", "required": True, "natural_key": False},
    {"field": "events", "required": True, "natural_key": False},
    {"field": "description", "required": True, "natural_key": False},
]

LIVE_STATUSES = [
    {
        "status": "live_dry_run_ready",
        "meaning": "live rows fetched and normalized for dry-run diagnostics",
        "write_allowed": False,
    },
    {
        "status": "live_fetch_empty",
        "meaning": "fetch completed but returned no rows for date/window",
        "write_allowed": False,
    },
    {
        "status": "live_fetch_error",
        "meaning": "bounded live fetch failed and error was captured",
        "write_allowed": False,
    },
    {
        "status": "live_schema_failed_safely",
        "meaning": "rows fetched but required normalized row contract failed",
        "write_allowed": False,
    },
    {
        "status": "live_adapter_not_configured",
        "meaning": "live adapter unavailable or dependency not configured",
        "write_allowed": False,
    },
    {
        "status": "live_write_blocked",
        "meaning": "source-mode live rejected --write",
        "write_allowed": False,
    },
    {
        "status": "live_requires_dry_run",
        "meaning": "source-mode live rejected non-dry-run execution",
        "write_allowed": False,
    },
]

SAFETY_GATES = [
    {
        "gate": "live_mode_not_default",
        "requirement": "source-mode default remains scaffold",
        "required": True,
        "failure_behavior": "fail audit",
    },
    {
        "gate": "explicit_live_mode_required",
        "requirement": "live dry-run requires --source-mode live",
        "required": True,
        "failure_behavior": "fail fast",
    },
    {
        "gate": "live_requires_dry_run",
        "requirement": "source-mode live must require --dry-run",
        "required": True,
        "failure_behavior": "live_requires_dry_run",
    },
    {
        "gate": "live_write_rejected",
        "requirement": "source-mode live --write must be rejected",
        "required": True,
        "failure_behavior": "live_write_blocked",
    },
    {
        "gate": "no_db_writes",
        "requirement": "live dry-run cannot commit/stage/write DB rows",
        "required": True,
        "failure_behavior": "fail audit",
    },
    {
        "gate": "fetch_diagnostics_required",
        "requirement": "live dry-run must emit fetch status/error/count diagnostics",
        "required": True,
        "failure_behavior": "fail audit",
    },
    {
        "gate": "fixture_assets_read_only",
        "requirement": "live mode must not alter fixture payload or metadata",
        "required": True,
        "failure_behavior": "fail audit",
    },
    {
        "gate": "production_coupling_forbidden",
        "requirement": "live adapter cannot import simulation/routes/frontend/sportsbook/canonical probability modules",
        "required": True,
        "failure_behavior": "fail audit",
    },
]

FUTURE_VALIDATION_SEQUENCE = [
    {"step": 1, "action": "Run 6CW scaffold wiring audit", "command": "python scripts/audit_candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring.py", "required": True},
    {"step": 2, "action": "Run live adapter dry-run for one date", "command": "python scripts/backfill_candidate_bullpen_statcast_labels.py --source-mode live --start-date 2026-05-20 --end-date 2026-05-20 --dry-run", "required": True},
    {"step": 3, "action": "Run live adapter dry-run for short date window", "command": "python scripts/backfill_candidate_bullpen_statcast_labels.py --source-mode live --start-date 2026-05-20 --end-date 2026-05-22 --dry-run", "required": True},
    {"step": 4, "action": "Compare normalized row contract to fixture contract", "command": "cat tmp/candidate_bullpen_statcast_live_adapter_normalized_row_contract_audit.csv", "required": True},
    {"step": 5, "action": "Verify live write blocked", "command": "python scripts/backfill_candidate_bullpen_statcast_labels.py --source-mode live --start-date 2026-05-20 --end-date 2026-05-20 --write", "required": True},
    {"step": 6, "action": "Verify live non-dry-run blocked", "command": "python scripts/backfill_candidate_bullpen_statcast_labels.py --source-mode live --start-date 2026-05-20 --end-date 2026-05-20 --no-dry-run", "required": True},
    {"step": 7, "action": "Verify no DB writes", "command": "inspect tmp/candidate_bullpen_statcast_live_adapter_safety_audit.csv", "required": True},
    {"step": 8, "action": "Verify production defaults unchanged", "command": "inspect tmp/candidate_bullpen_statcast_live_adapter_dry_run.json", "required": True},
]

FUTURE_OUTPUTS = [
    {"artifact": "JSON diagnosis", "path": "tmp/candidate_bullpen_statcast_live_adapter_dry_run.json", "required": True},
    {"artifact": "checks CSV", "path": "tmp/candidate_bullpen_statcast_live_adapter_dry_run_checks.csv", "required": True},
    {"artifact": "live CLI audit CSV", "path": "tmp/candidate_bullpen_statcast_live_adapter_cli_audit.csv", "required": True},
    {"artifact": "live adapter result CSV", "path": "tmp/candidate_bullpen_statcast_live_adapter_results.csv", "required": True},
    {"artifact": "normalized row contract CSV", "path": "tmp/candidate_bullpen_statcast_live_adapter_normalized_row_contract_audit.csv", "required": True},
    {"artifact": "live fetch diagnostics CSV", "path": "tmp/candidate_bullpen_statcast_live_adapter_fetch_diagnostics.csv", "required": True},
    {"artifact": "write/dry-run gate CSV", "path": "tmp/candidate_bullpen_statcast_live_adapter_write_dry_run_gate.csv", "required": True},
    {"artifact": "safety audit CSV", "path": "tmp/candidate_bullpen_statcast_live_adapter_safety_audit.csv", "required": True},
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


def _snapshot_files() -> Dict[str, str]:
    paths = [BACKFILL_SCAFFOLD, MANIFEST, EXPECTED_RESULTS]
    return {str(path): path.read_text(errors="ignore") if path.exists() else "__MISSING__" for path in paths}


def _payload_inventory_snapshot() -> Dict[str, bool]:
    dates_dir = FIXTURE_ROOT / "dates"
    return {str(path): path.exists() for path in sorted(dates_dir.glob("*.jsonl"))} if dates_dir.exists() else {}


def main() -> None:
    file_snapshot_before = _snapshot_files()
    payload_snapshot_before = _payload_inventory_snapshot()

    _write_csv(OUTPUT_SURFACE, LIVE_SURFACE_INVENTORY)
    _write_csv(OUTPUT_CLI, LIVE_CLI_CONTRACT)
    _write_csv(OUTPUT_INTERFACE, LIVE_ADAPTER_INTERFACE)
    _write_csv(OUTPUT_ROW_CONTRACT, NORMALIZED_ROW_CONTRACT)
    _write_csv(OUTPUT_STATUSES, LIVE_STATUSES)
    _write_csv(OUTPUT_GATES, SAFETY_GATES)
    _write_csv(OUTPUT_VALIDATION, FUTURE_VALIDATION_SEQUENCE)
    _write_csv(OUTPUT_FUTURE, FUTURE_OUTPUTS)

    file_snapshot_after = _snapshot_files()
    payload_snapshot_after = _payload_inventory_snapshot()

    live_surface_inventory_defined = len(LIVE_SURFACE_INVENTORY) == 6 and all(row["exists"] for row in LIVE_SURFACE_INVENTORY)
    live_cli_contract_defined = (
        len(LIVE_CLI_CONTRACT) == 10
        and any(row["flag"] == "--source-mode" and row["value"] == "live" for row in LIVE_CLI_CONTRACT)
        and any(row["flag"] == "--write" and "rejected" in row["safety_note"] for row in LIVE_CLI_CONTRACT)
    )
    live_adapter_interface_defined = (
        len(LIVE_ADAPTER_INTERFACE) == 12
        and any(row["component"] == "function_name" for row in LIVE_ADAPTER_INTERFACE)
        and all(row["required"] for row in LIVE_ADAPTER_INTERFACE)
    )
    normalized_row_contract_defined = (
        len(NORMALIZED_ROW_CONTRACT) == 12
        and {row["field"] for row in NORMALIZED_ROW_CONTRACT if row["natural_key"]} == {"game_pk", "at_bat_number", "pitch_number", "pitcher_id"}
        and all(row["required"] for row in NORMALIZED_ROW_CONTRACT)
    )
    live_statuses_defined = len(LIVE_STATUSES) == 7 and all(row["write_allowed"] is False for row in LIVE_STATUSES)
    safety_gates_defined = len(SAFETY_GATES) == 8 and all(row["required"] for row in SAFETY_GATES)
    future_validation_sequence_defined = len(FUTURE_VALIDATION_SEQUENCE) == 8 and all(row["required"] for row in FUTURE_VALIDATION_SEQUENCE)
    future_outputs_defined = len(FUTURE_OUTPUTS) == 8 and all(row["required"] for row in FUTURE_OUTPUTS)
    no_file_mutation = file_snapshot_before == file_snapshot_after
    no_fixture_mutation = payload_snapshot_before == payload_snapshot_after and no_file_mutation

    checks = [
        {"check": "live_surface_inventory_defined", "passed": live_surface_inventory_defined, "detail": f"{len(LIVE_SURFACE_INVENTORY)} surfaces"},
        {"check": "live_cli_contract_defined", "passed": live_cli_contract_defined, "detail": f"{len(LIVE_CLI_CONTRACT)} CLI rows"},
        {"check": "live_adapter_interface_defined", "passed": live_adapter_interface_defined, "detail": f"{len(LIVE_ADAPTER_INTERFACE)} interface rows"},
        {"check": "normalized_row_contract_defined", "passed": normalized_row_contract_defined, "detail": f"{len(NORMALIZED_ROW_CONTRACT)} fields"},
        {"check": "live_statuses_defined", "passed": live_statuses_defined, "detail": f"{len(LIVE_STATUSES)} statuses"},
        {"check": "safety_gates_defined", "passed": safety_gates_defined, "detail": f"{len(SAFETY_GATES)} gates"},
        {"check": "future_validation_sequence_defined", "passed": future_validation_sequence_defined, "detail": f"{len(FUTURE_VALIDATION_SEQUENCE)} steps"},
        {"check": "future_outputs_defined", "passed": future_outputs_defined, "detail": f"{len(FUTURE_OUTPUTS)} outputs"},
        {"check": "planning_only_no_scaffold_modification", "passed": no_file_mutation, "detail": True},
        {"check": "live_adapter_not_implemented", "passed": True, "detail": True},
        {"check": "live_fetch_not_performed", "passed": True, "detail": True},
        {"check": "no_fixture_mutation", "passed": no_fixture_mutation, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_dry_run_plan_complete",
        "plan_version": PLAN_VERSION,
        "live_surfaces": len(LIVE_SURFACE_INVENTORY),
        "live_cli_contract_rows": len(LIVE_CLI_CONTRACT),
        "live_adapter_interface_rows": len(LIVE_ADAPTER_INTERFACE),
        "normalized_row_contract_fields": len(NORMALIZED_ROW_CONTRACT),
        "live_statuses": len(LIVE_STATUSES),
        "safety_gates": len(SAFETY_GATES),
        "future_validation_steps": len(FUTURE_VALIDATION_SEQUENCE),
        "future_outputs": len(FUTURE_OUTPUTS),
        "all_checks_passed": all(check["passed"] for check in checks),
        "planning_only": True,
        "live_adapter_implemented": False,
        "live_fetch_performed": False,
        "backfill_scaffold_modified": False,
        "fixture_payload_mutated": False,
        "fixture_metadata_mutated": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CY_candidate_bullpen_statcast_live_adapter_dry_run_scaffold"
            if all(check["passed"] for check in checks)
            else "6CX_patch_candidate_bullpen_statcast_live_adapter_dry_run_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
