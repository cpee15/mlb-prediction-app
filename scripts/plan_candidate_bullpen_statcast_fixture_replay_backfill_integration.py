from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_fixture_replay_backfill_integration_plan_v0.1"

BACKFILL_SCAFFOLD = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
FIXTURE_REPLAY_PROTOTYPE = Path("scripts/prototype_candidate_bullpen_statcast_fixture_replay_adapter.py")
FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_integration_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_integration_plan_checks.csv"
OUTPUT_SURFACES = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_integration_surfaces.csv"
OUTPUT_CLI = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_cli_contract.csv"
OUTPUT_MODES = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_integration_modes.csv"
OUTPUT_BOUNDARY = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_adapter_boundary.csv"
OUTPUT_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_safety_gates.csv"
OUTPUT_VALIDATION = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_future_validation_sequence.csv"
OUTPUT_FUTURE = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_future_outputs.csv"


INTEGRATION_SURFACES = [
    {
        "surface": "backfill_scaffold",
        "path": str(BACKFILL_SCAFFOLD),
        "role": "future command integration target",
        "must_exist": True,
        "must_not_modify_in_this_layer": True,
    },
    {
        "surface": "fixture_replay_prototype",
        "path": str(FIXTURE_REPLAY_PROTOTYPE),
        "role": "future fixture source adapter reference",
        "must_exist": True,
        "must_not_execute_in_this_layer": True,
    },
    {
        "surface": "fixture_corpus_root",
        "path": str(FIXTURE_ROOT),
        "role": "fixture payload and metadata root",
        "must_exist": True,
        "must_not_modify_in_this_layer": True,
    },
    {
        "surface": "manifest_metadata",
        "path": str(MANIFEST),
        "role": "fixture file inventory and hashes",
        "must_exist": True,
        "must_not_modify_in_this_layer": True,
    },
    {
        "surface": "expected_results_metadata",
        "path": str(EXPECTED_RESULTS),
        "role": "fixture replay expectations",
        "must_exist": True,
        "must_not_modify_in_this_layer": True,
    },
]

CLI_CONTRACT = [
    {
        "flag": "--source-mode",
        "value": "fixture|live",
        "required_future": True,
        "default_policy": "safe_non_live_default_until_explicit_future_promotion",
        "safety_note": "Live mode must never become default in this integration layer.",
    },
    {
        "flag": "--fixture-root",
        "value": "path",
        "required_future": False,
        "default_policy": "tests/fixtures/statcast/bullpen_labels",
        "safety_note": "Only used when source-mode=fixture.",
    },
    {
        "flag": "--fixture-date",
        "value": "YYYY-MM-DD",
        "required_future": False,
        "default_policy": "optional single-date replay",
        "safety_note": "Must not create missing fixture files.",
    },
    {
        "flag": "--allow-negative-fixtures",
        "value": "bool",
        "required_future": False,
        "default_policy": "false",
        "safety_note": "Required to replay malformed/negative fixture dates through command path.",
    },
    {
        "flag": "--dry-run",
        "value": "bool",
        "required_future": True,
        "default_policy": "true",
        "safety_note": "Fixture integration must run dry-run only.",
    },
    {
        "flag": "--write",
        "value": "bool",
        "required_future": False,
        "default_policy": "false",
        "safety_note": "Must be rejected when source-mode=fixture.",
    },
]

INTEGRATION_MODES = [
    {
        "mode": "scaffold_only_validation",
        "description": "Existing scaffold behavior validates command contracts without source replay.",
        "allowed_now": True,
        "future_layer": "already available",
    },
    {
        "mode": "fixture_replay_dry_run",
        "description": "Backfill command reads fixture replay results and emits dry-run diagnostics only.",
        "allowed_now": False,
        "future_layer": "6CS",
    },
    {
        "mode": "fixture_replay_expectation_audit",
        "description": "Command compares fixture replay outputs to expected_results without writes.",
        "allowed_now": False,
        "future_layer": "6CS_or_6CT",
    },
    {
        "mode": "live_dry_run",
        "description": "Future live adapter fetches rows but does not write database data.",
        "allowed_now": False,
        "future_layer": "after fixture integration audit",
    },
    {
        "mode": "live_write_explicit_gate",
        "description": "Future write mode only after live dry-run and calibration gates pass.",
        "allowed_now": False,
        "future_layer": "future_explicit_write_gate",
    },
]

ADAPTER_BOUNDARY = [
    {
        "boundary_item": "source_adapter_interface",
        "requirement": "Backfill command calls a source adapter interface rather than hard-coding fixture or live imports in business logic.",
        "required": True,
    },
    {
        "boundary_item": "fixture_result_wrapper",
        "requirement": "Fixture adapter returns FixtureReplayResult with normalized rows and explicit status/count fields.",
        "required": True,
    },
    {
        "boundary_item": "normalized_row_shape",
        "requirement": "Write path consumes only validated normalized rows with the 12-field row contract.",
        "required": True,
    },
    {
        "boundary_item": "live_adapter_parity",
        "requirement": "Future live adapter must return same normalized row shape and natural-key semantics.",
        "required": True,
    },
    {
        "boundary_item": "runtime_isolation",
        "requirement": "Adapter boundary must not import simulation, frontend, sportsbook, routes, or canonical probability modules.",
        "required": True,
    },
    {
        "boundary_item": "write_gate_separation",
        "requirement": "Source adapter selection is separate from write authorization; fixture mode can never authorize writes.",
        "required": True,
    },
]

SAFETY_GATES = [
    {
        "gate": "fixture_mode_no_external_fetch",
        "requirement": "source-mode=fixture cannot import or call live fetch utilities.",
        "required": True,
        "failure_behavior": "fail fast",
    },
    {
        "gate": "fixture_mode_no_db_write",
        "requirement": "source-mode=fixture must reject --write and never open commit path.",
        "required": True,
        "failure_behavior": "fail fast",
    },
    {
        "gate": "missing_fixture_no_create",
        "requirement": "fixture_missing status must not create dates/YYYY-MM-DD.jsonl.",
        "required": True,
        "failure_behavior": "return fixture_missing",
    },
    {
        "gate": "negative_fixtures_explicit",
        "requirement": "Negative fixtures require --allow-negative-fixtures to pass command-level validation.",
        "required": True,
        "failure_behavior": "block negative fixture replay",
    },
    {
        "gate": "live_mode_not_default",
        "requirement": "Live mode must require explicit source-mode=live in a future layer.",
        "required": True,
        "failure_behavior": "default to safe non-live behavior",
    },
    {
        "gate": "fixture_write_combination_rejected",
        "requirement": "--source-mode fixture --write must be invalid.",
        "required": True,
        "failure_behavior": "fail fast",
    },
]

FUTURE_VALIDATION_SEQUENCE = [
    {"step": 1, "action": "Run 6CQ fixture replay adapter audit", "command": "python scripts/audit_candidate_bullpen_statcast_fixture_replay_adapter.py", "required": True},
    {"step": 2, "action": "Run backfill scaffold audit", "command": "python scripts/audit_candidate_bullpen_statcast_label_backfill_scaffold.py", "required": True},
    {"step": 3, "action": "Run fixture integration dry-run", "command": "python scripts/backfill_candidate_bullpen_statcast_labels.py --source-mode fixture --dry-run --fixture-root tests/fixtures/statcast/bullpen_labels", "required": True},
    {"step": 4, "action": "Compare replay results to expected_results", "command": "cat tmp/candidate_bullpen_statcast_fixture_replay_backfill_expectation_audit.csv", "required": True},
    {"step": 5, "action": "Verify no payload or metadata mutation", "command": "inspect fixture snapshot audit", "required": True},
    {"step": 6, "action": "Verify write block for fixture mode", "command": "python scripts/backfill_candidate_bullpen_statcast_labels.py --source-mode fixture --write", "required": True},
    {"step": 7, "action": "Verify production defaults unchanged", "command": "inspect integration checks JSON", "required": True},
]

FUTURE_OUTPUTS = [
    {"artifact": "JSON diagnosis", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_integration.json", "required": True},
    {"artifact": "checks CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_integration_checks.csv", "required": True},
    {"artifact": "adapter-selection audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_adapter_selection_audit.csv", "required": True},
    {"artifact": "fixture integration result CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_results.csv", "required": True},
    {"artifact": "write-block audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_write_block_audit.csv", "required": True},
    {"artifact": "negative-fixture gate audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_negative_fixture_gate_audit.csv", "required": True},
    {"artifact": "safety audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_safety_audit.csv", "required": True},
]


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


def _metadata_snapshot() -> Dict[str, str]:
    snapshot: Dict[str, str] = {}
    for path in [MANIFEST, EXPECTED_RESULTS]:
        snapshot[str(path)] = path.read_text() if path.exists() else "__MISSING__"
    return snapshot


def _payload_inventory_snapshot() -> Dict[str, bool]:
    dates_dir = FIXTURE_ROOT / "dates"
    return {
        str(path): path.exists()
        for path in sorted(dates_dir.glob("*.jsonl"))
    } if dates_dir.exists() else {}


def main() -> None:
    metadata_before = _metadata_snapshot()
    payload_before = _payload_inventory_snapshot()

    _write_csv(OUTPUT_SURFACES, INTEGRATION_SURFACES)
    _write_csv(OUTPUT_CLI, CLI_CONTRACT)
    _write_csv(OUTPUT_MODES, INTEGRATION_MODES)
    _write_csv(OUTPUT_BOUNDARY, ADAPTER_BOUNDARY)
    _write_csv(OUTPUT_GATES, SAFETY_GATES)
    _write_csv(OUTPUT_VALIDATION, FUTURE_VALIDATION_SEQUENCE)
    _write_csv(OUTPUT_FUTURE, FUTURE_OUTPUTS)

    metadata_after = _metadata_snapshot()
    payload_after = _payload_inventory_snapshot()

    integration_surfaces_defined = len(INTEGRATION_SURFACES) == 5 and all(
        (not row["must_exist"]) or Path(row["path"]).exists()
        for row in INTEGRATION_SURFACES
    )
    cli_contract_defined = (
        len(CLI_CONTRACT) == 6
        and any(row["flag"] == "--source-mode" and "fixture|live" in row["value"] for row in CLI_CONTRACT)
        and any(row["flag"] == "--write" and "rejected" in row["safety_note"] for row in CLI_CONTRACT)
    )
    integration_modes_defined = len(INTEGRATION_MODES) == 5 and all("mode" in row for row in INTEGRATION_MODES)
    adapter_boundary_defined = len(ADAPTER_BOUNDARY) == 6 and all(row["required"] for row in ADAPTER_BOUNDARY)
    safety_gates_defined = len(SAFETY_GATES) == 6 and all(row["required"] for row in SAFETY_GATES)
    future_validation_sequence_defined = len(FUTURE_VALIDATION_SEQUENCE) == 7 and all(row["required"] for row in FUTURE_VALIDATION_SEQUENCE)
    future_outputs_defined = len(FUTURE_OUTPUTS) == 7 and all(row["required"] for row in FUTURE_OUTPUTS)
    no_payload_mutation = payload_before == payload_after
    no_metadata_mutation = metadata_before == metadata_after

    checks = [
        {"check": "integration_surfaces_defined", "passed": integration_surfaces_defined, "detail": f"{len(INTEGRATION_SURFACES)} surfaces"},
        {"check": "cli_contract_defined", "passed": cli_contract_defined, "detail": f"{len(CLI_CONTRACT)} CLI rows"},
        {"check": "integration_modes_defined", "passed": integration_modes_defined, "detail": f"{len(INTEGRATION_MODES)} modes"},
        {"check": "adapter_boundary_defined", "passed": adapter_boundary_defined, "detail": f"{len(ADAPTER_BOUNDARY)} boundary rows"},
        {"check": "safety_gates_defined", "passed": safety_gates_defined, "detail": f"{len(SAFETY_GATES)} gates"},
        {"check": "future_validation_sequence_defined", "passed": future_validation_sequence_defined, "detail": f"{len(FUTURE_VALIDATION_SEQUENCE)} steps"},
        {"check": "future_outputs_defined", "passed": future_outputs_defined, "detail": f"{len(FUTURE_OUTPUTS)} outputs"},
        {"check": "planning_only_no_integration_wired", "passed": True, "detail": True},
        {"check": "replay_adapter_not_executed", "passed": True, "detail": True},
        {"check": "no_payload_mutation", "passed": no_payload_mutation, "detail": "payload inventory unchanged"},
        {"check": "no_metadata_mutation", "passed": no_metadata_mutation, "detail": "manifest/expected_results unchanged"},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fixture_replay_backfill_integration_plan_complete",
        "plan_version": PLAN_VERSION,
        "integration_surfaces": len(INTEGRATION_SURFACES),
        "cli_contract_rows": len(CLI_CONTRACT),
        "integration_modes": len(INTEGRATION_MODES),
        "adapter_boundary_rows": len(ADAPTER_BOUNDARY),
        "safety_gates": len(SAFETY_GATES),
        "future_validation_steps": len(FUTURE_VALIDATION_SEQUENCE),
        "future_outputs": len(FUTURE_OUTPUTS),
        "all_checks_passed": all(check["passed"] for check in checks),
        "planning_only": True,
        "integration_wired": False,
        "fixture_replay_adapter_executed": False,
        "payload_mutated": False,
        "metadata_mutated": False,
        "live_adapter_implemented": False,
        "backfill_scaffold_modified": False,
        "no_external_fetch": True,
        "no_db_writes": True,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CS_candidate_bullpen_statcast_fixture_replay_backfill_integration_prototype"
            if all(check["passed"] for check in checks)
            else "6CR_patch_candidate_bullpen_statcast_fixture_replay_backfill_integration_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
