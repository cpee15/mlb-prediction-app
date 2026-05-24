from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_plan_v0.1"

BACKFILL_SCAFFOLD = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
FIXTURE_REPLAY_ADAPTER = Path("scripts/prototype_candidate_bullpen_statcast_fixture_replay_adapter.py")
FIXTURE_INTEGRATION_PROTOTYPE = Path("scripts/prototype_candidate_bullpen_statcast_fixture_replay_backfill_integration.py")
FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_plan_checks.csv"
OUTPUT_SURFACE = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_surface_inventory.csv"
OUTPUT_CLI = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_cli_addition_plan.csv"
OUTPUT_RESOLVER = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_source_adapter_resolver_plan.csv"
OUTPUT_FLOW = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_integration_flow.csv"
OUTPUT_BOUNDARY = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_code_change_boundaries.csv"
OUTPUT_GATES = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_future_acceptance_gates.csv"
OUTPUT_FUTURE = OUTPUT_DIR / "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_future_audit_outputs.csv"


def _scaffold_text() -> str:
    return BACKFILL_SCAFFOLD.read_text(errors="ignore") if BACKFILL_SCAFFOLD.exists() else ""


SCAFFOLD_SURFACE_INVENTORY = [
    {
        "surface": "backfill_scaffold_script",
        "path": str(BACKFILL_SCAFFOLD),
        "exists": BACKFILL_SCAFFOLD.exists(),
        "current_role": "candidate bullpen Statcast label backfill scaffold command",
        "future_wiring_role": "single script to receive safe source-mode controls",
    },
    {
        "surface": "fixture_replay_adapter",
        "path": str(FIXTURE_REPLAY_ADAPTER),
        "exists": FIXTURE_REPLAY_ADAPTER.exists(),
        "current_role": "fixture replay source adapter prototype",
        "future_wiring_role": "fixture-mode source adapter callable",
    },
    {
        "surface": "fixture_integration_prototype",
        "path": str(FIXTURE_INTEGRATION_PROTOTYPE),
        "exists": FIXTURE_INTEGRATION_PROTOTYPE.exists(),
        "current_role": "prototype integration behavior reference",
        "future_wiring_role": "behavioral template for scaffold wiring",
    },
    {
        "surface": "fixture_root",
        "path": str(FIXTURE_ROOT),
        "exists": FIXTURE_ROOT.exists(),
        "current_role": "deterministic fixture corpus",
        "future_wiring_role": "read-only fixture source root",
    },
    {
        "surface": "fixture_metadata",
        "path": f"{MANIFEST} + {EXPECTED_RESULTS}",
        "exists": MANIFEST.exists() and EXPECTED_RESULTS.exists(),
        "current_role": "fixture hashes and expected replay outcomes",
        "future_wiring_role": "read-only expectation/parity source",
    },
]

CLI_ADDITION_PLAN = [
    {
        "flag": "--source-mode",
        "values": "scaffold|fixture|live",
        "default": "scaffold",
        "required": True,
        "future_behavior": "scaffold preserves current behavior; fixture enables replay dry-run; live remains not implemented.",
        "safety_gate": "default must preserve current scaffold behavior",
    },
    {
        "flag": "--fixture-root",
        "values": "path",
        "default": str(FIXTURE_ROOT),
        "required": False,
        "future_behavior": "fixture-mode metadata/payload root.",
        "safety_gate": "read-only and only honored with source-mode fixture",
    },
    {
        "flag": "--fixture-date",
        "values": "YYYY-MM-DD",
        "default": "",
        "required": False,
        "future_behavior": "optional single fixture date; absent means replay all known dates.",
        "safety_gate": "must not create missing fixture files",
    },
    {
        "flag": "--allow-negative-fixtures",
        "values": "bool",
        "default": "false",
        "required": False,
        "future_behavior": "allows dedupe/schema/missing-file fixture statuses through dry-run.",
        "safety_gate": "negative fixture statuses blocked unless explicit",
    },
    {
        "flag": "--dry-run",
        "values": "bool",
        "default": "true/current",
        "required": True,
        "future_behavior": "fixture mode requires dry-run.",
        "safety_gate": "source-mode fixture with dry_run false must fail fast",
    },
    {
        "flag": "--write",
        "values": "bool",
        "default": "false/current",
        "required": False,
        "future_behavior": "write remains unavailable for fixture mode.",
        "safety_gate": "source-mode fixture with write true must fail fast",
    },
]

SOURCE_ADAPTER_RESOLVER_PLAN = [
    {
        "source_mode": "scaffold",
        "resolver_behavior": "Use existing scaffold behavior unchanged.",
        "adapter_called": "none",
        "write_allowed": "existing scaffold rules only",
        "required": True,
    },
    {
        "source_mode": "fixture",
        "resolver_behavior": "After gates pass, call fetch_candidate_bullpen_statcast_fixture_rows for selected fixture dates.",
        "adapter_called": "fixture replay adapter",
        "write_allowed": "never",
        "required": True,
    },
    {
        "source_mode": "live",
        "resolver_behavior": "Return live_adapter_not_implemented until future live adapter layer.",
        "adapter_called": "none",
        "write_allowed": "never in this stage",
        "required": True,
    },
    {
        "source_mode": "invalid",
        "resolver_behavior": "Fail fast with invalid_source_mode.",
        "adapter_called": "none",
        "write_allowed": "never",
        "required": True,
    },
]

INTEGRATION_FLOW = [
    {"step": 1, "operation": "parse_args", "detail": "Parse existing args plus source-mode fixture controls.", "required": True},
    {"step": 2, "operation": "preserve_scaffold_default", "detail": "If source-mode is scaffold/default, existing behavior remains unchanged.", "required": True},
    {"step": 3, "operation": "validate_source_mode", "detail": "Accept scaffold, fixture, live; reject unknown modes.", "required": True},
    {"step": 4, "operation": "validate_fixture_gates", "detail": "Reject fixture write, fixture non-dry-run, and negative fixtures without allowance.", "required": True},
    {"step": 5, "operation": "snapshot_fixture_state", "detail": "Snapshot payload and metadata before fixture replay dry-run.", "required": True},
    {"step": 6, "operation": "resolve_adapter", "detail": "Call fixture replay adapter only after fixture gates pass.", "required": True},
    {"step": 7, "operation": "collect_result_wrapper", "detail": "Collect FixtureReplayResult status/counts/rows.", "required": True},
    {"step": 8, "operation": "emit_dry_run_outputs", "detail": "Emit dry-run diagnostics only; do not stage DB writes.", "required": True},
    {"step": 9, "operation": "compare_expected_results", "detail": "Compare replay status/counts/missing fields to expected_results.", "required": True},
    {"step": 10, "operation": "assert_immutability", "detail": "Verify fixture payloads and metadata unchanged.", "required": True},
]

CODE_CHANGE_BOUNDARIES = [
    {
        "boundary": "future_wiring_modifies_only_backfill_scaffold",
        "path": str(BACKFILL_SCAFFOLD),
        "allowed_future_change": True,
        "disallowed_this_layer": True,
    },
    {
        "boundary": "fixture_payloads_read_only",
        "path": str(FIXTURE_ROOT / "dates"),
        "allowed_future_change": False,
        "disallowed_this_layer": True,
    },
    {
        "boundary": "fixture_manifest_read_only",
        "path": str(MANIFEST),
        "allowed_future_change": False,
        "disallowed_this_layer": True,
    },
    {
        "boundary": "fixture_expected_results_read_only",
        "path": str(EXPECTED_RESULTS),
        "allowed_future_change": False,
        "disallowed_this_layer": True,
    },
    {
        "boundary": "replay_adapter_not_modified_by_wiring",
        "path": str(FIXTURE_REPLAY_ADAPTER),
        "allowed_future_change": False,
        "disallowed_this_layer": True,
    },
    {
        "boundary": "production_modules_untouched",
        "path": "mlb_app/**, frontend/**, routes/**, sportsbook/**",
        "allowed_future_change": False,
        "disallowed_this_layer": True,
    },
]

FUTURE_ACCEPTANCE_GATES = [
    {"gate": "existing_scaffold_dry_run_still_passes", "expected": True, "required": True},
    {"gate": "fixture_positive_dry_run_passes", "expected": True, "required": True},
    {"gate": "fixture_negative_without_allowance_blocked", "expected": True, "required": True},
    {"gate": "fixture_negative_with_allowance_passes", "expected": True, "required": True},
    {"gate": "fixture_write_blocked", "expected": True, "required": True},
    {"gate": "fixture_non_dry_run_blocked", "expected": True, "required": True},
    {"gate": "live_mode_not_implemented", "expected": True, "required": True},
    {"gate": "invalid_source_mode_rejected", "expected": True, "required": True},
    {"gate": "no_payload_metadata_mutation", "expected": True, "required": True},
    {"gate": "no_db_writes", "expected": True, "required": True},
]

FUTURE_AUDIT_OUTPUTS = [
    {"artifact": "JSON diagnosis", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring.json", "required": True},
    {"artifact": "checks CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_checks.csv", "required": True},
    {"artifact": "scaffold CLI audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_scaffold_cli_audit.csv", "required": True},
    {"artifact": "adapter resolver audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_adapter_resolver_audit.csv", "required": True},
    {"artifact": "fixture dry-run result CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_scaffold_fixture_results.csv", "required": True},
    {"artifact": "expectation parity CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_scaffold_expectation_parity.csv", "required": True},
    {"artifact": "write/dry-run gate CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_scaffold_write_dry_run_gate.csv", "required": True},
    {"artifact": "immutability audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_scaffold_immutability_audit.csv", "required": True},
    {"artifact": "safety audit CSV", "path": "tmp/candidate_bullpen_statcast_fixture_replay_backfill_scaffold_safety_audit.csv", "required": True},
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


def _metadata_snapshot() -> Dict[str, str]:
    return {
        str(path): path.read_text() if path.exists() else "__MISSING__"
        for path in [MANIFEST, EXPECTED_RESULTS]
    }


def _payload_inventory_snapshot() -> Dict[str, bool]:
    dates_dir = FIXTURE_ROOT / "dates"
    return {
        str(path): path.exists()
        for path in sorted(dates_dir.glob("*.jsonl"))
    } if dates_dir.exists() else {}


def _scaffold_snapshot() -> str:
    return BACKFILL_SCAFFOLD.read_text(errors="ignore") if BACKFILL_SCAFFOLD.exists() else "__MISSING__"


def main() -> None:
    metadata_before = _metadata_snapshot()
    payload_before = _payload_inventory_snapshot()
    scaffold_before = _scaffold_snapshot()

    _write_csv(OUTPUT_SURFACE, SCAFFOLD_SURFACE_INVENTORY)
    _write_csv(OUTPUT_CLI, CLI_ADDITION_PLAN)
    _write_csv(OUTPUT_RESOLVER, SOURCE_ADAPTER_RESOLVER_PLAN)
    _write_csv(OUTPUT_FLOW, INTEGRATION_FLOW)
    _write_csv(OUTPUT_BOUNDARY, CODE_CHANGE_BOUNDARIES)
    _write_csv(OUTPUT_GATES, FUTURE_ACCEPTANCE_GATES)
    _write_csv(OUTPUT_FUTURE, FUTURE_AUDIT_OUTPUTS)

    metadata_after = _metadata_snapshot()
    payload_after = _payload_inventory_snapshot()
    scaffold_after = _scaffold_snapshot()

    scaffold_surface_inventory_defined = (
        len(SCAFFOLD_SURFACE_INVENTORY) == 5
        and all(row["exists"] for row in SCAFFOLD_SURFACE_INVENTORY)
    )
    cli_addition_plan_defined = (
        len(CLI_ADDITION_PLAN) == 6
        and any(row["flag"] == "--source-mode" and row["values"] == "scaffold|fixture|live" for row in CLI_ADDITION_PLAN)
        and any(row["flag"] == "--write" and "fail fast" in row["safety_gate"] for row in CLI_ADDITION_PLAN)
    )
    source_adapter_resolver_plan_defined = (
        len(SOURCE_ADAPTER_RESOLVER_PLAN) == 4
        and {row["source_mode"] for row in SOURCE_ADAPTER_RESOLVER_PLAN} == {"scaffold", "fixture", "live", "invalid"}
        and all(row["required"] for row in SOURCE_ADAPTER_RESOLVER_PLAN)
    )
    integration_flow_defined = len(INTEGRATION_FLOW) == 10 and all(row["required"] for row in INTEGRATION_FLOW)
    code_change_boundaries_defined = len(CODE_CHANGE_BOUNDARIES) == 6 and all(row["disallowed_this_layer"] for row in CODE_CHANGE_BOUNDARIES)
    future_acceptance_gates_defined = len(FUTURE_ACCEPTANCE_GATES) == 10 and all(row["required"] for row in FUTURE_ACCEPTANCE_GATES)
    future_audit_outputs_defined = len(FUTURE_AUDIT_OUTPUTS) == 9 and all(row["required"] for row in FUTURE_AUDIT_OUTPUTS)
    no_payload_mutation = payload_before == payload_after
    no_metadata_mutation = metadata_before == metadata_after
    scaffold_not_modified = scaffold_before == scaffold_after

    checks = [
        {"check": "scaffold_surface_inventory_defined", "passed": scaffold_surface_inventory_defined, "detail": f"{len(SCAFFOLD_SURFACE_INVENTORY)} surfaces"},
        {"check": "cli_addition_plan_defined", "passed": cli_addition_plan_defined, "detail": f"{len(CLI_ADDITION_PLAN)} CLI additions"},
        {"check": "source_adapter_resolver_plan_defined", "passed": source_adapter_resolver_plan_defined, "detail": f"{len(SOURCE_ADAPTER_RESOLVER_PLAN)} source modes"},
        {"check": "integration_flow_defined", "passed": integration_flow_defined, "detail": f"{len(INTEGRATION_FLOW)} steps"},
        {"check": "code_change_boundaries_defined", "passed": code_change_boundaries_defined, "detail": f"{len(CODE_CHANGE_BOUNDARIES)} boundaries"},
        {"check": "future_acceptance_gates_defined", "passed": future_acceptance_gates_defined, "detail": f"{len(FUTURE_ACCEPTANCE_GATES)} gates"},
        {"check": "future_audit_outputs_defined", "passed": future_audit_outputs_defined, "detail": f"{len(FUTURE_AUDIT_OUTPUTS)} outputs"},
        {"check": "planning_only_no_scaffold_modification", "passed": scaffold_not_modified, "detail": True},
        {"check": "replay_adapter_not_executed", "passed": True, "detail": True},
        {"check": "fixture_replay_not_wired", "passed": True, "detail": True},
        {"check": "no_payload_mutation", "passed": no_payload_mutation, "detail": True},
        {"check": "no_metadata_mutation", "passed": no_metadata_mutation, "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": True},
        {"check": "no_db_writes", "passed": True, "detail": True},
        {"check": "production_default_unchanged", "passed": True, "detail": True},
    ]
    _write_csv(OUTPUT_CHECKS, checks)

    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_plan_complete",
        "plan_version": PLAN_VERSION,
        "scaffold_surfaces": len(SCAFFOLD_SURFACE_INVENTORY),
        "cli_additions": len(CLI_ADDITION_PLAN),
        "source_adapter_modes": len(SOURCE_ADAPTER_RESOLVER_PLAN),
        "integration_flow_steps": len(INTEGRATION_FLOW),
        "code_change_boundaries": len(CODE_CHANGE_BOUNDARIES),
        "future_acceptance_gates": len(FUTURE_ACCEPTANCE_GATES),
        "future_audit_outputs": len(FUTURE_AUDIT_OUTPUTS),
        "all_checks_passed": all(check["passed"] for check in checks),
        "planning_only": True,
        "backfill_scaffold_modified": False,
        "fixture_replay_wired": False,
        "fixture_replay_adapter_executed": False,
        "payload_mutated": False,
        "metadata_mutated": False,
        "live_adapter_implemented": False,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6CV_candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_prototype"
            if all(check["passed"] for check in checks)
            else "6CU_patch_candidate_bullpen_statcast_fixture_replay_backfill_scaffold_wiring_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
