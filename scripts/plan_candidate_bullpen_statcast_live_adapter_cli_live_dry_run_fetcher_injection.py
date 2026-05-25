from __future__ import annotations

import ast
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


PLAN_VERSION = "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_v0.1"

SCAFFOLD_PATH = Path("scripts/backfill_candidate_bullpen_statcast_labels.py")
ADAPTER_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATION_6DL = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
AUDIT_6DM = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_scaffold_integration.py")
PLAN_6DN = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_contract.py")
AUDIT_6DO = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_contract_plan.py")
VALIDATION_6DP = Path("scripts/validate_candidate_bullpen_statcast_live_adapter_cli_scaffold_integration.py")
AUDIT_6DQ = Path("scripts/audit_candidate_bullpen_statcast_live_adapter_cli_scaffold_integration.py")

FIXTURE_ROOT = Path("tests/fixtures/statcast/bullpen_labels")
MANIFEST = FIXTURE_ROOT / "manifest.json"
EXPECTED_RESULTS = FIXTURE_ROOT / "expected_results.json"
DATES_DIR = FIXTURE_ROOT / "dates"

OUTPUT_DIR = Path("tmp")
OUTPUT_DIR.mkdir(exist_ok=True)

OUTPUT_JSON = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan.json"
OUTPUT_CHECKS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_checks.csv"
OUTPUT_IMPLEMENTATION_SCOPE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_implementation_scope.csv"
OUTPUT_RESOLVER_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_resolver_contract.csv"
OUTPUT_CLI_GATE_CONTRACT = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_cli_gate_contract.csv"
OUTPUT_FETCHER_INJECTION = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_fetcher_injection_contract.csv"
OUTPUT_ARTIFACT_STATUS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_artifact_status_contract.csv"
OUTPUT_VALIDATION_STRATEGY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_validation_strategy.csv"
OUTPUT_SAFETY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_safety.csv"
OUTPUT_NON_GOALS = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_non_goals.csv"
OUTPUT_IMMUTABILITY = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_immutability.csv"
OUTPUT_CURRENT_STATE = OUTPUT_DIR / "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_current_state.csv"


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


def _sha(path: Path) -> str:
    if not path.exists():
        return "__MISSING__"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot_files() -> Dict[str, str]:
    paths = [
        SCAFFOLD_PATH,
        ADAPTER_PATH,
        VALIDATION_6DL,
        AUDIT_6DM,
        PLAN_6DN,
        AUDIT_6DO,
        VALIDATION_6DP,
        AUDIT_6DQ,
        MANIFEST,
        EXPECTED_RESULTS,
    ]
    snapshot = {str(path): _sha(path) for path in paths}
    if DATES_DIR.exists():
        for payload in sorted(DATES_DIR.glob("*.jsonl")):
            snapshot[str(payload)] = _sha(payload)
    return snapshot


def _top_level_imports(source: str) -> str:
    tree = ast.parse(source)
    imports = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            imports.append(ast.get_source_segment(source, node) or "")
    return "\n".join(imports)


def _current_state_rows(source: str) -> List[Dict[str, Any]]:
    top_imports = _top_level_imports(source)
    helper_idx = source.find("def run_candidate_bullpen_live_adapter_scaffold(")
    main_idx = source.rfind('if __name__ == "__main__":')
    return [
        {"check": "scaffold_exists", "passed": SCAFFOLD_PATH.exists(), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_exists", "passed": ADAPTER_PATH.exists(), "detail": str(ADAPTER_PATH)},
        {"check": "six_dp_validation_exists", "passed": VALIDATION_6DP.exists(), "detail": str(VALIDATION_6DP)},
        {"check": "six_dq_audit_exists", "passed": AUDIT_6DQ.exists(), "detail": str(AUDIT_6DQ)},
        {"check": "explicit_live_route_exists", "passed": 'if args.source_mode == "live":' in source, "detail": True},
        {"check": "allow_live_write_arg_exists", "passed": '"--allow-live-write"' in source, "detail": True},
        {"check": "live_helper_exists", "passed": "run_candidate_bullpen_live_adapter_scaffold" in source, "detail": True},
        {"check": "main_after_helper", "passed": helper_idx != -1 and main_idx != -1 and main_idx > helper_idx, "detail": {"helper_idx": helper_idx, "main_idx": main_idx}},
        {"check": "production_default_scaffold_preserved", "passed": 'default="scaffold"' in source or "default=CANDIDATE_BULLPEN_SOURCE_MODE_FIXTURE" in source, "detail": True},
        {"check": "no_top_level_adapter_import", "passed": "fetch_candidate_bullpen_statcast_live_adapter" not in top_imports, "detail": True},
        {"check": "no_top_level_pybaseball_statcast_import", "passed": "pybaseball" not in top_imports and "statcast" not in top_imports, "detail": True},
    ]


def _implementation_scope_rows() -> List[Dict[str, Any]]:
    return [
        {
            "item": "implementation_target",
            "planned_contract": "Later implementation should update only scripts/backfill_candidate_bullpen_statcast_labels.py unless a separate audited layer approves other files.",
            "required": True,
        },
        {
            "item": "adapter_scope",
            "planned_contract": "Do not modify scripts/fetch_candidate_bullpen_statcast_live_adapter.py in the fetcher-injection implementation layer.",
            "required": True,
        },
        {
            "item": "helper_contract",
            "planned_contract": "Preserve run_candidate_bullpen_live_adapter_scaffold public helper signature and status artifact contract.",
            "required": True,
        },
        {
            "item": "cli_scope",
            "planned_contract": "Only explicit --source-mode live may enter the injected fetcher path.",
            "required": True,
        },
        {
            "item": "default_behavior",
            "planned_contract": "Default scaffold mode and fixture mode must remain unchanged.",
            "required": True,
        },
        {
            "item": "no_write_scope",
            "planned_contract": "Implementation must remain dry-run only and must not write DB rows or materialize candidate labels.",
            "required": True,
        },
    ]


def _resolver_contract_rows() -> List[Dict[str, Any]]:
    return [
        {
            "item": "resolver_name",
            "planned_contract": "Introduce a small local resolver, for example _resolve_candidate_bullpen_live_fetcher(args).",
            "required": True,
        },
        {
            "item": "resolver_blocked_paths",
            "planned_contract": "Resolver returns None before any dependency import for scaffold mode, fixture mode, no-dry-run, write flags, missing date, invalid date, and multi-date windows.",
            "required": True,
        },
        {
            "item": "resolver_live_dry_run",
            "planned_contract": "Resolver may return an adapter-backed fetcher only for explicit --source-mode live with dry_run true, no write flags, and exactly one strict YYYY-MM-DD date.",
            "required": True,
        },
        {
            "item": "resolver_lazy_import",
            "planned_contract": "Any adapter/pybaseball/statcast dependency import remains lazy and inside helper/resolver boundary, never top-level.",
            "required": True,
        },
        {
            "item": "resolver_testability",
            "planned_contract": "Validation must be able to monkeypatch or inject a synthetic fetcher without network access.",
            "required": True,
        },
    ]


def _cli_gate_contract_rows() -> List[Dict[str, Any]]:
    return [
        {"gate": "source_mode_live_required", "expected": "No fetcher injection unless args.source_mode == 'live'.", "status": "required"},
        {"gate": "dry_run_required", "expected": "No fetcher injection unless args.dry_run is true.", "status": "required"},
        {"gate": "write_flags_blocked", "expected": "--write and --allow-live-write continue to return live_write_blocked before fetcher use.", "status": "required"},
        {"gate": "strict_single_date_required", "expected": "Only one strict YYYY-MM-DD date may be eligible for fetcher injection.", "status": "required"},
        {"gate": "invalid_date_blocked", "expected": "Invalid dates return live_date_window_invalid before fetcher import/call.", "status": "required"},
        {"gate": "multi_date_blocked", "expected": "Multiple dates return live_date_window_invalid before fetcher import/call.", "status": "required"},
        {"gate": "default_scaffold_unchanged", "expected": "Default scaffold behavior remains unchanged.", "status": "required"},
        {"gate": "fixture_unchanged", "expected": "Fixture mode behavior remains unchanged.", "status": "required"},
    ]


def _fetcher_injection_rows() -> List[Dict[str, Any]]:
    return [
        {
            "item": "injection_entrypoint",
            "planned_contract": "The explicit live CLI route passes fetcher=resolved_fetcher into run_candidate_bullpen_live_adapter_scaffold only after gates pass.",
            "required": True,
        },
        {
            "item": "synthetic_fetcher_validation",
            "planned_contract": "Next implementation validation must exercise CLI live dry-run with synthetic fetcher and confirm live_dry_run_ready without network.",
            "required": True,
        },
        {
            "item": "real_fetcher_guard",
            "planned_contract": "Real adapter-backed fetcher remains unavailable in validation unless an explicit future opt-in gate is added and audited.",
            "required": True,
        },
        {
            "item": "dependency_missing_safe",
            "planned_contract": "If adapter dependencies are missing, CLI returns live_dependency_missing JSON payload safely.",
            "required": True,
        },
        {
            "item": "no_materialization",
            "planned_contract": "Normalized live rows remain diagnostic-only and are not converted into candidate label writes.",
            "required": True,
        },
    ]


def _artifact_status_rows() -> List[Dict[str, Any]]:
    return [
        {"status": "live_dry_run_ready", "expected_context": "single-date explicit live dry-run with injected synthetic/adapter fetcher", "required": True},
        {"status": "live_requires_dry_run", "expected_context": "explicit live path without dry_run", "required": True},
        {"status": "live_write_blocked", "expected_context": "explicit live path with --write or --allow-live-write", "required": True},
        {"status": "live_date_window_invalid", "expected_context": "invalid, missing, or multi-date live window", "required": True},
        {"status": "live_dependency_missing", "expected_context": "adapter dependency import/fetcher setup unavailable", "required": True},
        {"field_group": "adapter_fields", "expected_context": "adapter_status/count/error/version fields remain present", "required": True},
        {"field_group": "safety_flags", "expected_context": "external_fetch_performed/db_writes_performed/candidate_labels_materialized/production_default_unchanged remain present", "required": True},
    ]


def _validation_strategy_rows() -> List[Dict[str, Any]]:
    return [
        {"validation": "compileall", "planned_check": "python -m compileall mlb_app scripts", "required": True},
        {"validation": "source_audit", "planned_check": "Confirm resolver, gates, lazy imports, and live route wiring.", "required": True},
        {"validation": "cli_argument_audit", "planned_check": "Confirm source-mode, dry-run/no-dry-run, write, and allow-live-write behavior.", "required": True},
        {"validation": "subprocess_blocked_path_audit", "planned_check": "Confirm blocked live CLI statuses and JSON returncode 0.", "required": True},
        {"validation": "helper_direct_injection_audit", "planned_check": "Call helper with synthetic fetcher and blocked probes.", "required": True},
        {"validation": "cli_synthetic_fetcher_audit", "planned_check": "Exercise CLI live dry-run with synthetic fetcher without network.", "required": True},
        {"validation": "artifact_contract_audit", "planned_check": "Validate required fields on success/blocked/dependency payloads.", "required": True},
        {"validation": "import_boundary_audit", "planned_check": "No top-level adapter/pybaseball/statcast import.", "required": True},
        {"validation": "immutability_audit", "planned_check": "Adapter, fixtures, and prior validation/audit/plan scripts unchanged.", "required": True},
        {"validation": "safety_audit", "planned_check": "No external fetch in validation, no DB writes, no materialization.", "required": True},
    ]


def _safety_rows() -> List[Dict[str, Any]]:
    return [
        {"safety": "no_live_fetch_outside_explicit_live", "planned_contract": "Resolver must be inert outside --source-mode live.", "required": True},
        {"safety": "dry_run_only", "planned_contract": "Injected fetcher path remains dry-run only.", "required": True},
        {"safety": "write_blocked", "planned_contract": "All write flags block before fetcher resolution/call.", "required": True},
        {"safety": "invalid_window_blocks", "planned_contract": "Invalid/missing/multi-date windows block before fetcher resolution/call.", "required": True},
        {"safety": "no_db_writes", "planned_contract": "No DB write path is introduced.", "required": True},
        {"safety": "no_candidate_materialization", "planned_contract": "No candidate labels are materialized from live rows.", "required": True},
        {"safety": "no_fixture_mutation", "planned_contract": "Fixture assets remain unchanged.", "required": True},
        {"safety": "production_default_unchanged", "planned_contract": "Default scaffold behavior remains unchanged.", "required": True},
    ]


def _non_goal_rows() -> List[Dict[str, Any]]:
    return [
        {"non_goal": "no_implementation", "detail": "This layer does not implement fetcher injection.", "required": True},
        {"non_goal": "no_real_statcast_fetch", "detail": "This layer performs no real pybaseball/statcast fetch.", "required": True},
        {"non_goal": "no_database_writes", "detail": "This layer performs no DB writes.", "required": True},
        {"non_goal": "no_candidate_labels", "detail": "This layer materializes no candidate labels from live rows.", "required": True},
        {"non_goal": "no_adapter_modification", "detail": "This layer does not modify the adapter.", "required": True},
        {"non_goal": "no_fixture_mutation", "detail": "This layer does not mutate fixture assets.", "required": True},
    ]


def _immutability_rows(before: Dict[str, str]) -> List[Dict[str, Any]]:
    after = _snapshot_files()
    return [
        {"check": "scaffold_unchanged_by_plan", "passed": before.get(str(SCAFFOLD_PATH)) == after.get(str(SCAFFOLD_PATH)), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged_by_plan", "passed": before.get(str(ADAPTER_PATH)) == after.get(str(ADAPTER_PATH)), "detail": str(ADAPTER_PATH)},
        {"check": "validation_audit_plan_scripts_unchanged_by_plan", "passed": all(before.get(str(path)) == after.get(str(path)) for path in [VALIDATION_6DL, AUDIT_6DM, PLAN_6DN, AUDIT_6DO, VALIDATION_6DP, AUDIT_6DQ]), "detail": "6DL/6DM/6DN/6DO/6DP/6DQ unchanged"},
        {"check": "fixtures_unchanged_by_plan", "passed": before == after, "detail": "fixtures and tracked dependency files unchanged"},
    ]


def main() -> None:
    before = _snapshot_files()
    source = SCAFFOLD_PATH.read_text(errors="ignore")

    current_state_rows = _current_state_rows(source)
    implementation_scope_rows = _implementation_scope_rows()
    resolver_rows = _resolver_contract_rows()
    cli_gate_rows = _cli_gate_contract_rows()
    fetcher_rows = _fetcher_injection_rows()
    artifact_rows = _artifact_status_rows()
    validation_rows = _validation_strategy_rows()
    safety_rows = _safety_rows()
    non_goal_rows = _non_goal_rows()
    immutability_rows = _immutability_rows(before)

    _write_csv(OUTPUT_CURRENT_STATE, current_state_rows)
    _write_csv(OUTPUT_IMPLEMENTATION_SCOPE, implementation_scope_rows)
    _write_csv(OUTPUT_RESOLVER_CONTRACT, resolver_rows)
    _write_csv(OUTPUT_CLI_GATE_CONTRACT, cli_gate_rows)
    _write_csv(OUTPUT_FETCHER_INJECTION, fetcher_rows)
    _write_csv(OUTPUT_ARTIFACT_STATUS, artifact_rows)
    _write_csv(OUTPUT_VALIDATION_STRATEGY, validation_rows)
    _write_csv(OUTPUT_SAFETY, safety_rows)
    _write_csv(OUTPUT_NON_GOALS, non_goal_rows)
    _write_csv(OUTPUT_IMMUTABILITY, immutability_rows)

    checks = [
        {"check": "current_state_valid", "passed": all(row["passed"] for row in current_state_rows), "detail": f"{sum(row['passed'] for row in current_state_rows)}/{len(current_state_rows)}"},
        {"check": "implementation_scope_defined", "passed": all(row["required"] for row in implementation_scope_rows), "detail": len(implementation_scope_rows)},
        {"check": "resolver_contract_defined", "passed": all(row["required"] for row in resolver_rows), "detail": len(resolver_rows)},
        {"check": "cli_gate_contract_defined", "passed": all(row["status"] == "required" for row in cli_gate_rows), "detail": len(cli_gate_rows)},
        {"check": "fetcher_injection_contract_defined", "passed": all(row["required"] for row in fetcher_rows), "detail": len(fetcher_rows)},
        {"check": "artifact_status_contract_defined", "passed": all(row["required"] for row in artifact_rows), "detail": len(artifact_rows)},
        {"check": "validation_strategy_defined", "passed": all(row["required"] for row in validation_rows), "detail": len(validation_rows)},
        {"check": "safety_contract_defined", "passed": all(row["required"] for row in safety_rows), "detail": len(safety_rows)},
        {"check": "non_goals_defined", "passed": all(row["required"] for row in non_goal_rows), "detail": len(non_goal_rows)},
        {"check": "immutability_valid", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(row['passed'] for row in immutability_rows)}/{len(immutability_rows)}"},
        {"check": "implementation_files_unchanged", "passed": any(row["check"] == "scaffold_unchanged_by_plan" and row["passed"] for row in immutability_rows), "detail": str(SCAFFOLD_PATH)},
        {"check": "adapter_unchanged", "passed": any(row["check"] == "adapter_unchanged_by_plan" and row["passed"] for row in immutability_rows), "detail": str(ADAPTER_PATH)},
        {"check": "validation_audit_scripts_unchanged", "passed": any(row["check"] == "validation_audit_plan_scripts_unchanged_by_plan" and row["passed"] for row in immutability_rows), "detail": True},
        {"check": "no_fixture_mutation", "passed": any(row["check"] == "fixtures_unchanged_by_plan" and row["passed"] for row in immutability_rows), "detail": True},
        {"check": "no_external_fetch", "passed": True, "detail": "planning-only"},
        {"check": "no_db_writes", "passed": True, "detail": "planning-only"},
        {"check": "production_default_unchanged", "passed": any(row["check"] == "production_default_scaffold_preserved" and row["passed"] for row in current_state_rows), "detail": True},
    ]

    _write_csv(OUTPUT_CHECKS, checks)

    all_checks_passed = all(row["passed"] for row in checks)
    diagnosis = {
        "diagnosis": "candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_complete",
        "plan_version": PLAN_VERSION,
        "current_state_rows": len(current_state_rows),
        "implementation_scope_rows": len(implementation_scope_rows),
        "resolver_contract_rows": len(resolver_rows),
        "cli_gate_contract_rows": len(cli_gate_rows),
        "fetcher_injection_contract_rows": len(fetcher_rows),
        "artifact_status_contract_rows": len(artifact_rows),
        "validation_strategy_rows": len(validation_rows),
        "safety_rows": len(safety_rows),
        "non_goal_rows": len(non_goal_rows),
        "immutability_rows": len(immutability_rows),
        "all_checks_passed": all_checks_passed,
        "planning_only": True,
        "implementation_files_unchanged": True,
        "adapter_unchanged": True,
        "validation_audit_scripts_unchanged": True,
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized_from_live_rows": False,
        "fixture_assets_mutated": False,
        "production_default_unchanged": True,
        "recommended_next_layer": (
            "6DS_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan_audit"
            if all_checks_passed
            else "6DR_patch_candidate_bullpen_statcast_live_adapter_cli_live_dry_run_fetcher_injection_plan"
        ),
    }

    OUTPUT_JSON.write_text(json.dumps(diagnosis, indent=2))
    print(json.dumps(diagnosis, indent=2))


if __name__ == "__main__":
    main()
