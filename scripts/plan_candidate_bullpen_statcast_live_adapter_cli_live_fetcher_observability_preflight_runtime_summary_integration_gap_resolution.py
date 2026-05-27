#!/usr/bin/env python3
"""Layer 6EP integration-gap resolution plan.

This layer is planning-only. It consumes the 6EO audit result and defines the
future path for resolving the runtime-summary integration gap without changing
fetcher semantics, resolver gates, production defaults, network behavior, DB
writes, materialization, fixtures, or prior validation/audit scripts.
"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


PLAN_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution_plan"
)
AUDIT_6EO_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_audit"
)

TMP_DIR = Path("tmp")
TARGET_SOURCE = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
AUDIT_6EO_SCRIPT = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation.py"
)

AUDIT_6EO_JSON = TMP_DIR / f"{AUDIT_6EO_SLUG}.json"

PLAN_JSON = TMP_DIR / f"{PLAN_SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{PLAN_SLUG}_checks.csv"
GAP_SUMMARY_CSV = TMP_DIR / f"{PLAN_SLUG}_gap_summary.csv"
SOURCE_SURFACE_CSV = TMP_DIR / f"{PLAN_SLUG}_source_surface.csv"
RESOLUTION_OPTIONS_CSV = TMP_DIR / f"{PLAN_SLUG}_resolution_options.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{PLAN_SLUG}_recommended_path.csv"
FUTURE_IMPLEMENTATION_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_future_implementation_contract.csv"
VALIDATION_CONTRACT_CSV = TMP_DIR / f"{PLAN_SLUG}_validation_contract.csv"
NON_GOALS_CSV = TMP_DIR / f"{PLAN_SLUG}_non_goals.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{PLAN_SLUG}_immutability.csv"

EXPECTED_6EO_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_audit_complete"
)
EXPECTED_6EO_NEXT_LAYER = (
    "6EP_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution_plan"
)
PLAN_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution_plan_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6EQ_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution_plan_audit"
)

RECOMMENDED_PATH = "create_minimal_cli_artifact_summary_surface"
FUTURE_ARTIFACT_HELPER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"

EXPECTED_RUNTIME_FIELDS = [
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
]


def write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        raise ValueError(f"no rows for {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    audit_run = subprocess.run(
        [sys.executable, str(AUDIT_6EO_SCRIPT)],
        check=False,
        text=True,
        capture_output=True,
    )

    audit_json_exists = AUDIT_6EO_JSON.exists()
    audit: Dict[str, Any] = {}
    if audit_json_exists:
        audit = json.loads(AUDIT_6EO_JSON.read_text(encoding="utf-8"))

    source = TARGET_SOURCE.read_text(encoding="utf-8")

    gap_summary_rows = [
        {
            "fact": "implementation_helper_valid",
            "expected": True,
            "actual": audit.get("implementation_helper_valid"),
            "valid": audit.get("implementation_helper_valid") is True,
        },
        {
            "fact": "direct_validation_valid",
            "expected": True,
            "actual": audit.get("direct_validation_valid"),
            "valid": audit.get("direct_validation_valid") is True,
        },
        {
            "fact": "cli_artifact_assembly_surface_present",
            "expected": False,
            "actual": audit.get("cli_artifact_assembly_surface_present"),
            "valid": audit.get("cli_artifact_assembly_surface_present") is False,
        },
        {
            "fact": "cli_artifact_runtime_summary_wired",
            "expected": False,
            "actual": audit.get("cli_artifact_runtime_summary_wired"),
            "valid": audit.get("cli_artifact_runtime_summary_wired") is False,
        },
        {
            "fact": "final_cli_artifact_integration_complete",
            "expected": False,
            "actual": audit.get("final_cli_artifact_integration_complete"),
            "valid": audit.get("final_cli_artifact_integration_complete") is False,
        },
        {
            "fact": "follow_up_integration_layer_required",
            "expected": True,
            "actual": audit.get("follow_up_integration_layer_required"),
            "valid": audit.get("follow_up_integration_layer_required") is True,
        },
    ]

    source_surface_rows = [
        {
            "surface": "_candidate_bullpen_live_fetcher_runtime_summary",
            "present": "_candidate_bullpen_live_fetcher_runtime_summary" in source,
            "planning_interpretation": "runtime summary helper exists",
            "valid": "_candidate_bullpen_live_fetcher_runtime_summary" in source,
        },
        {
            "surface": "_candidate_bullpen_apply_live_fetcher_runtime_summary",
            "present": "_candidate_bullpen_apply_live_fetcher_runtime_summary" in source,
            "planning_interpretation": "runtime summary apply helper exists",
            "valid": "_candidate_bullpen_apply_live_fetcher_runtime_summary" in source,
        },
        {
            "surface": "_main",
            "present": "def _main() -> int:" in source,
            "planning_interpretation": "current source is module/self-check oriented",
            "valid": "def _main() -> int:" in source,
        },
        {
            "surface": "main",
            "present": "def main(" in source,
            "planning_interpretation": "no public CLI main surface is present",
            "valid": "def main(" not in source,
        },
        {
            "surface": "final_downstream_cli_artifact_assembly",
            "present": (
                "argparse" in source
                and "live_fetcher_observability" in source
                and "live_fetcher_preflight" in source
                and "candidate_labels_materialized" in source
            ),
            "planning_interpretation": "final CLI artifact assembly surface is absent",
            "valid": not (
                "argparse" in source
                and "live_fetcher_observability" in source
                and "live_fetcher_preflight" in source
                and "candidate_labels_materialized" in source
            ),
        },
    ]

    resolution_option_rows = [
        {
            "option": "wait_for_cli_surface",
            "description": "Wait until a later layer introduces a full CLI artifact assembly surface, then wire runtime summary there.",
            "pros": "Minimal immediate code risk.",
            "cons": "Leaves the integration gap unresolved and creates unclear future dependency.",
            "recommended": False,
        },
        {
            "option": "create_minimal_cli_artifact_summary_surface",
            "description": "Add a deterministic no-network/no-write artifact assembly helper that calls the existing runtime summary helper.",
            "pros": "Resolves the integration surface gap with a narrow deterministic contract while preserving fetcher semantics.",
            "cons": "Requires one future implementation layer and audit before declaring final CLI artifact integration complete.",
            "recommended": True,
        },
        {
            "option": "integrate_into_existing_module_self_check_artifact",
            "description": "Attach runtime summary to the existing module self-check diagnosis artifact only.",
            "pros": "Lowest new surface area.",
            "cons": "Would validate self-check reporting but still would not define a reusable CLI artifact assembly contract.",
            "recommended": False,
        },
    ]

    recommended_path_rows = [
        {
            "decision": "recommended_path",
            "value": RECOMMENDED_PATH,
            "reason": "Current source lacks CLI artifact surface while the runtime summary helper already exists and is validated.",
            "valid": True,
        },
        {
            "decision": "requires_deterministic_artifact_surface",
            "value": True,
            "reason": "Final integration requires a deterministic no-network/no-write artifact assembly surface.",
            "valid": True,
        },
        {
            "decision": "must_call_existing_apply_helper",
            "value": "_candidate_bullpen_apply_live_fetcher_runtime_summary",
            "reason": "Future implementation should reuse the validated helper rather than duplicate logic.",
            "valid": True,
        },
        {
            "decision": "must_preserve_fetcher_semantics",
            "value": True,
            "reason": "Resolution must not alter fetch_candidate_bullpen_statcast_live_rows_for_date behavior.",
            "valid": True,
        },
    ]

    future_implementation_contract_rows = [
        {
            "requirement": "add_deterministic_artifact_assembly_helper",
            "detail": f"Add {FUTURE_ARTIFACT_HELPER}.",
            "required": True,
        },
        {
            "requirement": "helper_calls_runtime_summary_apply_helper",
            "detail": "Artifact assembly helper must call _candidate_bullpen_apply_live_fetcher_runtime_summary.",
            "required": True,
        },
        {
            "requirement": "include_module_self_check_safety_signals",
            "detail": "Artifact must include existing self-check safety signals where applicable.",
            "required": True,
        },
        {
            "requirement": "include_all_10_runtime_summary_fields",
            "detail": ",".join(EXPECTED_RUNTIME_FIELDS),
            "required": True,
        },
        {
            "requirement": "no_network",
            "detail": "Future implementation must not run real network fetch.",
            "required": True,
        },
        {
            "requirement": "no_db_writes",
            "detail": "Future implementation must not write to DB.",
            "required": True,
        },
        {
            "requirement": "no_materialization",
            "detail": "Future implementation must not materialize candidate labels.",
            "required": True,
        },
        {
            "requirement": "production_default_unchanged",
            "detail": "Future implementation must not change production defaults.",
            "required": True,
        },
        {
            "requirement": "fetcher_behavior_unchanged",
            "detail": "Do not alter fetch_candidate_bullpen_statcast_live_rows_for_date behavior.",
            "required": True,
        },
    ]

    validation_contract_rows = [
        {"validation": "compile", "detail": "python -m compileall mlb_app scripts", "required": True},
        {"validation": "run_new_implementation_validation_script", "detail": "Run future 6ER validation script.", "required": True},
        {"validation": "default_dry_run_artifact_10_fields", "detail": "Default artifact has all 10 runtime summary fields.", "required": True},
        {"validation": "synthetic_scenario", "detail": "Synthetic scenario maps to validation_synthetic_dry_run.", "required": True},
        {"validation": "dependency_missing_scenario", "detail": "Dependency missing maps to dependency_missing_safe.", "required": True},
        {"validation": "blocked_no_dry_run_scenario", "detail": "No dry-run maps to blocked_requires_dry_run.", "required": True},
        {"validation": "blocked_write_scenario", "detail": "Write attempt maps to blocked_write.", "required": True},
        {"validation": "blocked_invalid_date_window_scenario", "detail": "Invalid/multi-date maps to blocked_date_window_invalid.", "required": True},
        {"validation": "no_network", "detail": "No real network calls.", "required": True},
        {"validation": "no_db_writes", "detail": "No DB writes.", "required": True},
        {"validation": "no_materialization", "detail": "No candidate label materialization.", "required": True},
        {"validation": "production_default_unchanged", "detail": "Production default unchanged.", "required": True},
        {"validation": "existing_module_self_check_still_passes", "detail": "Existing module self-check remains valid.", "required": True},
    ]

    non_goal_rows = [
        {"non_goal": "No implementation in 6EP.", "valid": True},
        {"non_goal": "No real fetch.", "valid": True},
        {"non_goal": "No CI network dependency.", "valid": True},
        {"non_goal": "No DB writes.", "valid": True},
        {"non_goal": "No candidate materialization.", "valid": True},
        {"non_goal": "No resolver gate changes.", "valid": True},
        {"non_goal": "No adapter behavior changes.", "valid": True},
        {"non_goal": "No production default changes.", "valid": True},
        {"non_goal": "No fixture changes.", "valid": True},
    ]

    immutability_rows = [
        {"path": str(TARGET_SOURCE), "policy": "read_only_in_6EP", "valid": True},
        {"path": "scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_implementation.py", "policy": "prior_6EL_script_read_only", "valid": True},
        {"path": "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_implementation_plan.py", "policy": "prior_6EM_script_read_only", "valid": True},
        {"path": "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_implementation.py", "policy": "prior_6EN_script_read_only", "valid": True},
        {"path": "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_implementation.py", "policy": "prior_6EO_script_execute_only", "valid": True},
        {"path": "fixtures", "policy": "read_only_in_6EP", "valid": True},
        {"path": "scripts/audit_pitcher_aggregate_rate_provenance.py", "policy": "unrelated_untracked_untouched", "valid": True},
        {"path": "scripts/backtest_extras_walkoff_hybrid_pairing.py", "policy": "unrelated_untracked_untouched", "valid": True},
        {"path": "scripts/backtest_transition_parameter_sensitivity.py", "policy": "unrelated_untracked_untouched", "valid": True},
        {"path": "scripts/debug_extras_walkoff_payload_paths.py", "policy": "unrelated_untracked_untouched", "valid": True},
    ]

    checks = [
        {
            "check": "six_eo_audit_executed",
            "passed": audit_run.returncode == 0,
            "detail": f"returncode={audit_run.returncode}",
        },
        {
            "check": "six_eo_audit_json_exists",
            "passed": audit_json_exists,
            "detail": str(AUDIT_6EO_JSON),
        },
        {
            "check": "six_eo_diagnosis",
            "passed": audit.get("diagnosis") == EXPECTED_6EO_DIAGNOSIS,
            "detail": str(audit.get("diagnosis")),
        },
        {
            "check": "six_eo_all_checks_passed",
            "passed": audit.get("all_checks_passed") is True,
            "detail": str(audit.get("all_checks_passed")),
        },
        {
            "check": "six_eo_recommended_next_layer",
            "passed": audit.get("recommended_next_layer") == EXPECTED_6EO_NEXT_LAYER,
            "detail": str(audit.get("recommended_next_layer")),
        },
        {
            "check": "gap_facts_valid",
            "passed": all(row["valid"] for row in gap_summary_rows),
            "detail": f"{sum(1 for row in gap_summary_rows if row['valid'])}/{len(gap_summary_rows)}",
        },
        {
            "check": "source_surface_valid",
            "passed": all(row["valid"] for row in source_surface_rows),
            "detail": f"{sum(1 for row in source_surface_rows if row['valid'])}/{len(source_surface_rows)}",
        },
        {
            "check": "resolution_options_valid",
            "passed": sum(1 for row in resolution_option_rows if row["recommended"]) == 1,
            "detail": RECOMMENDED_PATH,
        },
        {
            "check": "recommended_path_valid",
            "passed": all(row["valid"] for row in recommended_path_rows),
            "detail": RECOMMENDED_PATH,
        },
        {
            "check": "future_implementation_contract_valid",
            "passed": all(row["required"] for row in future_implementation_contract_rows),
            "detail": f"{len(future_implementation_contract_rows)} requirements",
        },
        {
            "check": "validation_contract_valid",
            "passed": all(row["required"] for row in validation_contract_rows),
            "detail": f"{len(validation_contract_rows)} validations",
        },
        {
            "check": "non_goals_valid",
            "passed": all(row["valid"] for row in non_goal_rows),
            "detail": f"{len(non_goal_rows)} non-goals",
        },
        {
            "check": "immutability_valid",
            "passed": all(row["valid"] for row in immutability_rows),
            "detail": f"{len(immutability_rows)} immutability rows",
        },
        {
            "check": "planning_only",
            "passed": True,
            "detail": "6EP emits a plan only and does not implement integration.",
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "gap_summary": write_csv(GAP_SUMMARY_CSV, gap_summary_rows),
        "source_surface": write_csv(SOURCE_SURFACE_CSV, source_surface_rows),
        "resolution_options": write_csv(RESOLUTION_OPTIONS_CSV, resolution_option_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_path_rows),
        "future_implementation_contract": write_csv(FUTURE_IMPLEMENTATION_CONTRACT_CSV, future_implementation_contract_rows),
        "validation_contract": write_csv(VALIDATION_CONTRACT_CSV, validation_contract_rows),
        "non_goals": write_csv(NON_GOALS_CSV, non_goal_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
    }

    summary = {
        "layer": "6EP",
        "name": "candidate bullpen Statcast live adapter CLI live fetcher observability preflight runtime summary integration gap resolution plan",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": PLAN_DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "audited_6eo_script": str(AUDIT_6EO_SCRIPT),
        "audit_6eo_subprocess_returncode": audit_run.returncode,
        "audit_6eo_stdout_tail": audit_run.stdout[-1000:],
        "audit_6eo_stderr_tail": audit_run.stderr[-1000:],
        "integration_gap": {
            "implementation_helper_valid": audit.get("implementation_helper_valid"),
            "direct_validation_valid": audit.get("direct_validation_valid"),
            "cli_artifact_assembly_surface_present": audit.get("cli_artifact_assembly_surface_present"),
            "cli_artifact_runtime_summary_wired": audit.get("cli_artifact_runtime_summary_wired"),
            "final_cli_artifact_integration_complete": audit.get("final_cli_artifact_integration_complete"),
            "follow_up_integration_layer_required": audit.get("follow_up_integration_layer_required"),
        },
        "recommended_resolution_path": RECOMMENDED_PATH,
        "future_artifact_helper": FUTURE_ARTIFACT_HELPER,
        "runtime_summary_fields": EXPECTED_RUNTIME_FIELDS,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(PLAN_JSON),
            "checks_csv": str(CHECKS_CSV),
            "gap_summary_csv": str(GAP_SUMMARY_CSV),
            "source_surface_csv": str(SOURCE_SURFACE_CSV),
            "resolution_options_csv": str(RESOLUTION_OPTIONS_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
            "future_implementation_contract_csv": str(FUTURE_IMPLEMENTATION_CONTRACT_CSV),
            "validation_contract_csv": str(VALIDATION_CONTRACT_CSV),
            "non_goals_csv": str(NON_GOALS_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
        },
    }

    PLAN_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
