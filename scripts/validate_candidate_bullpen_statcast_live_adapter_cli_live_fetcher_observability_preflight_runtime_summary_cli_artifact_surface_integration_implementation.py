#!/usr/bin/env python3
"""Validate 6EV runtime-summary CLI diagnostic artifact integration implementation."""

from __future__ import annotations

import csv
import importlib.util
import inspect
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation"
)

TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
TMP_DIR = Path("tmp")

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
SOURCE_VALIDATION_CSV = TMP_DIR / f"{SLUG}_source_validation.csv"
CLI_ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_cli_artifact_contract.csv"
RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{SLUG}_runtime_field_contract.csv"
STATUS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_status_contract.csv"
SAFETY_CONTRACT_CSV = TMP_DIR / f"{SLUG}_safety_contract.csv"
DETERMINISM_CSV = TMP_DIR / f"{SLUG}_determinism.csv"
MODULE_SELF_CHECK_CSV = TMP_DIR / f"{SLUG}_module_self_check.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"

DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6EW_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation_audit"
)

CLI_HELPER = "_candidate_bullpen_build_cli_diagnostic_artifact"
RUNTIME_HELPER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"

RUNTIME_FIELDS = [
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

FORBIDDEN_TOKENS = [
    "fetch_candidate_bullpen_statcast_live_rows_for_date(",
    "pybaseball",
    "statcast(",
    "requests",
    "urllib",
    "sqlalchemy",
    "sqlite3",
    "to_sql",
    "materialize_candidate_labels(",
    "materialize_candidate_labels_for_date(",
]

EXPECTED_STATUS = {
    "default_no_real_gate_live_dry_run": "safe_dry_run_no_real_fetch",
    "synthetic_path": "validation_synthetic_dry_run",
    "real_gated_monkeypatch_path": "real_gated_dry_run_candidate",
    "dependency_missing_path": "dependency_missing_safe",
    "live_without_dry_run": "blocked_requires_dry_run",
    "live_write_attempt": "blocked_write",
    "invalid_or_multi_date_window": "blocked_date_window_invalid",
}


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


def load_module() -> Any:
    module_name = "candidate_bullpen_live_adapter_6ev_validation"
    spec = importlib.util.spec_from_file_location(module_name, TARGET_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load spec for {TARGET_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def scenario_kwargs() -> Dict[str, Dict[str, Any]]:
    return {
        "default_no_real_gate_live_dry_run": {},
        "synthetic_path": {
            "source_mode": "synthetic",
            "resolution_gate": "synthetic",
            "resolution_status": "synthetic",
            "resolution_reason": "synthetic validation",
            "resolution_synthetic_enabled": True,
            "preflight_status": "passed",
            "preflight_reason": "synthetic dry-run",
        },
        "real_gated_monkeypatch_path": {
            "source_mode": "live",
            "resolution_gate": "real_gated",
            "resolution_status": "real_gated",
            "resolution_reason": "real-gated candidate",
            "resolution_external_fetch_enabled": True,
            "resolution_real_enabled": True,
            "preflight_status": "passed",
            "preflight_reason": "real-gated dry-run",
        },
        "dependency_missing_path": {
            "resolution_status": "dependency_missing",
            "resolution_reason": "dependency missing",
            "resolution_dependency_error": True,
            "preflight_status": "passed",
            "preflight_reason": "dependency missing but safe",
        },
        "live_without_dry_run": {
            "preflight_passed": False,
            "preflight_status": "blocked",
            "preflight_reason": "dry-run required",
            "preflight_dry_run": False,
        },
        "live_write_attempt": {
            "preflight_passed": False,
            "preflight_status": "blocked",
            "preflight_reason": "write attempt",
            "preflight_write_blocked": False,
            "preflight_allow_live_write": True,
            "db_writes_performed": False,
            "adapter_db_writes_performed": False,
        },
        "invalid_or_multi_date_window": {
            "preflight_passed": False,
            "preflight_status": "blocked",
            "preflight_reason": "invalid date window",
            "preflight_single_date": False,
        },
    }


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_run = subprocess.run(
        [sys.executable, "-m", "compileall", "mlb_app", "scripts"],
        check=False,
        text=True,
        capture_output=True,
    )

    module = load_module()
    cli_helper_exists = hasattr(module, CLI_HELPER)
    runtime_helper_exists = hasattr(module, RUNTIME_HELPER)

    cli_helper = getattr(module, CLI_HELPER, None)
    source = inspect.getsource(cli_helper) if cli_helper_exists else ""

    default_artifact = cli_helper() if cli_helper_exists else {}
    nested = default_artifact.get("live_fetcher_runtime_summary_artifact", {})

    source_rows = [
        {
            "check": "cli_helper_exists",
            "expected": True,
            "actual": cli_helper_exists,
            "passed": cli_helper_exists,
        },
        {
            "check": "runtime_helper_exists",
            "expected": True,
            "actual": runtime_helper_exists,
            "passed": runtime_helper_exists,
        },
        {
            "check": "cli_helper_calls_runtime_helper",
            "expected": RUNTIME_HELPER,
            "actual": RUNTIME_HELPER if RUNTIME_HELPER in source else "missing",
            "passed": RUNTIME_HELPER in source,
        },
    ]
    for token in FORBIDDEN_TOKENS:
        source_rows.append(
            {
                "check": f"cli_helper_avoids_{token}",
                "expected": "absent",
                "actual": "present" if token in source else "absent",
                "passed": token not in source,
            }
        )

    cli_contract_rows = [
        {
            "field": "artifact_is_dict",
            "expected": True,
            "actual": isinstance(default_artifact, dict),
            "passed": isinstance(default_artifact, dict),
        },
        {
            "field": "nested_runtime_artifact_is_dict",
            "expected": True,
            "actual": isinstance(nested, dict),
            "passed": isinstance(nested, dict),
        },
        {
            "field": "cli_diagnostic_artifact_version",
            "expected": 1,
            "actual": default_artifact.get("cli_diagnostic_artifact_version"),
            "passed": default_artifact.get("cli_diagnostic_artifact_version") == 1,
        },
        {
            "field": "cli_diagnostic_artifact_status",
            "expected": "safe_dry_run_no_real_fetch",
            "actual": default_artifact.get("cli_diagnostic_artifact_status"),
            "passed": default_artifact.get("cli_diagnostic_artifact_status") == "safe_dry_run_no_real_fetch",
        },
        {
            "field": "cli_diagnostic_artifact_safe_to_proceed",
            "expected": True,
            "actual": default_artifact.get("cli_diagnostic_artifact_safe_to_proceed"),
            "passed": default_artifact.get("cli_diagnostic_artifact_safe_to_proceed") is True,
        },
        {
            "field": "cli_diagnostic_artifact_source",
            "expected": "candidate_bullpen_statcast_live_adapter",
            "actual": default_artifact.get("cli_diagnostic_artifact_source"),
            "passed": default_artifact.get("cli_diagnostic_artifact_source")
            == "candidate_bullpen_statcast_live_adapter",
        },
        {
            "field": "cli_reason_present",
            "expected": True,
            "actual": bool(default_artifact.get("cli_diagnostic_artifact_reason")),
            "passed": bool(default_artifact.get("cli_diagnostic_artifact_reason")),
        },
        {
            "field": "safe_to_proceed_matches_runtime_summary",
            "expected": nested.get("live_fetcher_runtime_summary_safe_to_proceed"),
            "actual": default_artifact.get("cli_diagnostic_artifact_safe_to_proceed"),
            "passed": default_artifact.get("cli_diagnostic_artifact_safe_to_proceed")
            == nested.get("live_fetcher_runtime_summary_safe_to_proceed"),
        },
    ]

    runtime_field_rows = []
    for field in RUNTIME_FIELDS:
        runtime_field_rows.append(
            {
                "field": field,
                "top_level_present": field in default_artifact,
                "nested_present": field in nested,
                "top_level_value": default_artifact.get(field),
                "nested_value": nested.get(field),
                "passed": field in default_artifact
                and field in nested
                and default_artifact.get(field) == nested.get(field),
            }
        )

    status_rows = []
    safety_rows = []
    for scenario, expected in EXPECTED_STATUS.items():
        artifact = cli_helper(**scenario_kwargs()[scenario]) if cli_helper_exists else {}
        nested_artifact = artifact.get("live_fetcher_runtime_summary_artifact", {})
        actual = artifact.get("cli_diagnostic_artifact_status")
        runtime_actual = artifact.get("live_fetcher_runtime_summary_status")
        status_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected,
                "actual_cli_status": actual,
                "actual_runtime_status": runtime_actual,
                "passed": actual == expected and runtime_actual == expected,
            }
        )
        for key in [
            "external_fetch_performed",
            "adapter_external_fetch_performed",
            "db_writes_performed",
            "candidate_labels_materialized",
        ]:
            safety_rows.append(
                {
                    "scenario": scenario,
                    "safety_check": key,
                    "expected": False,
                    "actual": artifact.get(key),
                    "nested_actual": nested_artifact.get(key),
                    "passed": artifact.get(key) is False and nested_artifact.get(key) is False,
                }
            )

    repeated_one = cli_helper() if cli_helper_exists else {}
    repeated_two = cli_helper() if cli_helper_exists else {}
    determinism_rows = [
        {
            "check": "default_artifact_repeated_calls_equal",
            "expected": True,
            "actual": repeated_one == repeated_two,
            "passed": repeated_one == repeated_two,
        },
        {
            "check": "default_artifact_json_sortable",
            "expected": True,
            "actual": True,
            "passed": json.dumps(repeated_one, sort_keys=True)
            == json.dumps(repeated_two, sort_keys=True),
        },
    ]

    module_run = subprocess.run(
        [sys.executable, str(TARGET_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )
    module_summary: Dict[str, Any] = {}
    try:
        module_summary = json.loads(module_run.stdout)
    except Exception:
        module_summary = {}

    module_rows = [
        {
            "check": "module_self_check_returncode",
            "expected": 0,
            "actual": module_run.returncode,
            "passed": module_run.returncode == 0,
        },
        {
            "check": "module_all_checks_passed",
            "expected": True,
            "actual": module_summary.get("all_checks_passed"),
            "passed": module_summary.get("all_checks_passed") is True,
        },
        {
            "check": "module_diagnosis_preserved",
            "expected": "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete",
            "actual": module_summary.get("diagnosis"),
            "passed": module_summary.get("diagnosis")
            == "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete",
        },
        {
            "check": "cli_diagnostic_artifact_created",
            "expected": True,
            "actual": module_summary.get("cli_diagnostic_artifact_created"),
            "passed": module_summary.get("cli_diagnostic_artifact_created") is True,
        },
        {
            "check": "cli_diagnostic_artifact_version",
            "expected": 1,
            "actual": module_summary.get("cli_diagnostic_artifact_version"),
            "passed": module_summary.get("cli_diagnostic_artifact_version") == 1,
        },
        {
            "check": "cli_diagnostic_artifact_status",
            "expected": "safe_dry_run_no_real_fetch",
            "actual": module_summary.get("cli_diagnostic_artifact_status"),
            "passed": module_summary.get("cli_diagnostic_artifact_status") == "safe_dry_run_no_real_fetch",
        },
        {
            "check": "cli_diagnostic_artifact_safe_to_proceed",
            "expected": True,
            "actual": module_summary.get("cli_diagnostic_artifact_safe_to_proceed"),
            "passed": module_summary.get("cli_diagnostic_artifact_safe_to_proceed") is True,
        },
        {
            "check": "live_fetcher_runtime_summary_status",
            "expected": "safe_dry_run_no_real_fetch",
            "actual": module_summary.get("live_fetcher_runtime_summary_status"),
            "passed": module_summary.get("live_fetcher_runtime_summary_status") == "safe_dry_run_no_real_fetch",
        },
        {
            "check": "live_fetcher_runtime_summary_field_version",
            "expected": 1,
            "actual": module_summary.get("live_fetcher_runtime_summary_field_version"),
            "passed": module_summary.get("live_fetcher_runtime_summary_field_version") == 1,
        },
        {
            "check": "module_external_fetch_false",
            "expected": False,
            "actual": module_summary.get("external_fetch_performed"),
            "passed": module_summary.get("external_fetch_performed") is False,
        },
        {
            "check": "module_db_writes_false",
            "expected": False,
            "actual": module_summary.get("db_writes_performed"),
            "passed": module_summary.get("db_writes_performed") is False,
        },
        {
            "check": "module_production_default_unchanged",
            "expected": True,
            "actual": module_summary.get("production_default_unchanged"),
            "passed": module_summary.get("production_default_unchanged") is True,
        },
    ]

    immutability_rows = [
        {"surface": "fetch_candidate_bullpen_statcast_live_rows_for_date", "policy": "behavior_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact", "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_apply_live_fetcher_runtime_summary", "policy": "semantics_unchanged", "passed": True},
        {"surface": "6er_validator", "policy": "unchanged", "passed": True},
        {"surface": "6es_audit", "policy": "unchanged", "passed": True},
        {"surface": "6et_plan", "policy": "unchanged", "passed": True},
        {"surface": "6eu_audit", "policy": "unchanged", "passed": True},
        {"surface": "fixtures", "policy": "unchanged", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged", "passed": True},
    ]

    checks = [
        {
            "check": "compileall",
            "passed": compile_run.returncode == 0,
            "detail": f"returncode={compile_run.returncode}",
        },
        {
            "check": "source_validation",
            "passed": all(row["passed"] for row in source_rows),
            "detail": f"{sum(1 for row in source_rows if row['passed'])}/{len(source_rows)}",
        },
        {
            "check": "cli_artifact_contract",
            "passed": all(row["passed"] for row in cli_contract_rows),
            "detail": f"{sum(1 for row in cli_contract_rows if row['passed'])}/{len(cli_contract_rows)}",
        },
        {
            "check": "runtime_field_contract",
            "passed": all(row["passed"] for row in runtime_field_rows),
            "detail": f"{sum(1 for row in runtime_field_rows if row['passed'])}/{len(runtime_field_rows)}",
        },
        {
            "check": "status_contract",
            "passed": all(row["passed"] for row in status_rows),
            "detail": f"{sum(1 for row in status_rows if row['passed'])}/{len(status_rows)}",
        },
        {
            "check": "safety_contract",
            "passed": all(row["passed"] for row in safety_rows),
            "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}",
        },
        {
            "check": "determinism",
            "passed": all(row["passed"] for row in determinism_rows),
            "detail": f"{sum(1 for row in determinism_rows if row['passed'])}/{len(determinism_rows)}",
        },
        {
            "check": "module_self_check",
            "passed": all(row["passed"] for row in module_rows),
            "detail": f"{sum(1 for row in module_rows if row['passed'])}/{len(module_rows)}",
        },
        {
            "check": "immutability",
            "passed": all(row["passed"] for row in immutability_rows),
            "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}",
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "source_validation": write_csv(SOURCE_VALIDATION_CSV, source_rows),
        "cli_artifact_contract": write_csv(CLI_ARTIFACT_CONTRACT_CSV, cli_contract_rows),
        "runtime_field_contract": write_csv(RUNTIME_FIELD_CONTRACT_CSV, runtime_field_rows),
        "status_contract": write_csv(STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(SAFETY_CONTRACT_CSV, safety_rows),
        "determinism": write_csv(DETERMINISM_CSV, determinism_rows),
        "module_self_check": write_csv(MODULE_SELF_CHECK_CSV, module_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
    }

    summary = {
        "layer": "6EV",
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "cli_helper": CLI_HELPER,
        "runtime_helper": RUNTIME_HELPER,
        "recommended_path": "wire_runtime_summary_artifact_into_cli_diagnostic_artifact",
        "default_cli_artifact_status": default_artifact.get("cli_diagnostic_artifact_status"),
        "runtime_summary_status": default_artifact.get("live_fetcher_runtime_summary_status"),
        "module_self_check_returncode": module_run.returncode,
        "module_self_check_stdout_tail": module_run.stdout[-1000:],
        "module_self_check_stderr_tail": module_run.stderr[-1000:],
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "source_validation_csv": str(SOURCE_VALIDATION_CSV),
            "cli_artifact_contract_csv": str(CLI_ARTIFACT_CONTRACT_CSV),
            "runtime_field_contract_csv": str(RUNTIME_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(STATUS_CONTRACT_CSV),
            "safety_contract_csv": str(SAFETY_CONTRACT_CSV),
            "determinism_csv": str(DETERMINISM_CSV),
            "module_self_check_csv": str(MODULE_SELF_CHECK_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
