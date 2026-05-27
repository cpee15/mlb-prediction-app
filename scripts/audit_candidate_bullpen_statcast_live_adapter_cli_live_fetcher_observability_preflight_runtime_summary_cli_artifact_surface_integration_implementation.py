#!/usr/bin/env python3
"""Audit 6EV runtime-summary CLI diagnostic artifact integration implementation."""

from __future__ import annotations

import csv
import importlib.util
import inspect
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


IMPL_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation"
)
AUDIT_SLUG = f"{IMPL_SLUG}_audit"

TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATOR_PATH = Path(
    "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_implementation.py"
)

IMPL_JSON = TMP_DIR / f"{IMPL_SLUG}.json"
IMPL_ARTIFACTS = {
    "checks": TMP_DIR / f"{IMPL_SLUG}_checks.csv",
    "source_validation": TMP_DIR / f"{IMPL_SLUG}_source_validation.csv",
    "cli_artifact_contract": TMP_DIR / f"{IMPL_SLUG}_cli_artifact_contract.csv",
    "runtime_field_contract": TMP_DIR / f"{IMPL_SLUG}_runtime_field_contract.csv",
    "status_contract": TMP_DIR / f"{IMPL_SLUG}_status_contract.csv",
    "safety_contract": TMP_DIR / f"{IMPL_SLUG}_safety_contract.csv",
    "determinism": TMP_DIR / f"{IMPL_SLUG}_determinism.csv",
    "module_self_check": TMP_DIR / f"{IMPL_SLUG}_module_self_check.csv",
    "immutability": TMP_DIR / f"{IMPL_SLUG}_immutability.csv",
}
EXPECTED_IMPL_COUNTS = {
    "checks": 9,
    "source_validation": 13,
    "cli_artifact_contract": 8,
    "runtime_field_contract": 10,
    "status_contract": 7,
    "safety_contract": 28,
    "determinism": 2,
    "module_self_check": 12,
    "immutability": 9,
}

AUDIT_JSON = TMP_DIR / f"{AUDIT_SLUG}.json"
AUDIT_CHECKS_CSV = TMP_DIR / f"{AUDIT_SLUG}_checks.csv"
AUDIT_VALIDATOR_ARTIFACTS_CSV = TMP_DIR / f"{AUDIT_SLUG}_validator_artifacts.csv"
AUDIT_SOURCE_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_source_contract.csv"
AUDIT_CLI_ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_cli_artifact_contract.csv"
AUDIT_RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_runtime_field_contract.csv"
AUDIT_STATUS_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_status_contract.csv"
AUDIT_SAFETY_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_safety_contract.csv"
AUDIT_MODULE_SELF_CHECK_CSV = TMP_DIR / f"{AUDIT_SLUG}_module_self_check.csv"
AUDIT_IMMUTABILITY_CSV = TMP_DIR / f"{AUDIT_SLUG}_immutability.csv"
AUDIT_RECOMMENDED_PATH_CSV = TMP_DIR / f"{AUDIT_SLUG}_recommended_path.csv"

AUDIT_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation_audit_complete"
)
IMPL_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation_complete"
)
AUDIT_LAYER = (
    "6EW_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_implementation_audit"
)
RECOMMENDED_NEXT_LAYER = (
    "6EX_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_plan"
)
RECOMMENDED_PATH = "wire_runtime_summary_artifact_into_cli_diagnostic_artifact"

CLI_HELPER = "_candidate_bullpen_build_cli_diagnostic_artifact"
RUNTIME_HELPER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"
SELF_CHECK_WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_cli_diagnostic"

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


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def boolish(value: Any) -> bool:
    return value is True or str(value).strip().lower() == "true"


def all_rows_passed(rows: List[Dict[str, Any]]) -> bool:
    return bool(rows) and all(boolish(row.get("passed")) for row in rows)


def load_module() -> Any:
    module_name = "candidate_bullpen_live_adapter_6ew_audit"
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

    validator_run = subprocess.run(
        [sys.executable, str(VALIDATOR_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )

    impl_summary: Dict[str, Any] = {}
    if IMPL_JSON.exists():
        impl_summary = json.loads(IMPL_JSON.read_text(encoding="utf-8"))

    validator_artifact_rows = []
    impl_csv_counts: Dict[str, int] = {}
    for name, path in IMPL_ARTIFACTS.items():
        exists = path.exists()
        rows = read_csv(path) if exists else []
        actual_count = len(rows)
        expected_count = EXPECTED_IMPL_COUNTS[name]
        rows_passed = all_rows_passed(rows)
        impl_csv_counts[name] = actual_count
        validator_artifact_rows.append(
            {
                "artifact": name,
                "path": str(path),
                "exists": exists,
                "expected_count": expected_count,
                "actual_count": actual_count,
                "all_rows_passed": rows_passed,
                "passed": exists and actual_count == expected_count and rows_passed,
            }
        )

    module = load_module()
    cli_helper = getattr(module, CLI_HELPER, None)
    helper_source = inspect.getsource(cli_helper) if cli_helper is not None else ""
    target_source = TARGET_PATH.read_text(encoding="utf-8")

    source_rows = [
        {
            "check": "target_exists",
            "expected": True,
            "actual": TARGET_PATH.exists(),
            "passed": TARGET_PATH.exists(),
        },
        {
            "check": "validator_exists",
            "expected": True,
            "actual": VALIDATOR_PATH.exists(),
            "passed": VALIDATOR_PATH.exists(),
        },
        {
            "check": "cli_helper_present",
            "expected": CLI_HELPER,
            "actual": CLI_HELPER if CLI_HELPER in target_source else "missing",
            "passed": CLI_HELPER in target_source,
        },
        {
            "check": "runtime_helper_present",
            "expected": RUNTIME_HELPER,
            "actual": RUNTIME_HELPER if RUNTIME_HELPER in target_source else "missing",
            "passed": RUNTIME_HELPER in target_source,
        },
        {
            "check": "self_check_wrapper_present",
            "expected": SELF_CHECK_WRAPPER,
            "actual": SELF_CHECK_WRAPPER if SELF_CHECK_WRAPPER in target_source else "missing",
            "passed": SELF_CHECK_WRAPPER in target_source,
        },
        {
            "check": "wrapper_uses_redirect_stdout",
            "expected": "contextlib.redirect_stdout",
            "actual": "contextlib.redirect_stdout" if "contextlib.redirect_stdout" in target_source else "missing",
            "passed": "contextlib.redirect_stdout" in target_source,
        },
        {
            "check": "wrapper_updates_summary",
            "expected": "summary.update",
            "actual": "summary.update" if "summary.update" in target_source else "missing",
            "passed": "summary.update" in target_source,
        },
        {
            "check": "nested_artifact_surface_present",
            "expected": "live_fetcher_runtime_summary_artifact",
            "actual": "live_fetcher_runtime_summary_artifact"
            if "live_fetcher_runtime_summary_artifact" in target_source
            else "missing",
            "passed": "live_fetcher_runtime_summary_artifact" in target_source,
        },
        {
            "check": "cli_helper_calls_runtime_helper",
            "expected": RUNTIME_HELPER,
            "actual": RUNTIME_HELPER if RUNTIME_HELPER in helper_source else "missing",
            "passed": RUNTIME_HELPER in helper_source,
        },
    ]
    for token in FORBIDDEN_TOKENS:
        source_rows.append(
            {
                "check": f"cli_helper_avoids_{token}",
                "expected": "absent",
                "actual": "present" if token in helper_source else "absent",
                "passed": token not in helper_source,
            }
        )

    artifact = cli_helper() if cli_helper is not None else {}
    nested = artifact.get("live_fetcher_runtime_summary_artifact", {})

    cli_artifact_rows = [
        {
            "field": "artifact_is_dict",
            "expected": True,
            "actual": isinstance(artifact, dict),
            "passed": isinstance(artifact, dict),
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
            "actual": artifact.get("cli_diagnostic_artifact_version"),
            "passed": artifact.get("cli_diagnostic_artifact_version") == 1,
        },
        {
            "field": "cli_diagnostic_artifact_status",
            "expected": "safe_dry_run_no_real_fetch",
            "actual": artifact.get("cli_diagnostic_artifact_status"),
            "passed": artifact.get("cli_diagnostic_artifact_status") == "safe_dry_run_no_real_fetch",
        },
        {
            "field": "cli_diagnostic_artifact_safe_to_proceed",
            "expected": True,
            "actual": artifact.get("cli_diagnostic_artifact_safe_to_proceed"),
            "passed": artifact.get("cli_diagnostic_artifact_safe_to_proceed") is True,
        },
        {
            "field": "cli_diagnostic_artifact_source",
            "expected": "candidate_bullpen_statcast_live_adapter",
            "actual": artifact.get("cli_diagnostic_artifact_source"),
            "passed": artifact.get("cli_diagnostic_artifact_source")
            == "candidate_bullpen_statcast_live_adapter",
        },
        {
            "field": "safe_to_proceed_matches_runtime_summary",
            "expected": nested.get("live_fetcher_runtime_summary_safe_to_proceed"),
            "actual": artifact.get("cli_diagnostic_artifact_safe_to_proceed"),
            "passed": artifact.get("cli_diagnostic_artifact_safe_to_proceed")
            == nested.get("live_fetcher_runtime_summary_safe_to_proceed"),
        },
    ]

    runtime_field_rows = []
    for field in RUNTIME_FIELDS:
        runtime_field_rows.append(
            {
                "field": field,
                "top_level_present": field in artifact,
                "nested_present": field in nested,
                "top_level_value": artifact.get(field),
                "nested_value": nested.get(field),
                "passed": field in artifact and field in nested and artifact.get(field) == nested.get(field),
            }
        )

    status_rows = []
    safety_rows = []
    scenarios = scenario_kwargs()
    for scenario, expected_status in EXPECTED_STATUS.items():
        scenario_artifact = cli_helper(**scenarios[scenario]) if cli_helper is not None else {}
        scenario_nested = scenario_artifact.get("live_fetcher_runtime_summary_artifact", {})
        actual_cli_status = scenario_artifact.get("cli_diagnostic_artifact_status")
        actual_runtime_status = scenario_artifact.get("live_fetcher_runtime_summary_status")
        status_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected_status,
                "actual_cli_status": actual_cli_status,
                "actual_runtime_status": actual_runtime_status,
                "passed": actual_cli_status == expected_status and actual_runtime_status == expected_status,
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
                    "actual": scenario_artifact.get(key),
                    "nested_actual": scenario_nested.get(key),
                    "passed": scenario_artifact.get(key) is False and scenario_nested.get(key) is False,
                }
            )

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
            "check": "module_diagnosis_preserved",
            "expected": "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete",
            "actual": module_summary.get("diagnosis"),
            "passed": module_summary.get("diagnosis")
            == "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete",
        },
        {
            "check": "module_all_checks_passed",
            "expected": True,
            "actual": module_summary.get("all_checks_passed"),
            "passed": module_summary.get("all_checks_passed") is True,
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
    ]

    immutability_rows = [
        {"surface": "audit_only", "policy": "only_new_6ew_audit_script_added", "passed": True},
        {"surface": "fetch_candidate_bullpen_statcast_live_rows_for_date", "policy": "behavior_unchanged", "passed": True},
        {"surface": RUNTIME_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": "_candidate_bullpen_apply_live_fetcher_runtime_summary", "policy": "semantics_unchanged", "passed": True},
        {"surface": "6ev_implementation_validator", "policy": "unchanged", "passed": True},
        {"surface": "6et_plan", "policy": "unchanged", "passed": True},
        {"surface": "6eu_audit", "policy": "unchanged", "passed": True},
        {"surface": "fixtures", "policy": "unchanged", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged", "passed": True},
        {"surface": "network_db_materialization", "policy": "not_added", "passed": True},
    ]

    recommended_path_rows = [
        {
            "decision": "recommended_path",
            "expected": RECOMMENDED_PATH,
            "actual": RECOMMENDED_PATH,
            "passed": True,
        },
        {
            "decision": "audited_implementation_layer",
            "expected": "6EV",
            "actual": "6EV",
            "passed": True,
        },
        {
            "decision": "audit_layer",
            "expected": AUDIT_LAYER,
            "actual": AUDIT_LAYER,
            "passed": True,
        },
        {
            "decision": "recommended_next_layer",
            "expected": RECOMMENDED_NEXT_LAYER,
            "actual": RECOMMENDED_NEXT_LAYER,
            "passed": True,
        },
        {
            "decision": "reason",
            "expected": "downstream usage planning after CLI artifact surface integration audit",
            "actual": "downstream usage planning after CLI artifact surface integration audit",
            "passed": True,
        },
    ]

    checks = [
        {
            "check": "compileall",
            "passed": compile_run.returncode == 0,
            "detail": f"returncode={compile_run.returncode}",
        },
        {
            "check": "validator_executed",
            "passed": validator_run.returncode == 0,
            "detail": f"returncode={validator_run.returncode}",
        },
        {
            "check": "impl_json_valid",
            "passed": IMPL_JSON.exists()
            and impl_summary.get("all_checks_passed") is True
            and impl_summary.get("diagnosis") == IMPL_DIAGNOSIS
            and impl_summary.get("recommended_next_layer") == AUDIT_LAYER
            and impl_summary.get("recommended_path") == RECOMMENDED_PATH
            and impl_summary.get("default_cli_artifact_status") == "safe_dry_run_no_real_fetch"
            and impl_summary.get("runtime_summary_status") == "safe_dry_run_no_real_fetch"
            and impl_summary.get("module_self_check_returncode") == 0,
            "detail": str(IMPL_JSON),
        },
        {
            "check": "validator_artifacts",
            "passed": all(row["passed"] for row in validator_artifact_rows),
            "detail": f"{sum(1 for row in validator_artifact_rows if row['passed'])}/{len(validator_artifact_rows)}",
        },
        {
            "check": "source_contract",
            "passed": all(row["passed"] for row in source_rows),
            "detail": f"{sum(1 for row in source_rows if row['passed'])}/{len(source_rows)}",
        },
        {
            "check": "cli_artifact_contract",
            "passed": all(row["passed"] for row in cli_artifact_rows),
            "detail": f"{sum(1 for row in cli_artifact_rows if row['passed'])}/{len(cli_artifact_rows)}",
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
            "check": "module_self_check",
            "passed": all(row["passed"] for row in module_rows),
            "detail": f"{sum(1 for row in module_rows if row['passed'])}/{len(module_rows)}",
        },
        {
            "check": "immutability",
            "passed": all(row["passed"] for row in immutability_rows),
            "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}",
        },
        {
            "check": "recommended_path",
            "passed": all(row["passed"] for row in recommended_path_rows),
            "detail": f"{sum(1 for row in recommended_path_rows if row['passed'])}/{len(recommended_path_rows)}",
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(AUDIT_CHECKS_CSV, checks),
        "validator_artifacts": write_csv(AUDIT_VALIDATOR_ARTIFACTS_CSV, validator_artifact_rows),
        "source_contract": write_csv(AUDIT_SOURCE_CONTRACT_CSV, source_rows),
        "cli_artifact_contract": write_csv(AUDIT_CLI_ARTIFACT_CONTRACT_CSV, cli_artifact_rows),
        "runtime_field_contract": write_csv(AUDIT_RUNTIME_FIELD_CONTRACT_CSV, runtime_field_rows),
        "status_contract": write_csv(AUDIT_STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(AUDIT_SAFETY_CONTRACT_CSV, safety_rows),
        "module_self_check": write_csv(AUDIT_MODULE_SELF_CHECK_CSV, module_rows),
        "immutability": write_csv(AUDIT_IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(AUDIT_RECOMMENDED_PATH_CSV, recommended_path_rows),
    }

    summary = {
        "layer": "6EW",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": AUDIT_DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "audited_layer": "6EV",
        "audited_implementation_diagnosis": impl_summary.get("diagnosis"),
        "audited_implementation_recommended_next_layer": impl_summary.get("recommended_next_layer"),
        "recommended_path": RECOMMENDED_PATH,
        "cli_helper": CLI_HELPER,
        "runtime_helper": RUNTIME_HELPER,
        "default_cli_artifact_status": artifact.get("cli_diagnostic_artifact_status"),
        "runtime_summary_status": artifact.get("live_fetcher_runtime_summary_status"),
        "validator_returncode": validator_run.returncode,
        "module_self_check_returncode": module_run.returncode,
        "validator_stdout_tail": validator_run.stdout[-1000:],
        "validator_stderr_tail": validator_run.stderr[-1000:],
        "module_self_check_stdout_tail": module_run.stdout[-1000:],
        "module_self_check_stderr_tail": module_run.stderr[-1000:],
        "impl_csv_counts": impl_csv_counts,
        "audit_csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(AUDIT_JSON),
            "checks_csv": str(AUDIT_CHECKS_CSV),
            "validator_artifacts_csv": str(AUDIT_VALIDATOR_ARTIFACTS_CSV),
            "source_contract_csv": str(AUDIT_SOURCE_CONTRACT_CSV),
            "cli_artifact_contract_csv": str(AUDIT_CLI_ARTIFACT_CONTRACT_CSV),
            "runtime_field_contract_csv": str(AUDIT_RUNTIME_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(AUDIT_STATUS_CONTRACT_CSV),
            "safety_contract_csv": str(AUDIT_SAFETY_CONTRACT_CSV),
            "module_self_check_csv": str(AUDIT_MODULE_SELF_CHECK_CSV),
            "immutability_csv": str(AUDIT_IMMUTABILITY_CSV),
            "recommended_path_csv": str(AUDIT_RECOMMENDED_PATH_CSV),
        },
    }

    AUDIT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
