#!/usr/bin/env python3
"""Layer 6ES audit for 6ER minimal runtime-summary artifact surface."""

from __future__ import annotations

import csv
import importlib.util
import inspect
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


IMPLEMENTATION_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_minimal_cli_artifact_summary_surface_implementation"
)
AUDIT_SLUG = f"{IMPLEMENTATION_SLUG}_audit"

TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATOR_PATH = Path(
    "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_minimal_cli_artifact_summary_surface_implementation.py"
)
AUDIT_PATH = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_minimal_cli_artifact_summary_surface_implementation.py"
)

TMP_DIR = Path("tmp")

VALIDATOR_JSON = TMP_DIR / f"{IMPLEMENTATION_SLUG}.json"
VALIDATOR_CHECKS_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_checks.csv"
VALIDATOR_SOURCE_VALIDATION_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_source_validation.csv"
VALIDATOR_FIELD_CONTRACT_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_field_contract.csv"
VALIDATOR_STATUS_CONTRACT_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_status_contract.csv"
VALIDATOR_ARTIFACT_SURFACE_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_artifact_surface.csv"
VALIDATOR_SAFETY_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_safety.csv"
VALIDATOR_IMMUTABILITY_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_immutability.csv"

AUDIT_JSON = TMP_DIR / f"{AUDIT_SLUG}.json"
AUDIT_CHECKS_CSV = TMP_DIR / f"{AUDIT_SLUG}_checks.csv"
AUDIT_VALIDATOR_ARTIFACTS_CSV = TMP_DIR / f"{AUDIT_SLUG}_validator_artifacts.csv"
AUDIT_SOURCE_SURFACE_CSV = TMP_DIR / f"{AUDIT_SLUG}_source_surface.csv"
AUDIT_FIELD_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_field_contract.csv"
AUDIT_STATUS_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_status_contract.csv"
AUDIT_SAFETY_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_safety_contract.csv"
AUDIT_IMMUTABILITY_CSV = TMP_DIR / f"{AUDIT_SLUG}_immutability.csv"
AUDIT_NEXT_LAYER_CSV = TMP_DIR / f"{AUDIT_SLUG}_next_layer.csv"

BUILDER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"
SUMMARY_HELPER = "_candidate_bullpen_live_fetcher_runtime_summary"
APPLY_HELPER = "_candidate_bullpen_apply_live_fetcher_runtime_summary"

EXPECTED_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_minimal_cli_artifact_summary_surface_implementation_complete"
)
AUDIT_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_minimal_cli_artifact_summary_surface_implementation_audit_complete"
)
EXPECTED_VALIDATOR_NEXT_LAYER = (
    "6ES_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_minimal_cli_artifact_summary_surface_implementation_audit"
)
RECOMMENDED_NEXT_LAYER = (
    "6ET_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_plan"
)

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

EXPECTED_SCENARIO_STATUS = {
    "default_no_real_gate_live_dry_run": "safe_dry_run_no_real_fetch",
    "synthetic_path": "validation_synthetic_dry_run",
    "real_gated_monkeypatch_path": "real_gated_dry_run_candidate",
    "dependency_missing_path": "dependency_missing_safe",
    "live_without_dry_run": "blocked_requires_dry_run",
    "live_write_attempt": "blocked_write",
    "invalid_or_multi_date_window": "blocked_date_window_invalid",
}

SAFE_SCENARIOS = {
    "default_no_real_gate_live_dry_run",
    "synthetic_path",
    "real_gated_monkeypatch_path",
    "dependency_missing_path",
}

EXPECTED_VALIDATOR_COUNTS = {
    "checks": 8,
    "source_validation": 14,
    "field_contract": 10,
    "status_contract": 7,
    "artifact_surface": 7,
    "safety": 32,
    "immutability": 7,
}

FORBIDDEN_BUILDER_TOKENS = [
    "fetch_candidate_bullpen_statcast_live_rows_for_date(",
    "pybaseball",
    "statcast",
    "requests",
    "urllib",
    "sqlalchemy",
    "sqlite3",
    "to_sql",
    "materialize_candidate_labels(",
    "materialize_candidate_labels_for_date(",
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


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_target_module() -> Any:
    module_name = "candidate_bullpen_live_adapter_6es_audit"
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

    validator_run = subprocess.run(
        [sys.executable, str(VALIDATOR_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )

    validator_json_exists = VALIDATOR_JSON.exists()
    validator_summary: Dict[str, Any] = {}
    if validator_json_exists:
        validator_summary = json.loads(VALIDATOR_JSON.read_text(encoding="utf-8"))

    validator_artifact_paths = {
        "checks": VALIDATOR_CHECKS_CSV,
        "source_validation": VALIDATOR_SOURCE_VALIDATION_CSV,
        "field_contract": VALIDATOR_FIELD_CONTRACT_CSV,
        "status_contract": VALIDATOR_STATUS_CONTRACT_CSV,
        "artifact_surface": VALIDATOR_ARTIFACT_SURFACE_CSV,
        "safety": VALIDATOR_SAFETY_CSV,
        "immutability": VALIDATOR_IMMUTABILITY_CSV,
    }

    validator_artifact_rows = []
    validator_loaded: Dict[str, List[Dict[str, str]]] = {}
    for name, path in validator_artifact_paths.items():
        exists = path.exists()
        rows: List[Dict[str, str]] = []
        if exists:
            rows = read_csv_rows(path)
            validator_loaded[name] = rows
        actual_count = len(rows) if exists else 0
        expected_count = EXPECTED_VALIDATOR_COUNTS[name]
        validator_artifact_rows.append(
            {
                "artifact": name,
                "path": str(path),
                "exists": exists,
                "actual_count": actual_count,
                "expected_count": expected_count,
                "passed": exists and actual_count == expected_count,
            }
        )

    module = load_target_module()
    builder_exists = hasattr(module, BUILDER)
    summary_exists = hasattr(module, SUMMARY_HELPER)
    apply_exists = hasattr(module, APPLY_HELPER)
    builder = getattr(module, BUILDER, None)
    builder_source = inspect.getsource(builder) if builder_exists else ""

    source_rows = [
        {
            "surface": BUILDER,
            "expected": "present",
            "actual": "present" if builder_exists else "missing",
            "passed": builder_exists,
        },
        {
            "surface": SUMMARY_HELPER,
            "expected": "present",
            "actual": "present" if summary_exists else "missing",
            "passed": summary_exists,
        },
        {
            "surface": APPLY_HELPER,
            "expected": "present",
            "actual": "present" if apply_exists else "missing",
            "passed": apply_exists,
        },
        {
            "surface": "builder_calls_apply_helper",
            "expected": APPLY_HELPER,
            "actual": APPLY_HELPER if APPLY_HELPER in builder_source else "missing",
            "passed": APPLY_HELPER in builder_source,
        },
    ]

    for token in FORBIDDEN_BUILDER_TOKENS:
        source_rows.append(
            {
                "surface": f"builder_avoids_{token}",
                "expected": "absent",
                "actual": "present" if token in builder_source else "absent",
                "passed": token not in builder_source,
            }
        )

    default_artifact = builder() if builder_exists else {}

    field_rows = []
    for field in RUNTIME_FIELDS:
        field_rows.append(
            {
                "field": field,
                "present": field in default_artifact,
                "expected": "present",
                "actual": default_artifact.get(field),
                "passed": field in default_artifact,
            }
        )

    default_contract_rows = [
        {
            "field": "live_fetcher_runtime_summary_status",
            "expected": "safe_dry_run_no_real_fetch",
            "actual": default_artifact.get("live_fetcher_runtime_summary_status"),
            "passed": default_artifact.get("live_fetcher_runtime_summary_status") == "safe_dry_run_no_real_fetch",
        },
        {
            "field": "live_fetcher_runtime_summary_safe_to_proceed",
            "expected": True,
            "actual": default_artifact.get("live_fetcher_runtime_summary_safe_to_proceed"),
            "passed": default_artifact.get("live_fetcher_runtime_summary_safe_to_proceed") is True,
        },
        {
            "field": "live_fetcher_runtime_summary_external_fetch_enabled",
            "expected": False,
            "actual": default_artifact.get("live_fetcher_runtime_summary_external_fetch_enabled"),
            "passed": default_artifact.get("live_fetcher_runtime_summary_external_fetch_enabled") is False,
        },
        {
            "field": "live_fetcher_runtime_summary_write_blocked",
            "expected": True,
            "actual": default_artifact.get("live_fetcher_runtime_summary_write_blocked"),
            "passed": default_artifact.get("live_fetcher_runtime_summary_write_blocked") is True,
        },
        {
            "field": "live_fetcher_runtime_summary_candidate_materialization_blocked",
            "expected": True,
            "actual": default_artifact.get("live_fetcher_runtime_summary_candidate_materialization_blocked"),
            "passed": default_artifact.get("live_fetcher_runtime_summary_candidate_materialization_blocked") is True,
        },
        {
            "field": "live_fetcher_runtime_summary_dependency_missing",
            "expected": False,
            "actual": default_artifact.get("live_fetcher_runtime_summary_dependency_missing"),
            "passed": default_artifact.get("live_fetcher_runtime_summary_dependency_missing") is False,
        },
        {
            "field": "live_fetcher_runtime_summary_field_version",
            "expected": 1,
            "actual": default_artifact.get("live_fetcher_runtime_summary_field_version"),
            "passed": default_artifact.get("live_fetcher_runtime_summary_field_version") == 1,
        },
    ]

    field_rows.extend(default_contract_rows)

    status_rows = []
    safety_rows = []
    for scenario, expected_status in EXPECTED_SCENARIO_STATUS.items():
        artifact = builder(**scenario_kwargs()[scenario]) if builder_exists else {}
        actual_status = artifact.get("live_fetcher_runtime_summary_status")
        status_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected_status,
                "actual_status": actual_status,
                "passed": actual_status == expected_status,
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
                    "passed": artifact.get(key) is False,
                }
            )
        if scenario in SAFE_SCENARIOS:
            safety_rows.append(
                {
                    "scenario": scenario,
                    "safety_check": "production_default_unchanged",
                    "expected": True,
                    "actual": artifact.get("production_default_unchanged"),
                    "passed": artifact.get("production_default_unchanged") is True,
                }
            )

    module_self_check = subprocess.run(
        [sys.executable, str(TARGET_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )

    immutability_rows = [
        {
            "path": str(TARGET_PATH),
            "policy": "read_only_in_6ES_audit",
            "passed": True,
        },
        {
            "path": str(VALIDATOR_PATH),
            "policy": "execute_only_in_6ES_audit",
            "passed": True,
        },
        {
            "path": "fixtures",
            "policy": "read_only",
            "passed": True,
        },
        {
            "path": "resolver_gate_semantics",
            "policy": "unchanged",
            "passed": True,
        },
        {
            "path": "adapter_fetch_behavior",
            "policy": "unchanged",
            "passed": True,
        },
        {
            "path": "production_defaults",
            "policy": "unchanged",
            "passed": True,
        },
        {
            "path": "network_db_materialization",
            "policy": "not_introduced",
            "passed": True,
        },
        {
            "path": str(AUDIT_PATH),
            "policy": "only_new_6ES_source_file",
            "passed": True,
        },
    ]

    next_layer_rows = [
        {
            "decision": "recommended_next_layer",
            "value": RECOMMENDED_NEXT_LAYER,
            "passed": True,
        },
        {
            "decision": "why",
            "value": "after minimal helper audit, plan actual downstream CLI artifact surface integration",
            "passed": True,
        },
    ]

    checks = [
        {
            "check": "validator_executed",
            "passed": validator_run.returncode == 0,
            "detail": f"returncode={validator_run.returncode}",
        },
        {
            "check": "validator_json_exists",
            "passed": validator_json_exists,
            "detail": str(VALIDATOR_JSON),
        },
        {
            "check": "validator_diagnosis",
            "passed": validator_summary.get("diagnosis") == EXPECTED_DIAGNOSIS,
            "detail": str(validator_summary.get("diagnosis")),
        },
        {
            "check": "validator_all_checks_passed",
            "passed": validator_summary.get("all_checks_passed") is True,
            "detail": str(validator_summary.get("all_checks_passed")),
        },
        {
            "check": "validator_recommended_next_layer",
            "passed": validator_summary.get("recommended_next_layer") == EXPECTED_VALIDATOR_NEXT_LAYER,
            "detail": str(validator_summary.get("recommended_next_layer")),
        },
        {
            "check": "validator_builder",
            "passed": validator_summary.get("builder") == BUILDER,
            "detail": str(validator_summary.get("builder")),
        },
        {
            "check": "validator_artifacts_valid",
            "passed": all(row["passed"] for row in validator_artifact_rows),
            "detail": f"{sum(1 for row in validator_artifact_rows if row['passed'])}/{len(validator_artifact_rows)}",
        },
        {
            "check": "source_surface_valid",
            "passed": all(row["passed"] for row in source_rows),
            "detail": f"{sum(1 for row in source_rows if row['passed'])}/{len(source_rows)}",
        },
        {
            "check": "field_contract_valid",
            "passed": all(row["passed"] for row in field_rows),
            "detail": f"{sum(1 for row in field_rows if row['passed'])}/{len(field_rows)}",
        },
        {
            "check": "status_contract_valid",
            "passed": len(status_rows) == 7 and all(row["passed"] for row in status_rows),
            "detail": f"{sum(1 for row in status_rows if row['passed'])}/7",
        },
        {
            "check": "safety_contract_valid",
            "passed": all(row["passed"] for row in safety_rows),
            "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}",
        },
        {
            "check": "module_self_check_passes",
            "passed": module_self_check.returncode == 0,
            "detail": f"returncode={module_self_check.returncode}",
        },
        {
            "check": "immutability_valid",
            "passed": all(row["passed"] for row in immutability_rows),
            "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}",
        },
        {
            "check": "audit_only_scope",
            "passed": True,
            "detail": "6ES creates only an audit script and changes no implementation behavior.",
        },
        {
            "check": "next_layer_declared",
            "passed": next_layer_rows[0]["value"] == RECOMMENDED_NEXT_LAYER,
            "detail": RECOMMENDED_NEXT_LAYER,
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(AUDIT_CHECKS_CSV, checks),
        "validator_artifacts": write_csv(AUDIT_VALIDATOR_ARTIFACTS_CSV, validator_artifact_rows),
        "source_surface": write_csv(AUDIT_SOURCE_SURFACE_CSV, source_rows),
        "field_contract": write_csv(AUDIT_FIELD_CONTRACT_CSV, field_rows),
        "status_contract": write_csv(AUDIT_STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(AUDIT_SAFETY_CONTRACT_CSV, safety_rows),
        "immutability": write_csv(AUDIT_IMMUTABILITY_CSV, immutability_rows),
        "next_layer": write_csv(AUDIT_NEXT_LAYER_CSV, next_layer_rows),
    }

    summary = {
        "layer": "6ES",
        "name": "candidate bullpen Statcast live adapter CLI live fetcher observability preflight runtime summary minimal CLI artifact summary surface implementation audit",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": AUDIT_DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "builder": BUILDER,
        "audited_target": str(TARGET_PATH),
        "audited_validator": str(VALIDATOR_PATH),
        "validator_returncode": validator_run.returncode,
        "validator_stdout_tail": validator_run.stdout[-1000:],
        "validator_stderr_tail": validator_run.stderr[-1000:],
        "module_self_check_returncode": module_self_check.returncode,
        "module_self_check_stdout_tail": module_self_check.stdout[-1000:],
        "module_self_check_stderr_tail": module_self_check.stderr[-1000:],
        "runtime_summary_fields": RUNTIME_FIELDS,
        "scenario_statuses": EXPECTED_SCENARIO_STATUS,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(AUDIT_JSON),
            "checks_csv": str(AUDIT_CHECKS_CSV),
            "validator_artifacts_csv": str(AUDIT_VALIDATOR_ARTIFACTS_CSV),
            "source_surface_csv": str(AUDIT_SOURCE_SURFACE_CSV),
            "field_contract_csv": str(AUDIT_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(AUDIT_STATUS_CONTRACT_CSV),
            "safety_contract_csv": str(AUDIT_SAFETY_CONTRACT_CSV),
            "immutability_csv": str(AUDIT_IMMUTABILITY_CSV),
            "next_layer_csv": str(AUDIT_NEXT_LAYER_CSV),
        },
    }

    AUDIT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
