#!/usr/bin/env python3
"""Validate Layer 6ER minimal CLI artifact summary surface implementation."""

from __future__ import annotations

import csv
import importlib.util
import inspect
import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List


SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_minimal_cli_artifact_summary_surface_implementation"
)

TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
TMP_DIR = Path("tmp")

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
SOURCE_VALIDATION_CSV = TMP_DIR / f"{SLUG}_source_validation.csv"
FIELD_CONTRACT_CSV = TMP_DIR / f"{SLUG}_field_contract.csv"
STATUS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_status_contract.csv"
ARTIFACT_SURFACE_CSV = TMP_DIR / f"{SLUG}_artifact_surface.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"

DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_minimal_cli_artifact_summary_surface_implementation_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6ES_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_minimal_cli_artifact_summary_surface_implementation_audit"
)

BUILDER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"
SUMMARY_HELPER = "_candidate_bullpen_live_fetcher_runtime_summary"
APPLY_HELPER = "_candidate_bullpen_apply_live_fetcher_runtime_summary"

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

EXPECTED_STATUS = {
    "default_no_real_gate_live_dry_run": "safe_dry_run_no_real_fetch",
    "synthetic_path": "validation_synthetic_dry_run",
    "real_gated_monkeypatch_path": "real_gated_dry_run_candidate",
    "dependency_missing_path": "dependency_missing_safe",
    "live_without_dry_run": "blocked_requires_dry_run",
    "live_write_attempt": "blocked_write",
    "invalid_or_multi_date_window": "blocked_date_window_invalid",
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


def load_module() -> Any:
    module_name = "candidate_bullpen_live_adapter_6er_validation"
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

    module = load_module()
    source = TARGET_PATH.read_text(encoding="utf-8")

    builder_exists = hasattr(module, BUILDER)
    summary_exists = hasattr(module, SUMMARY_HELPER)
    apply_exists = hasattr(module, APPLY_HELPER)

    builder = getattr(module, BUILDER, None)
    builder_source = inspect.getsource(builder) if builder_exists else ""

    scenario_results: Dict[str, Dict[str, Any]] = {}
    status_rows: List[Dict[str, Any]] = []
    artifact_rows: List[Dict[str, Any]] = []
    safety_rows: List[Dict[str, Any]] = []
    immutability_rows: List[Dict[str, Any]] = []

    for scenario, kwargs in scenario_kwargs().items():
        before_kwargs = deepcopy(kwargs)
        artifact = builder(**kwargs) if builder_exists else {}
        scenario_results[scenario] = artifact

        expected = EXPECTED_STATUS[scenario]
        actual = artifact.get("live_fetcher_runtime_summary_status")
        status_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected,
                "actual_status": actual,
                "safe_to_proceed": artifact.get("live_fetcher_runtime_summary_safe_to_proceed"),
                "write_blocked": artifact.get("live_fetcher_runtime_summary_write_blocked"),
                "external_fetch_enabled": artifact.get("live_fetcher_runtime_summary_external_fetch_enabled"),
                "dependency_missing": artifact.get("live_fetcher_runtime_summary_dependency_missing"),
                "field_version": artifact.get("live_fetcher_runtime_summary_field_version"),
                "passed": actual == expected
                and artifact.get("live_fetcher_runtime_summary_field_version") == 1,
            }
        )

        artifact_rows.append(
            {
                "scenario": scenario,
                "returns_dict": isinstance(artifact, dict),
                "all_runtime_fields_present": all(field in artifact for field in RUNTIME_FIELDS),
                "calls_apply_helper_contract": APPLY_HELPER in builder_source,
                "passed": isinstance(artifact, dict)
                and all(field in artifact for field in RUNTIME_FIELDS)
                and APPLY_HELPER in builder_source,
            }
        )

        safety_rows.extend(
            [
                {
                    "scenario": scenario,
                    "check": "external_fetch_not_performed",
                    "actual": artifact.get("external_fetch_performed"),
                    "passed": artifact.get("external_fetch_performed") is False,
                },
                {
                    "scenario": scenario,
                    "check": "adapter_external_fetch_not_performed",
                    "actual": artifact.get("adapter_external_fetch_performed"),
                    "passed": artifact.get("adapter_external_fetch_performed") is False,
                },
                {
                    "scenario": scenario,
                    "check": "db_writes_not_performed",
                    "actual": artifact.get("db_writes_performed"),
                    "passed": artifact.get("db_writes_performed") is False,
                },
                {
                    "scenario": scenario,
                    "check": "candidate_labels_not_materialized",
                    "actual": artifact.get("candidate_labels_materialized"),
                    "passed": artifact.get("candidate_labels_materialized") is False,
                },
            ]
        )

        if scenario in {
            "default_no_real_gate_live_dry_run",
            "synthetic_path",
            "real_gated_monkeypatch_path",
            "dependency_missing_path",
        }:
            safety_rows.append(
                {
                    "scenario": scenario,
                    "check": "production_default_unchanged",
                    "actual": artifact.get("production_default_unchanged"),
                    "passed": artifact.get("production_default_unchanged") is True,
                }
            )

        immutability_rows.append(
            {
                "scenario": scenario,
                "input_kwargs_preserved": kwargs == before_kwargs,
                "runtime_fields_additive": all(field in artifact for field in RUNTIME_FIELDS),
                "passed": kwargs == before_kwargs and all(field in artifact for field in RUNTIME_FIELDS),
            }
        )

    default_artifact = scenario_results.get("default_no_real_gate_live_dry_run", {})

    source_rows = [
        {
            "check": "builder_present",
            "passed": builder_exists,
            "detail": BUILDER,
        },
        {
            "check": "summary_helper_present",
            "passed": summary_exists,
            "detail": SUMMARY_HELPER,
        },
        {
            "check": "apply_helper_present",
            "passed": apply_exists,
            "detail": APPLY_HELPER,
        },
        {
            "check": "builder_calls_apply_helper",
            "passed": APPLY_HELPER in builder_source,
            "detail": APPLY_HELPER,
        },
    ]

    for token in FORBIDDEN_BUILDER_TOKENS:
        source_rows.append(
            {
                "check": f"builder_avoids_{token}",
                "passed": token not in builder_source,
                "detail": token,
            }
        )

    field_rows = []
    for field in RUNTIME_FIELDS:
        value = default_artifact.get(field)
        expected_type = "int" if field.endswith("_field_version") else (
            "bool" if field in {
                "live_fetcher_runtime_summary_safe_to_proceed",
                "live_fetcher_runtime_summary_external_fetch_enabled",
                "live_fetcher_runtime_summary_write_blocked",
                "live_fetcher_runtime_summary_candidate_materialization_blocked",
                "live_fetcher_runtime_summary_dependency_missing",
            } else "str"
        )
        actual_type = type(value).__name__
        field_rows.append(
            {
                "field": field,
                "present": field in default_artifact,
                "expected_type": expected_type,
                "actual_type": actual_type,
                "value": value,
                "passed": field in default_artifact and actual_type == expected_type,
            }
        )

    module_self_check = subprocess.run(
        [sys.executable, str(TARGET_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )

    checks = [
        {
            "check": "source_validation",
            "passed": all(row["passed"] for row in source_rows),
            "detail": f"{sum(1 for row in source_rows if row['passed'])}/{len(source_rows)}",
        },
        {
            "check": "field_contract",
            "passed": len(field_rows) == 10 and all(row["passed"] for row in field_rows),
            "detail": f"{sum(1 for row in field_rows if row['passed'])}/10",
        },
        {
            "check": "default_status_contract",
            "passed": default_artifact.get("live_fetcher_runtime_summary_status") == "safe_dry_run_no_real_fetch"
            and default_artifact.get("live_fetcher_runtime_summary_safe_to_proceed") is True
            and default_artifact.get("live_fetcher_runtime_summary_external_fetch_enabled") is False
            and default_artifact.get("live_fetcher_runtime_summary_write_blocked") is True
            and default_artifact.get("live_fetcher_runtime_summary_candidate_materialization_blocked") is True
            and default_artifact.get("live_fetcher_runtime_summary_dependency_missing") is False
            and default_artifact.get("live_fetcher_runtime_summary_field_version") == 1,
            "detail": default_artifact.get("live_fetcher_runtime_summary_status"),
        },
        {
            "check": "status_contract",
            "passed": len(status_rows) == 7 and all(row["passed"] for row in status_rows),
            "detail": f"{sum(1 for row in status_rows if row['passed'])}/7",
        },
        {
            "check": "artifact_surface",
            "passed": len(artifact_rows) == 7 and all(row["passed"] for row in artifact_rows),
            "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/7",
        },
        {
            "check": "safety",
            "passed": all(row["passed"] for row in safety_rows),
            "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}",
        },
        {
            "check": "immutability",
            "passed": all(row["passed"] for row in immutability_rows),
            "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}",
        },
        {
            "check": "module_self_check",
            "passed": module_self_check.returncode == 0,
            "detail": f"returncode={module_self_check.returncode}",
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "source_validation": write_csv(SOURCE_VALIDATION_CSV, source_rows),
        "field_contract": write_csv(FIELD_CONTRACT_CSV, field_rows),
        "status_contract": write_csv(STATUS_CONTRACT_CSV, status_rows),
        "artifact_surface": write_csv(ARTIFACT_SURFACE_CSV, artifact_rows),
        "safety": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
    }

    summary = {
        "layer": "6ER",
        "name": "candidate bullpen Statcast live adapter CLI live fetcher observability preflight runtime summary minimal CLI artifact summary surface implementation",
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "builder": BUILDER,
        "runtime_summary_fields": RUNTIME_FIELDS,
        "status_results": scenario_results,
        "module_self_check_returncode": module_self_check.returncode,
        "module_self_check_stdout_tail": module_self_check.stdout[-1000:],
        "module_self_check_stderr_tail": module_self_check.stderr[-1000:],
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "source_validation_csv": str(SOURCE_VALIDATION_CSV),
            "field_contract_csv": str(FIELD_CONTRACT_CSV),
            "status_contract_csv": str(STATUS_CONTRACT_CSV),
            "artifact_surface_csv": str(ARTIFACT_SURFACE_CSV),
            "safety_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
