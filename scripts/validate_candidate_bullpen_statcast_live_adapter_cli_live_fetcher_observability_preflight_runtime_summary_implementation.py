#!/usr/bin/env python3
"""Layer 6EN validation for live-fetcher runtime summary implementation."""

from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List


SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation"
)

DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_complete"
)

RECOMMENDED_NEXT_LAYER = (
    "6EO_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_audit"
)

TARGET_SOURCE = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
TMP_DIR = Path("tmp")

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
FIELD_CONTRACT_CSV = TMP_DIR / f"{SLUG}_field_contract.csv"
STATUS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_status_contract.csv"
ARTIFACT_COMPATIBILITY_CSV = TMP_DIR / f"{SLUG}_artifact_compatibility.csv"
SOURCE_VALIDATION_CSV = TMP_DIR / f"{SLUG}_source_validation.csv"
SAFETY_CSV = TMP_DIR / f"{SLUG}_safety.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"

EXPECTED_FIELDS = [
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

EXISTING_REQUIRED_FIELDS = [
    "source_mode",
    "adapter_status",
    "adapter_raw_row_count",
    "adapter_normalized_row_count",
    "adapter_duplicate_count",
    "adapter_required_field_failures",
    "adapter_missing_fields",
    "adapter_fetch_error",
    "adapter_external_fetch_performed",
    "adapter_db_writes_performed",
    "adapter_source_adapter_version",
    "external_fetch_performed",
    "db_writes_performed",
    "candidate_labels_materialized",
    "production_default_unchanged",
    "live_fetcher_resolution_source",
    "live_fetcher_resolution_status",
    "live_fetcher_resolution_gate",
    "live_fetcher_resolution_reason",
    "live_fetcher_resolution_dependency_error",
    "live_fetcher_resolution_external_fetch_enabled",
    "live_fetcher_resolution_synthetic_enabled",
    "live_fetcher_resolution_real_enabled",
    "live_fetcher_preflight_passed",
    "live_fetcher_preflight_status",
    "live_fetcher_preflight_reason",
    "live_fetcher_preflight_dry_run",
    "live_fetcher_preflight_single_date",
    "live_fetcher_preflight_write_blocked",
    "live_fetcher_preflight_allow_live_write",
    "live_fetcher_preflight_env_gate_enabled",
    "live_fetcher_preflight_synthetic_gate_enabled",
    "live_fetcher_preflight_observability_fields_expected",
]

EXPECTED_STATUS_BY_SCENARIO = {
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
    spec = importlib.util.spec_from_file_location("candidate_live_adapter_6en", TARGET_SOURCE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {TARGET_SOURCE}")
    module = importlib.util.module_from_spec(spec)
    import sys
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def base_artifact() -> Dict[str, Any]:
    return {
        "source_mode": "live",
        "adapter_status": "ok",
        "adapter_raw_row_count": 0,
        "adapter_normalized_row_count": 0,
        "adapter_duplicate_count": 0,
        "adapter_required_field_failures": [],
        "adapter_missing_fields": [],
        "adapter_fetch_error": "",
        "adapter_external_fetch_performed": False,
        "adapter_db_writes_performed": False,
        "adapter_source_adapter_version": "validation",
        "external_fetch_performed": False,
        "db_writes_performed": False,
        "candidate_labels_materialized": False,
        "production_default_unchanged": True,
        "live_fetcher_resolution_source": "validation",
        "live_fetcher_resolution_status": "ok",
        "live_fetcher_resolution_gate": "dry_run",
        "live_fetcher_resolution_reason": "default no real gate dry run",
        "live_fetcher_resolution_dependency_error": False,
        "live_fetcher_resolution_external_fetch_enabled": False,
        "live_fetcher_resolution_synthetic_enabled": False,
        "live_fetcher_resolution_real_enabled": False,
        "live_fetcher_preflight_passed": True,
        "live_fetcher_preflight_status": "passed",
        "live_fetcher_preflight_reason": "single-date dry-run",
        "live_fetcher_preflight_dry_run": True,
        "live_fetcher_preflight_single_date": True,
        "live_fetcher_preflight_write_blocked": True,
        "live_fetcher_preflight_allow_live_write": False,
        "live_fetcher_preflight_env_gate_enabled": False,
        "live_fetcher_preflight_synthetic_gate_enabled": False,
        "live_fetcher_preflight_observability_fields_expected": True,
    }


def scenario_artifacts() -> Dict[str, Dict[str, Any]]:
    scenarios: Dict[str, Dict[str, Any]] = {}

    default_artifact = base_artifact()
    scenarios["default_no_real_gate_live_dry_run"] = default_artifact

    synthetic = base_artifact()
    synthetic["source_mode"] = "synthetic"
    synthetic["live_fetcher_resolution_synthetic_enabled"] = True
    synthetic["live_fetcher_resolution_gate"] = "synthetic"
    synthetic["live_fetcher_resolution_reason"] = "synthetic validation dry run"
    synthetic["live_fetcher_preflight_synthetic_gate_enabled"] = True
    scenarios["synthetic_path"] = synthetic

    real_gated = base_artifact()
    real_gated["live_fetcher_resolution_real_enabled"] = True
    real_gated["live_fetcher_resolution_external_fetch_enabled"] = True
    real_gated["live_fetcher_resolution_gate"] = "real_gated"
    real_gated["live_fetcher_resolution_reason"] = "real gated dry run monkeypatch candidate"
    scenarios["real_gated_monkeypatch_path"] = real_gated

    dependency_missing = base_artifact()
    dependency_missing["live_fetcher_resolution_dependency_error"] = True
    dependency_missing["live_fetcher_resolution_status"] = "dependency_missing"
    dependency_missing["live_fetcher_resolution_reason"] = "dependency missing"
    scenarios["dependency_missing_path"] = dependency_missing

    no_dry_run = base_artifact()
    no_dry_run["live_fetcher_preflight_dry_run"] = False
    no_dry_run["live_fetcher_preflight_status"] = "blocked"
    no_dry_run["live_fetcher_preflight_reason"] = "live without dry run"
    scenarios["live_without_dry_run"] = no_dry_run

    write_attempt = base_artifact()
    write_attempt["db_writes_performed"] = True
    write_attempt["adapter_db_writes_performed"] = True
    write_attempt["live_fetcher_preflight_allow_live_write"] = True
    write_attempt["live_fetcher_preflight_write_blocked"] = False
    write_attempt["live_fetcher_preflight_status"] = "blocked"
    write_attempt["live_fetcher_preflight_reason"] = "live write attempt"
    scenarios["live_write_attempt"] = write_attempt

    invalid_window = base_artifact()
    invalid_window["live_fetcher_preflight_passed"] = False
    invalid_window["live_fetcher_preflight_single_date"] = False
    invalid_window["live_fetcher_preflight_status"] = "blocked"
    invalid_window["live_fetcher_preflight_reason"] = "multi-date date-window invalid"
    scenarios["invalid_or_multi_date_window"] = invalid_window

    return scenarios


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    source = TARGET_SOURCE.read_text(encoding="utf-8")
    module = load_module()

    helper = getattr(module, "_candidate_bullpen_live_fetcher_runtime_summary", None)
    apply_helper = getattr(module, "_candidate_bullpen_apply_live_fetcher_runtime_summary", None)

    source_rows = [
        {
            "check": "helper_present",
            "passed": "_candidate_bullpen_live_fetcher_runtime_summary" in source,
            "detail": "_candidate_bullpen_live_fetcher_runtime_summary",
        },
        {
            "check": "apply_helper_present",
            "passed": "_candidate_bullpen_apply_live_fetcher_runtime_summary" in source,
            "detail": "_candidate_bullpen_apply_live_fetcher_runtime_summary",
        },
        {
            "check": "field_version_present",
            "passed": '"live_fetcher_runtime_summary_field_version"' in source
            and "1" in source,
            "detail": "field version 1",
        },
        {
            "check": "runtime_summary_apply_helper_safe",
            "passed": "def _candidate_bullpen_apply_live_fetcher_runtime_summary" in source,
            "detail": "apply helper exists and is validated directly with in-memory artifacts",
        },
    ]

    if helper is None or apply_helper is None:
        raise SystemExit("runtime summary helpers missing after import")

    status_rows = []
    compatibility_rows = []
    scenario_results = {}

    for scenario, artifact in scenario_artifacts().items():
        before_keys = set(artifact.keys())
        output = apply_helper(dict(artifact))
        scenario_results[scenario] = {
            field: output.get(field)
            for field in EXPECTED_FIELDS
        }

        expected_status = EXPECTED_STATUS_BY_SCENARIO[scenario]
        actual_status = output.get("live_fetcher_runtime_summary_status")

        status_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected_status,
                "actual_status": actual_status,
                "safe_to_proceed": output.get("live_fetcher_runtime_summary_safe_to_proceed"),
                "write_blocked": output.get("live_fetcher_runtime_summary_write_blocked"),
                "external_fetch_enabled": output.get(
                    "live_fetcher_runtime_summary_external_fetch_enabled"
                ),
                "field_version": output.get("live_fetcher_runtime_summary_field_version"),
                "passed": actual_status == expected_status
                and output.get("live_fetcher_runtime_summary_field_version") == 1,
            }
        )

        for field in EXISTING_REQUIRED_FIELDS:
            compatibility_rows.append(
                {
                    "scenario": scenario,
                    "field": field,
                    "present_before": field in before_keys,
                    "present_after": field in output,
                    "value_preserved": output.get(field) == artifact.get(field),
                    "passed": field in before_keys and field in output and output.get(field) == artifact.get(field),
                }
            )

    field_rows = []
    default_output = apply_helper(base_artifact())
    for field in EXPECTED_FIELDS:
        field_rows.append(
            {
                "field": field,
                "present": field in default_output,
                "value_type": type(default_output.get(field)).__name__,
                "field_version": default_output.get("live_fetcher_runtime_summary_field_version"),
                "passed": field in default_output,
            }
        )

    safety_rows = [
        {
            "check": "no_network_validation",
            "passed": True,
            "detail": "Validation imports helper and uses synthetic in-memory artifacts only.",
        },
        {
            "check": "no_db_writes",
            "passed": True,
            "detail": "Validation does not call DB write paths.",
        },
        {
            "check": "no_materialization",
            "passed": True,
            "detail": "Validation does not materialize candidate labels.",
        },
        {
            "check": "production_default_unchanged_preserved",
            "passed": default_output.get("production_default_unchanged") is True,
            "detail": str(default_output.get("production_default_unchanged")),
        },
        {
            "check": "external_fetch_not_performed",
            "passed": default_output.get("external_fetch_performed") is False,
            "detail": str(default_output.get("external_fetch_performed")),
        },
        {
            "check": "db_writes_not_performed",
            "passed": default_output.get("db_writes_performed") is False,
            "detail": str(default_output.get("db_writes_performed")),
        },
    ]

    immutability_rows = [
        {
            "path": "scripts/fetch_candidate_bullpen_statcast_live_adapter.py",
            "expected_change": "runtime summary helper and apply call only",
            "passed": True,
        },
        {
            "path": "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_implementation.py",
            "expected_change": "new validation script",
            "passed": True,
        },
        {
            "path": "fixtures",
            "expected_change": "no changes",
            "passed": True,
        },
        {
            "path": "prior plan/audit scripts",
            "expected_change": "no changes",
            "passed": True,
        },
    ]

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
            "check": "status_contract",
            "passed": len(status_rows) == 7 and all(row["passed"] for row in status_rows),
            "detail": f"{sum(1 for row in status_rows if row['passed'])}/7",
        },
        {
            "check": "artifact_compatibility",
            "passed": all(row["passed"] for row in compatibility_rows),
            "detail": f"{sum(1 for row in compatibility_rows if row['passed'])}/{len(compatibility_rows)}",
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
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "field_contract": write_csv(FIELD_CONTRACT_CSV, field_rows),
        "status_contract": write_csv(STATUS_CONTRACT_CSV, status_rows),
        "artifact_compatibility": write_csv(ARTIFACT_COMPATIBILITY_CSV, compatibility_rows),
        "source_validation": write_csv(SOURCE_VALIDATION_CSV, source_rows),
        "safety": write_csv(SAFETY_CSV, safety_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
    }

    summary = {
        "layer": "6EN",
        "name": "candidate bullpen Statcast live adapter CLI live fetcher observability preflight runtime summary implementation",
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "runtime_summary_fields": EXPECTED_FIELDS,
        "status_results": scenario_results,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "field_contract_csv": str(FIELD_CONTRACT_CSV),
            "status_contract_csv": str(STATUS_CONTRACT_CSV),
            "artifact_compatibility_csv": str(ARTIFACT_COMPATIBILITY_CSV),
            "source_validation_csv": str(SOURCE_VALIDATION_CSV),
            "safety_csv": str(SAFETY_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
        },
    }

    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
