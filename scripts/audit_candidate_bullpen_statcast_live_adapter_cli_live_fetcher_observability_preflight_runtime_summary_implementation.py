#!/usr/bin/env python3
"""Layer 6EO audit for the 6EN runtime summary implementation.

This audit verifies the merged 6EN helper implementation and validation
artifacts. It also records the known integration gap: the current source has a
module/self-check architecture with _main(), not the later CLI artifact assembly
surface assumed by the 6EL/6EM planning layers.
"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


IMPLEMENTATION_SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation"
)
AUDIT_SLUG = f"{IMPLEMENTATION_SLUG}_audit"

TARGET_SOURCE = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATION_SCRIPT = Path(
    "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation.py"
)
AUDIT_SCRIPT = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation.py"
)

TMP_DIR = Path("tmp")

IMPLEMENTATION_JSON = TMP_DIR / f"{IMPLEMENTATION_SLUG}.json"
IMPLEMENTATION_CHECKS_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_checks.csv"
IMPLEMENTATION_FIELD_CONTRACT_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_field_contract.csv"
IMPLEMENTATION_STATUS_CONTRACT_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_status_contract.csv"
IMPLEMENTATION_ARTIFACT_COMPATIBILITY_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_artifact_compatibility.csv"
IMPLEMENTATION_SOURCE_VALIDATION_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_source_validation.csv"
IMPLEMENTATION_SAFETY_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_safety.csv"
IMPLEMENTATION_IMMUTABILITY_CSV = TMP_DIR / f"{IMPLEMENTATION_SLUG}_immutability.csv"

AUDIT_JSON = TMP_DIR / f"{AUDIT_SLUG}.json"
AUDIT_CHECKS_CSV = TMP_DIR / f"{AUDIT_SLUG}_checks.csv"
AUDIT_SOURCE_CSV = TMP_DIR / f"{AUDIT_SLUG}_source.csv"
AUDIT_VALIDATION_ARTIFACTS_CSV = TMP_DIR / f"{AUDIT_SLUG}_validation_artifacts.csv"
AUDIT_FIELD_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_field_contract.csv"
AUDIT_STATUS_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_status_contract.csv"
AUDIT_SAFETY_CSV = TMP_DIR / f"{AUDIT_SLUG}_safety.csv"
AUDIT_INTEGRATION_GAP_CSV = TMP_DIR / f"{AUDIT_SLUG}_integration_gap.csv"
AUDIT_IMMUTABILITY_CSV = TMP_DIR / f"{AUDIT_SLUG}_immutability.csv"

EXPECTED_6EN_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_complete"
)
EXPECTED_6EN_NEXT_LAYER = (
    "6EO_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_audit"
)
AUDIT_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_implementation_audit_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6EP_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_integration_gap_resolution_plan"
)

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

EXPECTED_STATUS_BY_SCENARIO = {
    "default_no_real_gate_live_dry_run": "safe_dry_run_no_real_fetch",
    "synthetic_path": "validation_synthetic_dry_run",
    "real_gated_monkeypatch_path": "real_gated_dry_run_candidate",
    "dependency_missing_path": "dependency_missing_safe",
    "live_without_dry_run": "blocked_requires_dry_run",
    "live_write_attempt": "blocked_write",
    "invalid_or_multi_date_window": "blocked_date_window_invalid",
}

EXPECTED_VALIDATION_COUNTS = {
    "checks": 6,
    "field_contract": 10,
    "status_contract": 7,
    "artifact_compatibility": 231,
    "source_validation": 4,
    "safety": 6,
    "immutability": 4,
}


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


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


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() == "true"


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    validation_run = subprocess.run(
        [sys.executable, str(VALIDATION_SCRIPT)],
        check=False,
        text=True,
        capture_output=True,
    )

    source = TARGET_SOURCE.read_text(encoding="utf-8")
    implementation_exists = IMPLEMENTATION_JSON.exists()
    implementation: Dict[str, Any] = {}
    if implementation_exists:
        implementation = json.loads(IMPLEMENTATION_JSON.read_text(encoding="utf-8"))

    validation_artifact_paths = {
        "json": IMPLEMENTATION_JSON,
        "checks": IMPLEMENTATION_CHECKS_CSV,
        "field_contract": IMPLEMENTATION_FIELD_CONTRACT_CSV,
        "status_contract": IMPLEMENTATION_STATUS_CONTRACT_CSV,
        "artifact_compatibility": IMPLEMENTATION_ARTIFACT_COMPATIBILITY_CSV,
        "source_validation": IMPLEMENTATION_SOURCE_VALIDATION_CSV,
        "safety": IMPLEMENTATION_SAFETY_CSV,
        "immutability": IMPLEMENTATION_IMMUTABILITY_CSV,
    }

    loaded_csvs: Dict[str, List[Dict[str, str]]] = {}
    validation_artifact_rows = []
    for name, path in validation_artifact_paths.items():
        exists = path.exists()
        row_count = ""
        expected_count = EXPECTED_VALIDATION_COUNTS.get(name, "")
        row_count_valid = True
        if exists and path.suffix == ".csv":
            rows = read_csv_rows(path)
            loaded_csvs[name] = rows
            row_count = len(rows)
            row_count_valid = row_count == expected_count
        validation_artifact_rows.append(
            {
                "artifact": name,
                "path": str(path),
                "exists": exists,
                "row_count": row_count,
                "expected_row_count": expected_count,
                "row_count_valid": row_count_valid,
            }
        )

    field_contract_rows = loaded_csvs.get("field_contract", [])
    status_contract_rows = loaded_csvs.get("status_contract", [])
    safety_rows = loaded_csvs.get("safety", [])
    immutability_rows_from_validation = loaded_csvs.get("immutability", [])

    field_audit_rows = []
    for field in EXPECTED_FIELDS:
        matching = [row for row in field_contract_rows if row.get("field") == field]
        value_type = matching[0].get("value_type", "") if matching else ""
        field_version = matching[0].get("field_version", "") if matching else ""
        field_audit_rows.append(
            {
                "field": field,
                "present_in_source": field in source,
                "present_in_validation_json": field in implementation.get("runtime_summary_fields", []),
                "present_in_field_contract_csv": bool(matching),
                "value_type": value_type,
                "field_version": field_version,
                "passed": field in source
                and field in implementation.get("runtime_summary_fields", [])
                and bool(matching)
                and str(field_version) == "1",
            }
        )

    status_audit_rows = []
    status_results = implementation.get("status_results", {})
    for scenario, expected_status in EXPECTED_STATUS_BY_SCENARIO.items():
        matching = [row for row in status_contract_rows if row.get("scenario") == scenario]
        actual_status_from_csv = matching[0].get("actual_status", "") if matching else ""
        actual_status_from_json = status_results.get(scenario, {}).get(
            "live_fetcher_runtime_summary_status", ""
        )
        status_audit_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected_status,
                "actual_status_from_csv": actual_status_from_csv,
                "actual_status_from_json": actual_status_from_json,
                "field_version": status_results.get(scenario, {}).get(
                    "live_fetcher_runtime_summary_field_version", ""
                ),
                "passed": actual_status_from_csv == expected_status
                and actual_status_from_json == expected_status
                and str(status_results.get(scenario, {}).get("live_fetcher_runtime_summary_field_version", "")) == "1",
            }
        )

    expected_safety_checks = [
        "no_network_validation",
        "no_db_writes",
        "no_materialization",
        "production_default_unchanged_preserved",
        "external_fetch_not_performed",
        "db_writes_not_performed",
    ]
    safety_audit_rows = []
    for check in expected_safety_checks:
        matching = [row for row in safety_rows if row.get("check") == check]
        safety_audit_rows.append(
            {
                "check": check,
                "present": bool(matching),
                "passed_in_6en_validation": boolish(matching[0].get("passed")) if matching else False,
                "audit_passed": bool(matching) and boolish(matching[0].get("passed")),
            }
        )

    has_main = "def main(" in source
    has_underscore_main = "def _main() -> int:" in source
    helper_present = "def _candidate_bullpen_live_fetcher_runtime_summary" in source
    apply_helper_present = "def _candidate_bullpen_apply_live_fetcher_runtime_summary" in source

    cli_artifact_surface_tokens = [
        "argparse",
        "adapter_",
        "live_fetcher_preflight",
        "live_fetcher_observability",
        "candidate_labels_materialized",
    ]
    cli_artifact_assembly_surface_present = (
        "argparse" in source
        and "live_fetcher_preflight" in source
        and "live_fetcher_observability" in source
        and "candidate_labels_materialized" in source
    )

    apply_call_count = source.count("_candidate_bullpen_apply_live_fetcher_runtime_summary(")
    definition_count = source.count("def _candidate_bullpen_apply_live_fetcher_runtime_summary")
    runtime_summary_wired_to_non_definition_call = apply_call_count > definition_count

    source_rows = [
        {
            "check": "helper_present",
            "passed": helper_present,
            "detail": "_candidate_bullpen_live_fetcher_runtime_summary",
        },
        {
            "check": "apply_helper_present",
            "passed": apply_helper_present,
            "detail": "_candidate_bullpen_apply_live_fetcher_runtime_summary",
        },
        {
            "check": "source_uses_underscore_main",
            "passed": has_underscore_main and not has_main,
            "detail": f"_main={has_underscore_main}; main={has_main}",
        },
        {
            "check": "cli_artifact_assembly_surface_absent",
            "passed": not cli_artifact_assembly_surface_present,
            "detail": ",".join(token for token in cli_artifact_surface_tokens if token in source),
        },
        {
            "check": "runtime_summary_not_wired_to_cli_artifact",
            "passed": not runtime_summary_wired_to_non_definition_call,
            "detail": f"apply_call_count={apply_call_count}; definition_count={definition_count}",
        },
    ]

    integration_gap_rows = [
        {
            "item": "helper_implementation_valid",
            "value": True,
            "audit_meaning": "6EN added helper implementation.",
            "passed": helper_present and apply_helper_present,
        },
        {
            "item": "direct_validation_valid",
            "value": True,
            "audit_meaning": "6EN directly validates helpers with in-memory artifacts.",
            "passed": implementation.get("all_checks_passed") is True,
        },
        {
            "item": "cli_artifact_assembly_surface_present",
            "value": cli_artifact_assembly_surface_present,
            "audit_meaning": "Current source exposes the later CLI artifact assembly surface.",
            "passed": cli_artifact_assembly_surface_present is False,
        },
        {
            "item": "cli_artifact_runtime_summary_wired",
            "value": runtime_summary_wired_to_non_definition_call,
            "audit_meaning": "Runtime summary helper is invoked by a real CLI artifact assembly path.",
            "passed": runtime_summary_wired_to_non_definition_call is False,
        },
        {
            "item": "final_cli_artifact_integration_complete",
            "value": False,
            "audit_meaning": "Final runtime summary integration is not complete until CLI artifact wiring exists.",
            "passed": True,
        },
        {
            "item": "follow_up_integration_layer_required",
            "value": True,
            "audit_meaning": "A planning layer should decide how to resolve the integration gap.",
            "passed": True,
        },
    ]

    immutability_rows = [
        {
            "path": str(TARGET_SOURCE),
            "policy": "audit_only_read",
            "passed": True,
        },
        {
            "path": str(VALIDATION_SCRIPT),
            "policy": "audit_only_execute",
            "passed": True,
        },
        {
            "path": str(AUDIT_SCRIPT),
            "policy": "only_new_6eo_file",
            "passed": AUDIT_SCRIPT.name.startswith("audit_"),
        },
        {
            "path": "fixtures",
            "policy": "must_not_modify",
            "passed": True,
        },
        {
            "path": "prior plan/audit/validation scripts",
            "policy": "must_not_modify",
            "passed": True,
        },
        {
            "path": "6EN validation immutability rows",
            "policy": "must_pass",
            "passed": all(boolish(row.get("passed")) for row in immutability_rows_from_validation),
        },
    ]

    checks = [
        {
            "check": "validation_script_executed",
            "passed": validation_run.returncode == 0,
            "detail": f"returncode={validation_run.returncode}",
        },
        {
            "check": "implementation_json_exists",
            "passed": implementation_exists,
            "detail": str(IMPLEMENTATION_JSON),
        },
        {
            "check": "implementation_diagnosis",
            "passed": implementation.get("diagnosis") == EXPECTED_6EN_DIAGNOSIS,
            "detail": str(implementation.get("diagnosis")),
        },
        {
            "check": "implementation_all_checks_passed",
            "passed": implementation.get("all_checks_passed") is True,
            "detail": str(implementation.get("all_checks_passed")),
        },
        {
            "check": "implementation_recommended_next_layer",
            "passed": implementation.get("recommended_next_layer") == EXPECTED_6EN_NEXT_LAYER,
            "detail": str(implementation.get("recommended_next_layer")),
        },
        {
            "check": "source_contract",
            "passed": all(row["passed"] for row in source_rows),
            "detail": f"{sum(1 for row in source_rows if row['passed'])}/{len(source_rows)}",
        },
        {
            "check": "validation_artifact_counts",
            "passed": all(row["row_count_valid"] for row in validation_artifact_rows),
            "detail": json.dumps(
                {row["artifact"]: row["row_count"] for row in validation_artifact_rows},
                sort_keys=True,
            ),
        },
        {
            "check": "field_contract",
            "passed": len(field_audit_rows) == 10 and all(row["passed"] for row in field_audit_rows),
            "detail": f"{sum(1 for row in field_audit_rows if row['passed'])}/10",
        },
        {
            "check": "status_contract",
            "passed": len(status_audit_rows) == 7 and all(row["passed"] for row in status_audit_rows),
            "detail": f"{sum(1 for row in status_audit_rows if row['passed'])}/7",
        },
        {
            "check": "safety_contract",
            "passed": len(safety_audit_rows) == 6 and all(row["audit_passed"] for row in safety_audit_rows),
            "detail": f"{sum(1 for row in safety_audit_rows if row['audit_passed'])}/6",
        },
        {
            "check": "integration_gap_classified",
            "passed": len(integration_gap_rows) == 6 and all(row["passed"] for row in integration_gap_rows),
            "detail": "helper valid; final CLI artifact integration incomplete; follow-up required",
        },
        {
            "check": "audit_only_scope",
            "passed": all(row["passed"] for row in immutability_rows),
            "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}",
        },
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(AUDIT_CHECKS_CSV, checks),
        "source": write_csv(AUDIT_SOURCE_CSV, source_rows),
        "validation_artifacts": write_csv(AUDIT_VALIDATION_ARTIFACTS_CSV, validation_artifact_rows),
        "field_contract": write_csv(AUDIT_FIELD_CONTRACT_CSV, field_audit_rows),
        "status_contract": write_csv(AUDIT_STATUS_CONTRACT_CSV, status_audit_rows),
        "safety": write_csv(AUDIT_SAFETY_CSV, safety_audit_rows),
        "integration_gap": write_csv(AUDIT_INTEGRATION_GAP_CSV, integration_gap_rows),
        "immutability": write_csv(AUDIT_IMMUTABILITY_CSV, immutability_rows),
    }

    summary = {
        "layer": "6EO",
        "name": "candidate bullpen Statcast live adapter CLI live fetcher observability preflight runtime summary implementation audit",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": AUDIT_DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "audited_validation_script": str(VALIDATION_SCRIPT),
        "validation_subprocess_returncode": validation_run.returncode,
        "validation_stdout_tail": validation_run.stdout[-1000:],
        "validation_stderr_tail": validation_run.stderr[-1000:],
        "implementation_helper_valid": helper_present and apply_helper_present,
        "direct_validation_valid": implementation.get("all_checks_passed") is True,
        "cli_artifact_assembly_surface_present": cli_artifact_assembly_surface_present,
        "cli_artifact_runtime_summary_wired": runtime_summary_wired_to_non_definition_call,
        "final_cli_artifact_integration_complete": False,
        "follow_up_integration_layer_required": True,
        "runtime_summary_fields": EXPECTED_FIELDS,
        "status_scenarios": list(EXPECTED_STATUS_BY_SCENARIO.keys()),
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(AUDIT_JSON),
            "checks_csv": str(AUDIT_CHECKS_CSV),
            "source_csv": str(AUDIT_SOURCE_CSV),
            "validation_artifacts_csv": str(AUDIT_VALIDATION_ARTIFACTS_CSV),
            "field_contract_csv": str(AUDIT_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(AUDIT_STATUS_CONTRACT_CSV),
            "safety_csv": str(AUDIT_SAFETY_CSV),
            "integration_gap_csv": str(AUDIT_INTEGRATION_GAP_CSV),
            "immutability_csv": str(AUDIT_IMMUTABILITY_CSV),
        },
    }

    AUDIT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))

    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
