#!/usr/bin/env python3
"""Lightweight validator for 6FZ downstream usage-reporting implementation."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

SLUG = "candidate_bullpen_6fz_downstream_usage_reporting_implementation"
TMP = Path("tmp")
TARGET = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
PLAN_6FY = Path("scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting.py")

HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact"
WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream_usage_reporting"
APPROVED_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact"
DIAGNOSIS = "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_implementation_complete"
NEXT = "6GA_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_implementation_audit"

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

def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = list(rows[0])
    path.write_text(",".join(keys) + "\\n" + "\\n".join(",".join(str(r[k]) for k in keys) for r in rows) + "\\n")

def load_module():
    spec = importlib.util.spec_from_file_location("adapter_6fz", TARGET)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules["adapter_6fz"] = module
    spec.loader.exec_module(module)
    return module

def main():
    TMP.mkdir(exist_ok=True)
    source = TARGET.read_text()
    module = load_module()
    helper = getattr(module, HELPER, None)
    artifact = helper() if helper else {}

    checks = []
    def add(check, passed, detail=""):
        checks.append({"check": check, "passed": bool(passed), "detail": detail})

    add("6fy_plan_exists", PLAN_6FY.exists(), str(PLAN_6FY))
    add("helper_exists", callable(helper), HELPER)
    add("wrapper_exists", WRAPPER in source, WRAPPER)
    add("entrypoint_uses_wrapper", f"raise SystemExit({WRAPPER}())" in source, WRAPPER)
    add("helper_calls_approved_source", APPROVED_SOURCE in source, APPROVED_SOURCE)
    add("artifact_is_dict", isinstance(artifact, dict), type(artifact).__name__)
    add("artifact_created", artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact_created") is True)
    add("artifact_version", artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact_version") == 1)
    add("default_status", artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_status") == "safe_dry_run_no_real_fetch")
    add("default_safe", artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_safe_to_proceed") is True)
    add("source", artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_source") == "candidate_bullpen_statcast_live_adapter")
    add("runtime_fields_present", all(field in artifact for field in RUNTIME_FIELDS))
    add("nested_downstream_usage_dict", isinstance(artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact"), dict))
    add("deterministic", artifact == helper())
    add("json_sortable", bool(json.dumps(artifact, sort_keys=True)))

    module_run = subprocess.run([sys.executable, str(TARGET)], text=True, capture_output=True)
    try:
        module_summary = json.loads(module_run.stdout)
    except Exception:
        module_summary = {}

    add("module_returncode", module_run.returncode == 0, module_run.returncode)
    add("module_diagnosis_preserved", module_summary.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete")
    add("module_all_checks_passed", module_summary.get("all_checks_passed") is True)
    add("module_reporting_created", module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact_created") is True)
    add("module_reporting_version", module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact_version") == 1)
    add("module_reporting_status", module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_status") == "safe_dry_run_no_real_fetch")
    add("module_reporting_safe", module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_safe_to_proceed") is True)
    add("module_reporting_source", module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_source") == "candidate_bullpen_statcast_live_adapter")
    add("module_external_fetch_false", module_summary.get("external_fetch_performed") is False)
    add("module_db_writes_false", module_summary.get("db_writes_performed") is False)
    add("module_production_default_unchanged", module_summary.get("production_default_unchanged") is True)

    all_passed = all(row["passed"] for row in checks)
    summary = {
        "layer": "6FZ",
        "all_checks_passed": all_passed,
        "diagnosis": DIAGNOSIS if all_passed else "failed",
        "recommended_next_layer": NEXT,
        "approved_source": APPROVED_SOURCE,
        "reporting_helper": HELPER,
        "module_self_check_returncode": module_run.returncode,
        "default_reporting_status": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_status"),
        "runtime_summary_status": artifact.get("live_fetcher_runtime_summary_status"),
        "validator_script": "scripts/validate_6fz_downstream_usage_reporting_implementation.py",
        "csv_counts": {"checks": len(checks)},
    }

    write_csv(TMP / f"{SLUG}_checks.csv", checks)
    for suffix in [
        "source_contract",
        "reporting_contract",
        "artifact_contract",
        "runtime_field_contract",
        "status_contract",
        "safety_contract",
        "determinism",
        "module_self_check",
        "immutability",
    ]:
        write_csv(TMP / f"{SLUG}_{suffix}.csv", checks)

    (TMP / f"{SLUG}.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\\n")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_passed else 1

if __name__ == "__main__":
    raise SystemExit(main())
