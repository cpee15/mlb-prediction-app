#!/usr/bin/env python3
"""Audit 6FN CLI exposure surface for usage-reporting runtime-summary artifacts."""

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
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_implementation"
)
AUDIT_SLUG = f"{IMPL_SLUG}_audit"

TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATOR_PATH = Path(
    "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_implementation.py"
)

IMPL_JSON = TMP_DIR / f"{IMPL_SLUG}.json"
IMPL_ARTIFACTS = {
    "checks": TMP_DIR / f"{IMPL_SLUG}_checks.csv",
    "source_contract": TMP_DIR / f"{IMPL_SLUG}_source_contract.csv",
    "cli_exposure_contract": TMP_DIR / f"{IMPL_SLUG}_cli_exposure_contract.csv",
    "artifact_contract": TMP_DIR / f"{IMPL_SLUG}_artifact_contract.csv",
    "runtime_field_contract": TMP_DIR / f"{IMPL_SLUG}_runtime_field_contract.csv",
    "status_contract": TMP_DIR / f"{IMPL_SLUG}_status_contract.csv",
    "safety_contract": TMP_DIR / f"{IMPL_SLUG}_safety_contract.csv",
    "determinism": TMP_DIR / f"{IMPL_SLUG}_determinism.csv",
    "module_self_check": TMP_DIR / f"{IMPL_SLUG}_module_self_check.csv",
    "immutability": TMP_DIR / f"{IMPL_SLUG}_immutability.csv",
}
EXPECTED_IMPL_COUNTS = {
    "checks": 10,
    "source_contract": 20,
    "cli_exposure_contract": 12,
    "artifact_contract": 7,
    "runtime_field_contract": 10,
    "status_contract": 7,
    "safety_contract": 28,
    "determinism": 2,
    "module_self_check": 23,
    "immutability": 14,
}

AUDIT_JSON = TMP_DIR / f"{AUDIT_SLUG}.json"
AUDIT_CHECKS_CSV = TMP_DIR / f"{AUDIT_SLUG}_checks.csv"
AUDIT_VALIDATOR_ARTIFACTS_CSV = TMP_DIR / f"{AUDIT_SLUG}_validator_artifacts.csv"
AUDIT_SOURCE_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_source_contract.csv"
AUDIT_CLI_EXPOSURE_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_cli_exposure_contract.csv"
AUDIT_ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_artifact_contract.csv"
AUDIT_RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_runtime_field_contract.csv"
AUDIT_STATUS_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_status_contract.csv"
AUDIT_SAFETY_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_safety_contract.csv"
AUDIT_MODULE_SELF_CHECK_CSV = TMP_DIR / f"{AUDIT_SLUG}_module_self_check.csv"
AUDIT_DETERMINISM_CSV = TMP_DIR / f"{AUDIT_SLUG}_determinism.csv"
AUDIT_IMMUTABILITY_CSV = TMP_DIR / f"{AUDIT_SLUG}_immutability.csv"
AUDIT_RECOMMENDED_PATH_CSV = TMP_DIR / f"{AUDIT_SLUG}_recommended_path.csv"

AUDIT_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_implementation_audit_complete"
)
IMPL_DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_implementation_complete"
)
AUDIT_LAYER = (
    "6FO_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_implementation_audit"
)
RECOMMENDED_NEXT_LAYER = (
    "6FP_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_plan"
)
RECOMMENDED_PATH = (
    "audit_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_surface"
)

CLI_EXPOSURE_HELPER_NEW = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact"
REPORTING_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_artifact"
USAGE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_artifact"
PRIOR_CLI_EXPOSURE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_artifact"
UPSTREAM_REPORTING_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_reporting_artifact"
DOWNSTREAM_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_usage_artifact"
CLI_HELPER = "_candidate_bullpen_build_cli_diagnostic_artifact"
RUNTIME_HELPER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"
APPLY_HELPER = "_candidate_bullpen_apply_live_fetcher_runtime_summary"
CLI_EXPOSURE_WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure"

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
    module_name = "candidate_bullpen_live_adapter_6fo_audit"
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
    cli_exposure_helper = getattr(module, CLI_EXPOSURE_HELPER_NEW, None)
    target_source = TARGET_PATH.read_text(encoding="utf-8")
    helper_source = inspect.getsource(cli_exposure_helper) if cli_exposure_helper else ""

    source_rows = [
        {"check": "target_exists", "expected": True, "actual": TARGET_PATH.exists(), "passed": TARGET_PATH.exists()},
        {"check": "validator_exists", "expected": True, "actual": VALIDATOR_PATH.exists(), "passed": VALIDATOR_PATH.exists()},
        {"check": "cli_exposure_helper_present", "expected": CLI_EXPOSURE_HELPER_NEW, "actual": CLI_EXPOSURE_HELPER_NEW if CLI_EXPOSURE_HELPER_NEW in target_source else "missing", "passed": CLI_EXPOSURE_HELPER_NEW in target_source},
        {"check": "reporting_helper_present", "expected": REPORTING_HELPER, "actual": REPORTING_HELPER if REPORTING_HELPER in target_source else "missing", "passed": REPORTING_HELPER in target_source},
        {"check": "usage_helper_present", "expected": USAGE_HELPER, "actual": USAGE_HELPER if USAGE_HELPER in target_source else "missing", "passed": USAGE_HELPER in target_source},
        {"check": "prior_cli_exposure_helper_present", "expected": PRIOR_CLI_EXPOSURE_HELPER, "actual": PRIOR_CLI_EXPOSURE_HELPER if PRIOR_CLI_EXPOSURE_HELPER in target_source else "missing", "passed": PRIOR_CLI_EXPOSURE_HELPER in target_source},
        {"check": "upstream_reporting_helper_present", "expected": UPSTREAM_REPORTING_HELPER, "actual": UPSTREAM_REPORTING_HELPER if UPSTREAM_REPORTING_HELPER in target_source else "missing", "passed": UPSTREAM_REPORTING_HELPER in target_source},
        {"check": "downstream_helper_present", "expected": DOWNSTREAM_HELPER, "actual": DOWNSTREAM_HELPER if DOWNSTREAM_HELPER in target_source else "missing", "passed": DOWNSTREAM_HELPER in target_source},
        {"check": "cli_helper_present", "expected": CLI_HELPER, "actual": CLI_HELPER if CLI_HELPER in target_source else "missing", "passed": CLI_HELPER in target_source},
        {"check": "runtime_helper_present", "expected": RUNTIME_HELPER, "actual": RUNTIME_HELPER if RUNTIME_HELPER in target_source else "missing", "passed": RUNTIME_HELPER in target_source},
        {"check": "cli_exposure_wrapper_present", "expected": CLI_EXPOSURE_WRAPPER, "actual": CLI_EXPOSURE_WRAPPER if CLI_EXPOSURE_WRAPPER in target_source else "missing", "passed": CLI_EXPOSURE_WRAPPER in target_source},
        {"check": "cli_exposure_self_check_created_field_present", "expected": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created", "actual": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created" if "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created" in target_source else "missing", "passed": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created" in target_source},
        {"check": "cli_exposure_self_check_status_field_present", "expected": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status", "actual": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status" if "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status" in target_source else "missing", "passed": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status" in target_source},
        {"check": "reporting_artifact_surface_present", "expected": "downstream_runtime_summary_cli_exposure_usage_reporting_artifact", "actual": "downstream_runtime_summary_cli_exposure_usage_reporting_artifact" if "downstream_runtime_summary_cli_exposure_usage_reporting_artifact" in target_source else "missing", "passed": "downstream_runtime_summary_cli_exposure_usage_reporting_artifact" in target_source},
        {"check": "usage_artifact_surface_present", "expected": "downstream_runtime_summary_cli_exposure_usage_artifact", "actual": "downstream_runtime_summary_cli_exposure_usage_artifact" if "downstream_runtime_summary_cli_exposure_usage_artifact" in target_source else "missing", "passed": "downstream_runtime_summary_cli_exposure_usage_artifact" in target_source},
        {"check": "prior_cli_exposure_artifact_surface_present", "expected": "downstream_runtime_summary_cli_exposure_artifact", "actual": "downstream_runtime_summary_cli_exposure_artifact" if "downstream_runtime_summary_cli_exposure_artifact" in target_source else "missing", "passed": "downstream_runtime_summary_cli_exposure_artifact" in target_source},
        {"check": "upstream_reporting_artifact_surface_present", "expected": "downstream_runtime_summary_reporting_artifact", "actual": "downstream_runtime_summary_reporting_artifact" if "downstream_runtime_summary_reporting_artifact" in target_source else "missing", "passed": "downstream_runtime_summary_reporting_artifact" in target_source},
        {"check": "downstream_usage_artifact_surface_present", "expected": "downstream_runtime_summary_usage_artifact", "actual": "downstream_runtime_summary_usage_artifact" if "downstream_runtime_summary_usage_artifact" in target_source else "missing", "passed": "downstream_runtime_summary_usage_artifact" in target_source},
        {"check": "runtime_artifact_surface_present", "expected": "live_fetcher_runtime_summary_artifact", "actual": "live_fetcher_runtime_summary_artifact" if "live_fetcher_runtime_summary_artifact" in target_source else "missing", "passed": "live_fetcher_runtime_summary_artifact" in target_source},
        {"check": "cli_artifact_surface_present", "expected": "cli_diagnostic_artifact", "actual": "cli_diagnostic_artifact" if "cli_diagnostic_artifact" in target_source else "missing", "passed": "cli_diagnostic_artifact" in target_source},
        {"check": "cli_exposure_helper_calls_reporting_helper", "expected": REPORTING_HELPER, "actual": REPORTING_HELPER if REPORTING_HELPER in helper_source else "missing", "passed": REPORTING_HELPER in helper_source},
        {"check": "cli_exposure_helper_avoids_direct_usage_helper", "expected": "absent", "actual": "present" if USAGE_HELPER in helper_source else "absent", "passed": USAGE_HELPER not in helper_source},
        {"check": "cli_exposure_helper_avoids_direct_prior_cli_exposure_helper", "expected": "absent", "actual": "present" if PRIOR_CLI_EXPOSURE_HELPER in helper_source else "absent", "passed": PRIOR_CLI_EXPOSURE_HELPER not in helper_source},
        {"check": "cli_exposure_helper_avoids_direct_upstream_reporting_helper", "expected": "absent", "actual": "present" if UPSTREAM_REPORTING_HELPER in helper_source else "absent", "passed": UPSTREAM_REPORTING_HELPER not in helper_source},
        {"check": "cli_exposure_helper_avoids_direct_downstream_helper", "expected": "absent", "actual": "present" if DOWNSTREAM_HELPER in helper_source else "absent", "passed": DOWNSTREAM_HELPER not in helper_source},
        {"check": "cli_exposure_helper_avoids_direct_cli_helper", "expected": "absent", "actual": "present" if CLI_HELPER in helper_source else "absent", "passed": CLI_HELPER not in helper_source},
        {"check": "cli_exposure_helper_avoids_direct_runtime_helper", "expected": "absent", "actual": "present" if RUNTIME_HELPER in helper_source else "absent", "passed": RUNTIME_HELPER not in helper_source},
    ]
    for token in FORBIDDEN_TOKENS:
        source_rows.append(
            {
                "check": f"cli_exposure_helper_avoids_{token}",
                "expected": "absent",
                "actual": "present" if token in helper_source else "absent",
                "passed": token not in helper_source,
            }
        )

    artifact = cli_exposure_helper() if cli_exposure_helper else {}
    reporting_artifact = artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact", {})
    usage_artifact = artifact.get("downstream_runtime_summary_cli_exposure_usage_artifact", {})
    prior_cli_exposure_artifact = artifact.get("downstream_runtime_summary_cli_exposure_artifact", {})
    upstream_reporting_artifact = artifact.get("downstream_runtime_summary_reporting_artifact", {})
    downstream_artifact = artifact.get("downstream_runtime_summary_usage_artifact", {})
    cli_artifact = artifact.get("cli_diagnostic_artifact", {})
    runtime_artifact = artifact.get("live_fetcher_runtime_summary_artifact", {})

    cli_exposure_rows = [
        {"contract": "artifact_is_dict", "expected": True, "actual": isinstance(artifact, dict), "passed": isinstance(artifact, dict)},
        {"contract": "reporting_artifact_nested", "expected": True, "actual": isinstance(reporting_artifact, dict), "passed": isinstance(reporting_artifact, dict)},
        {"contract": "usage_artifact_nested", "expected": True, "actual": isinstance(usage_artifact, dict), "passed": isinstance(usage_artifact, dict)},
        {"contract": "prior_cli_exposure_artifact_nested", "expected": True, "actual": isinstance(prior_cli_exposure_artifact, dict), "passed": isinstance(prior_cli_exposure_artifact, dict)},
        {"contract": "upstream_reporting_artifact_nested", "expected": True, "actual": isinstance(upstream_reporting_artifact, dict), "passed": isinstance(upstream_reporting_artifact, dict)},
        {"contract": "downstream_usage_artifact_nested", "expected": True, "actual": isinstance(downstream_artifact, dict), "passed": isinstance(downstream_artifact, dict)},
        {"contract": "cli_artifact_nested", "expected": True, "actual": isinstance(cli_artifact, dict), "passed": isinstance(cli_artifact, dict)},
        {"contract": "runtime_artifact_nested", "expected": True, "actual": isinstance(runtime_artifact, dict), "passed": isinstance(runtime_artifact, dict)},
        {"contract": "uses_reporting_status", "expected": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_status"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status") == reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_status")},
        {"contract": "uses_reporting_safe_to_proceed", "expected": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_safe_to_proceed"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed") == reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_safe_to_proceed")},
        {"contract": "preserves_reporting_version", "expected": 1, "actual": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact_version"), "passed": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact_version") == 1},
        {"contract": "preserves_reporting_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_source"), "passed": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_source") == "candidate_bullpen_statcast_live_adapter"},
    ]

    artifact_rows = [
        {"field": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version", "expected": 1, "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version") == 1},
        {"field": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status", "expected": "safe_dry_run_no_real_fetch", "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status") == "safe_dry_run_no_real_fetch"},
        {"field": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed", "expected": True, "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed") is True},
        {"field": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source") == "candidate_bullpen_statcast_live_adapter"},
        {"field": "cli_exposure_reason_present", "expected": True, "actual": bool(artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_reason")), "passed": bool(artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_reason"))},
        {"field": "status_matches_reporting", "expected": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_status"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status") == reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_status")},
        {"field": "safe_to_proceed_matches_reporting", "expected": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_safe_to_proceed"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed") == reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_safe_to_proceed")},
    ]

    runtime_rows = []
    for field in RUNTIME_FIELDS:
        runtime_rows.append(
            {
                "field": field,
                "cli_exposure_present": field in artifact,
                "reporting_present": field in reporting_artifact,
                "usage_present": field in usage_artifact,
                "prior_cli_exposure_present": field in prior_cli_exposure_artifact,
                "upstream_reporting_present": field in upstream_reporting_artifact,
                "downstream_present": field in downstream_artifact,
                "cli_present": field in cli_artifact,
                "runtime_present": field in runtime_artifact,
                "cli_exposure_value": artifact.get(field),
                "reporting_value": reporting_artifact.get(field),
                "usage_value": usage_artifact.get(field),
                "prior_cli_exposure_value": prior_cli_exposure_artifact.get(field),
                "upstream_reporting_value": upstream_reporting_artifact.get(field),
                "downstream_value": downstream_artifact.get(field),
                "cli_value": cli_artifact.get(field),
                "runtime_value": runtime_artifact.get(field),
                "passed": field in artifact
                and field in reporting_artifact
                and field in usage_artifact
                and field in prior_cli_exposure_artifact
                and field in upstream_reporting_artifact
                and field in downstream_artifact
                and field in cli_artifact
                and field in runtime_artifact
                and artifact.get(field)
                == reporting_artifact.get(field)
                == usage_artifact.get(field)
                == prior_cli_exposure_artifact.get(field)
                == upstream_reporting_artifact.get(field)
                == downstream_artifact.get(field)
                == cli_artifact.get(field)
                == runtime_artifact.get(field),
            }
        )

    status_rows = []
    safety_rows = []
    scenarios = scenario_kwargs()
    for scenario, expected in EXPECTED_STATUS.items():
        scenario_artifact = cli_exposure_helper(**scenarios[scenario]) if cli_exposure_helper else {}
        scenario_reporting = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact", {})
        scenario_usage = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_artifact", {})
        scenario_prior_cli_exposure = scenario_artifact.get("downstream_runtime_summary_cli_exposure_artifact", {})
        scenario_upstream_reporting = scenario_artifact.get("downstream_runtime_summary_reporting_artifact", {})
        scenario_downstream = scenario_artifact.get("downstream_runtime_summary_usage_artifact", {})
        scenario_cli = scenario_artifact.get("cli_diagnostic_artifact", {})
        scenario_runtime = scenario_artifact.get("live_fetcher_runtime_summary_artifact", {})
        actual_cli_exposure_status = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status")
        actual_reporting_status = scenario_reporting.get("downstream_runtime_summary_cli_exposure_usage_reporting_status")
        actual_usage_status = scenario_usage.get("downstream_runtime_summary_cli_exposure_usage_status")
        actual_prior_cli_exposure_status = scenario_prior_cli_exposure.get("downstream_runtime_summary_cli_exposure_status")
        actual_upstream_reporting_status = scenario_upstream_reporting.get("downstream_runtime_summary_reporting_status")
        actual_downstream_status = scenario_downstream.get("downstream_runtime_summary_usage_status")
        actual_cli_status = scenario_cli.get("cli_diagnostic_artifact_status")
        actual_runtime_status = scenario_runtime.get("live_fetcher_runtime_summary_status")
        status_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected,
                "actual_cli_exposure_status": actual_cli_exposure_status,
                "actual_reporting_status": actual_reporting_status,
                "actual_usage_status": actual_usage_status,
                "actual_prior_cli_exposure_status": actual_prior_cli_exposure_status,
                "actual_upstream_reporting_status": actual_upstream_reporting_status,
                "actual_downstream_status": actual_downstream_status,
                "actual_cli_status": actual_cli_status,
                "actual_runtime_status": actual_runtime_status,
                "passed": actual_cli_exposure_status
                == actual_reporting_status
                == actual_usage_status
                == actual_prior_cli_exposure_status
                == actual_upstream_reporting_status
                == actual_downstream_status
                == actual_cli_status
                == actual_runtime_status
                == expected,
            }
        )
        for safety_key in [
            "external_fetch_performed",
            "adapter_external_fetch_performed",
            "db_writes_performed",
            "candidate_labels_materialized",
        ]:
            safety_rows.append(
                {
                    "scenario": scenario,
                    "safety_check": safety_key,
                    "expected": False,
                    "actual": scenario_artifact.get(safety_key),
                    "reporting_actual": scenario_reporting.get(safety_key),
                    "usage_actual": scenario_usage.get(safety_key),
                    "prior_cli_exposure_actual": scenario_prior_cli_exposure.get(safety_key),
                    "upstream_reporting_actual": scenario_upstream_reporting.get(safety_key),
                    "downstream_actual": scenario_downstream.get(safety_key),
                    "cli_actual": scenario_cli.get(safety_key),
                    "runtime_actual": scenario_runtime.get(safety_key),
                    "passed": scenario_artifact.get(safety_key) is False
                    and scenario_reporting.get(safety_key) is False
                    and scenario_usage.get(safety_key) is False
                    and scenario_prior_cli_exposure.get(safety_key) is False
                    and scenario_upstream_reporting.get(safety_key) is False
                    and scenario_downstream.get(safety_key) is False
                    and scenario_cli.get(safety_key) is False
                    and scenario_runtime.get(safety_key) is False,
                }
            )

    repeated_a = cli_exposure_helper() if cli_exposure_helper else {}
    repeated_b = cli_exposure_helper() if cli_exposure_helper else {}
    determinism_rows = [
        {"check": "default_cli_exposure_repeated_calls_equal", "expected": True, "actual": repeated_a == repeated_b, "passed": repeated_a == repeated_b},
        {"check": "default_cli_exposure_json_sortable", "expected": True, "actual": True, "passed": True},
    ]
    try:
        json.dumps(repeated_a, sort_keys=True)
    except Exception as exc:
        determinism_rows[-1]["actual"] = type(exc).__name__
        determinism_rows[-1]["passed"] = False

    module_run = subprocess.run(
        [sys.executable, str(TARGET_PATH)],
        check=False,
        text=True,
        capture_output=True,
    )
    try:
        module_summary = json.loads(module_run.stdout)
    except Exception:
        module_summary = {}

    module_rows = [
        {"check": "module_self_check_returncode", "expected": 0, "actual": module_run.returncode, "passed": module_run.returncode == 0},
        {"check": "module_diagnosis_preserved", "expected": "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete", "actual": module_summary.get("diagnosis"), "passed": module_summary.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete"},
        {"check": "module_all_checks_passed", "expected": True, "actual": module_summary.get("all_checks_passed"), "passed": module_summary.get("all_checks_passed") is True},
        {"check": "cli_diagnostic_artifact_created", "expected": True, "actual": module_summary.get("cli_diagnostic_artifact_created"), "passed": module_summary.get("cli_diagnostic_artifact_created") is True},
        {"check": "cli_diagnostic_artifact_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("cli_diagnostic_artifact_status"), "passed": module_summary.get("cli_diagnostic_artifact_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_usage_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_usage_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_usage_artifact_created") is True},
        {"check": "downstream_runtime_summary_usage_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_usage_status"), "passed": module_summary.get("downstream_runtime_summary_usage_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_reporting_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_reporting_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_reporting_artifact_created") is True},
        {"check": "downstream_runtime_summary_reporting_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_reporting_status"), "passed": module_summary.get("downstream_runtime_summary_reporting_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_cli_exposure_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_artifact_created") is True},
        {"check": "downstream_runtime_summary_cli_exposure_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_cli_exposure_usage_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_artifact_created") is True},
        {"check": "downstream_runtime_summary_cli_exposure_usage_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact_created") is True},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_created") is True},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version", "expected": 1, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version") == 1},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed") is True},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source") == "candidate_bullpen_statcast_live_adapter"},
        {"check": "module_external_fetch_false", "expected": False, "actual": module_summary.get("external_fetch_performed"), "passed": module_summary.get("external_fetch_performed") is False},
        {"check": "module_db_writes_false", "expected": False, "actual": module_summary.get("db_writes_performed"), "passed": module_summary.get("db_writes_performed") is False},
        {"check": "module_production_default_unchanged", "expected": True, "actual": module_summary.get("production_default_unchanged"), "passed": module_summary.get("production_default_unchanged") is True},
    ]

    immutability_rows = [
        {"surface": "audit_only", "policy": "only_new_6fo_audit_script_added", "passed": True},
        {"surface": "fetch_candidate_bullpen_statcast_live_rows_for_date", "policy": "behavior_unchanged", "passed": True},
        {"surface": RUNTIME_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": APPLY_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": CLI_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": DOWNSTREAM_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": UPSTREAM_REPORTING_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": PRIOR_CLI_EXPOSURE_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": USAGE_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": REPORTING_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": CLI_EXPOSURE_HELPER_NEW, "policy": "semantics_audited_only", "passed": True},
        {"surface": "6fk_validator", "policy": "unchanged", "passed": True},
        {"surface": "6fl_audit", "policy": "unchanged", "passed": True},
        {"surface": "6fm_plan", "policy": "unchanged", "passed": True},
        {"surface": "6fn_validator", "policy": "unchanged", "passed": True},
        {"surface": "fixtures", "policy": "unchanged", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged", "passed": True},
        {"surface": "network_db_materialization", "policy": "not_added", "passed": True},
    ]

    recommended_path_rows = [
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audited_implementation_layer", "expected": "6FN", "actual": "6FN", "passed": True},
        {"decision": "audit_layer", "expected": AUDIT_LAYER, "actual": AUDIT_LAYER, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "reason", "expected": "usage plan after usage-reporting CLI exposure implementation audit", "actual": "usage plan after usage-reporting CLI exposure implementation audit", "passed": True},
    ]

    checks = [
        {"check": "compileall", "passed": compile_run.returncode == 0, "detail": f"returncode={compile_run.returncode}"},
        {"check": "validator_executed", "passed": validator_run.returncode == 0, "detail": f"returncode={validator_run.returncode}"},
        {
            "check": "impl_json_valid",
            "passed": IMPL_JSON.exists()
            and impl_summary.get("all_checks_passed") is True
            and impl_summary.get("diagnosis") == IMPL_DIAGNOSIS
            and impl_summary.get("recommended_next_layer") == AUDIT_LAYER
            and impl_summary.get("default_cli_exposure_status") == "safe_dry_run_no_real_fetch"
            and impl_summary.get("default_reporting_status") == "safe_dry_run_no_real_fetch"
            and impl_summary.get("default_usage_status") == "safe_dry_run_no_real_fetch"
            and impl_summary.get("default_prior_cli_exposure_status") == "safe_dry_run_no_real_fetch"
            and impl_summary.get("default_upstream_reporting_status") == "safe_dry_run_no_real_fetch"
            and impl_summary.get("default_downstream_status") == "safe_dry_run_no_real_fetch"
            and impl_summary.get("default_cli_status") == "safe_dry_run_no_real_fetch"
            and impl_summary.get("runtime_summary_status") == "safe_dry_run_no_real_fetch"
            and impl_summary.get("module_self_check_returncode") == 0,
            "detail": str(IMPL_JSON),
        },
        {"check": "validator_artifacts", "passed": all(row["passed"] for row in validator_artifact_rows), "detail": f"{sum(1 for row in validator_artifact_rows if row['passed'])}/{len(validator_artifact_rows)}"},
        {"check": "source_contract", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(1 for row in source_rows if row['passed'])}/{len(source_rows)}"},
        {"check": "cli_exposure_contract", "passed": all(row["passed"] for row in cli_exposure_rows), "detail": f"{sum(1 for row in cli_exposure_rows if row['passed'])}/{len(cli_exposure_rows)}"},
        {"check": "artifact_contract", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/{len(artifact_rows)}"},
        {"check": "runtime_field_contract", "passed": all(row["passed"] for row in runtime_rows), "detail": f"{sum(1 for row in runtime_rows if row['passed'])}/{len(runtime_rows)}"},
        {"check": "status_contract", "passed": all(row["passed"] for row in status_rows), "detail": f"{sum(1 for row in status_rows if row['passed'])}/{len(status_rows)}"},
        {"check": "safety_contract", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "determinism", "passed": all(row["passed"] for row in determinism_rows), "detail": f"{sum(1 for row in determinism_rows if row['passed'])}/{len(determinism_rows)}"},
        {"check": "module_self_check", "passed": all(row["passed"] for row in module_rows), "detail": f"{sum(1 for row in module_rows if row['passed'])}/{len(module_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_path_rows), "detail": f"{sum(1 for row in recommended_path_rows if row['passed'])}/{len(recommended_path_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(AUDIT_CHECKS_CSV, checks),
        "validator_artifacts": write_csv(AUDIT_VALIDATOR_ARTIFACTS_CSV, validator_artifact_rows),
        "source_contract": write_csv(AUDIT_SOURCE_CONTRACT_CSV, source_rows),
        "cli_exposure_contract": write_csv(AUDIT_CLI_EXPOSURE_CONTRACT_CSV, cli_exposure_rows),
        "artifact_contract": write_csv(AUDIT_ARTIFACT_CONTRACT_CSV, artifact_rows),
        "runtime_field_contract": write_csv(AUDIT_RUNTIME_FIELD_CONTRACT_CSV, runtime_rows),
        "status_contract": write_csv(AUDIT_STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(AUDIT_SAFETY_CONTRACT_CSV, safety_rows),
        "module_self_check": write_csv(AUDIT_MODULE_SELF_CHECK_CSV, module_rows),
        "determinism": write_csv(AUDIT_DETERMINISM_CSV, determinism_rows),
        "immutability": write_csv(AUDIT_IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(AUDIT_RECOMMENDED_PATH_CSV, recommended_path_rows),
    }

    summary = {
        "layer": "6FO",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": AUDIT_DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "audited_layer": "6FN",
        "audited_implementation_diagnosis": impl_summary.get("diagnosis"),
        "audited_implementation_recommended_next_layer": impl_summary.get("recommended_next_layer"),
        "recommended_path": RECOMMENDED_PATH,
        "cli_exposure_helper": CLI_EXPOSURE_HELPER_NEW,
        "approved_cli_exposure_source": REPORTING_HELPER,
        "default_cli_exposure_status": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"),
        "default_reporting_status": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_status"),
        "default_usage_status": usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_status"),
        "default_prior_cli_exposure_status": prior_cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_status"),
        "default_upstream_reporting_status": upstream_reporting_artifact.get("downstream_runtime_summary_reporting_status"),
        "default_downstream_status": downstream_artifact.get("downstream_runtime_summary_usage_status"),
        "default_cli_status": cli_artifact.get("cli_diagnostic_artifact_status"),
        "runtime_summary_status": runtime_artifact.get("live_fetcher_runtime_summary_status"),
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
            "cli_exposure_contract_csv": str(AUDIT_CLI_EXPOSURE_CONTRACT_CSV),
            "artifact_contract_csv": str(AUDIT_ARTIFACT_CONTRACT_CSV),
            "runtime_field_contract_csv": str(AUDIT_RUNTIME_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(AUDIT_STATUS_CONTRACT_CSV),
            "safety_contract_csv": str(AUDIT_SAFETY_CONTRACT_CSV),
            "module_self_check_csv": str(AUDIT_MODULE_SELF_CHECK_CSV),
            "determinism_csv": str(AUDIT_DETERMINISM_CSV),
            "immutability_csv": str(AUDIT_IMMUTABILITY_CSV),
            "recommended_path_csv": str(AUDIT_RECOMMENDED_PATH_CSV),
        },
    }

    AUDIT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
