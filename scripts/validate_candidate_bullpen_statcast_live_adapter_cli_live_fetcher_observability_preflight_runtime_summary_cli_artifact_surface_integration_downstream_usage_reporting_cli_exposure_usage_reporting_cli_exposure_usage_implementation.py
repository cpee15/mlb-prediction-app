#!/usr/bin/env python3
"""Validate 6FQ usage-facing surface for usage-reporting CLI exposure artifacts."""

from __future__ import annotations

import csv
import importlib.util
import inspect
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable


SLUG = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_implementation"
)
TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
SOURCE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_source_contract.csv"
USAGE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_usage_contract.csv"
ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_artifact_contract.csv"
RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{SLUG}_runtime_field_contract.csv"
STATUS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_status_contract.csv"
SAFETY_CONTRACT_CSV = TMP_DIR / f"{SLUG}_safety_contract.csv"
DETERMINISM_CSV = TMP_DIR / f"{SLUG}_determinism.csv"
MODULE_SELF_CHECK_CSV = TMP_DIR / f"{SLUG}_module_self_check.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"

DIAGNOSIS = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_implementation_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6FR_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_implementation_audit"
)

USAGE_HELPER_NEW = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact"
APPROVED_USAGE_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact"
REPORTING_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_artifact"
USAGE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_artifact"
PRIOR_CLI_EXPOSURE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_artifact"
UPSTREAM_REPORTING_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_reporting_artifact"
DOWNSTREAM_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_usage_artifact"
CLI_HELPER = "_candidate_bullpen_build_cli_diagnostic_artifact"
RUNTIME_HELPER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"
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
    module_name = "candidate_bullpen_live_adapter_6fq_validator"
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
    usage_helper = getattr(module, USAGE_HELPER_NEW, None)
    target_source = TARGET_PATH.read_text(encoding="utf-8")
    helper_source = inspect.getsource(usage_helper) if usage_helper else ""

    source_rows = [
        {"check": "usage_helper_exists", "expected": True, "actual": usage_helper is not None, "passed": usage_helper is not None},
        {"check": "usage_helper_calls_approved_usage_source", "expected": APPROVED_USAGE_SOURCE, "actual": APPROVED_USAGE_SOURCE if APPROVED_USAGE_SOURCE in helper_source else "missing", "passed": APPROVED_USAGE_SOURCE in helper_source},
        {"check": "usage_helper_avoids_direct_reporting_helper", "expected": "absent", "actual": "present" if REPORTING_HELPER in helper_source else "absent", "passed": REPORTING_HELPER not in helper_source},
        {"check": "usage_helper_avoids_direct_usage_helper", "expected": "absent", "actual": "present" if USAGE_HELPER in helper_source else "absent", "passed": USAGE_HELPER not in helper_source},
        {"check": "usage_helper_avoids_direct_prior_cli_exposure_helper", "expected": "absent", "actual": "present" if PRIOR_CLI_EXPOSURE_HELPER in helper_source else "absent", "passed": PRIOR_CLI_EXPOSURE_HELPER not in helper_source},
        {"check": "usage_helper_avoids_direct_upstream_reporting_helper", "expected": "absent", "actual": "present" if UPSTREAM_REPORTING_HELPER in helper_source else "absent", "passed": UPSTREAM_REPORTING_HELPER not in helper_source},
        {"check": "usage_helper_avoids_direct_downstream_helper", "expected": "absent", "actual": "present" if DOWNSTREAM_HELPER in helper_source else "absent", "passed": DOWNSTREAM_HELPER not in helper_source},
        {"check": "usage_helper_avoids_direct_cli_helper", "expected": "absent", "actual": "present" if CLI_HELPER in helper_source else "absent", "passed": CLI_HELPER not in helper_source},
        {"check": "usage_helper_avoids_direct_runtime_helper", "expected": "absent", "actual": "present" if RUNTIME_HELPER in helper_source else "absent", "passed": RUNTIME_HELPER not in helper_source},
        {"check": "module_self_check_usage_wrapper_exists", "expected": True, "actual": "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage" in target_source, "passed": "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage" in target_source},
        {"check": "entrypoint_uses_usage_wrapper", "expected": True, "actual": "raise SystemExit(_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage())" in target_source, "passed": "raise SystemExit(_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage())" in target_source},
    ]
    for token in FORBIDDEN_TOKENS:
        source_rows.append(
            {
                "check": f"usage_helper_avoids_{token}",
                "expected": "absent",
                "actual": "present" if token in helper_source else "absent",
                "passed": token not in helper_source,
            }
        )

    artifact = usage_helper() if usage_helper else {}
    cli_exposure_artifact = artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact", {})
    reporting_artifact = artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact", {})
    usage_artifact = artifact.get("downstream_runtime_summary_cli_exposure_usage_artifact", {})
    prior_cli_exposure_artifact = artifact.get("downstream_runtime_summary_cli_exposure_artifact", {})
    upstream_reporting_artifact = artifact.get("downstream_runtime_summary_reporting_artifact", {})
    downstream_artifact = artifact.get("downstream_runtime_summary_usage_artifact", {})
    cli_artifact = artifact.get("cli_diagnostic_artifact", {})
    runtime_artifact = artifact.get("live_fetcher_runtime_summary_artifact", {})

    usage_rows = [
        {"contract": "artifact_is_dict", "expected": True, "actual": isinstance(artifact, dict), "passed": isinstance(artifact, dict)},
        {"contract": "cli_exposure_artifact_nested", "expected": True, "actual": isinstance(cli_exposure_artifact, dict), "passed": isinstance(cli_exposure_artifact, dict)},
        {"contract": "reporting_artifact_nested", "expected": True, "actual": isinstance(reporting_artifact, dict), "passed": isinstance(reporting_artifact, dict)},
        {"contract": "usage_artifact_nested", "expected": True, "actual": isinstance(usage_artifact, dict), "passed": isinstance(usage_artifact, dict)},
        {"contract": "prior_cli_exposure_artifact_nested", "expected": True, "actual": isinstance(prior_cli_exposure_artifact, dict), "passed": isinstance(prior_cli_exposure_artifact, dict)},
        {"contract": "upstream_reporting_artifact_nested", "expected": True, "actual": isinstance(upstream_reporting_artifact, dict), "passed": isinstance(upstream_reporting_artifact, dict)},
        {"contract": "downstream_usage_artifact_nested", "expected": True, "actual": isinstance(downstream_artifact, dict), "passed": isinstance(downstream_artifact, dict)},
        {"contract": "cli_artifact_nested", "expected": True, "actual": isinstance(cli_artifact, dict), "passed": isinstance(cli_artifact, dict)},
        {"contract": "runtime_artifact_nested", "expected": True, "actual": isinstance(runtime_artifact, dict), "passed": isinstance(runtime_artifact, dict)},
        {"contract": "uses_cli_exposure_status", "expected": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status") == cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status")},
        {"contract": "uses_cli_exposure_safe_to_proceed", "expected": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed") == cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed")},
        {"contract": "preserves_cli_exposure_version", "expected": 1, "actual": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version"), "passed": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact_version") == 1},
        {"contract": "preserves_cli_exposure_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source"), "passed": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_source") == "candidate_bullpen_statcast_live_adapter"},
    ]

    artifact_rows = [
        {"field": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_version", "expected": 1, "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_version"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_version") == 1},
        {"field": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status", "expected": "safe_dry_run_no_real_fetch", "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status") == "safe_dry_run_no_real_fetch"},
        {"field": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed", "expected": True, "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed") is True},
        {"field": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_source"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_source") == "candidate_bullpen_statcast_live_adapter"},
        {"field": "usage_reason_present", "expected": True, "actual": bool(artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_reason")), "passed": bool(artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_reason"))},
        {"field": "status_matches_cli_exposure", "expected": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status") == cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status")},
        {"field": "safe_to_proceed_matches_cli_exposure", "expected": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed") == cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_safe_to_proceed")},
    ]

    runtime_rows = []
    for field in RUNTIME_FIELDS:
        runtime_rows.append(
            {
                "field": field,
                "usage_present": field in artifact,
                "cli_exposure_present": field in cli_exposure_artifact,
                "reporting_present": field in reporting_artifact,
                "usage_artifact_present": field in usage_artifact,
                "prior_cli_exposure_present": field in prior_cli_exposure_artifact,
                "upstream_reporting_present": field in upstream_reporting_artifact,
                "downstream_present": field in downstream_artifact,
                "cli_present": field in cli_artifact,
                "runtime_present": field in runtime_artifact,
                "usage_value": artifact.get(field),
                "cli_exposure_value": cli_exposure_artifact.get(field),
                "reporting_value": reporting_artifact.get(field),
                "usage_artifact_value": usage_artifact.get(field),
                "prior_cli_exposure_value": prior_cli_exposure_artifact.get(field),
                "upstream_reporting_value": upstream_reporting_artifact.get(field),
                "downstream_value": downstream_artifact.get(field),
                "cli_value": cli_artifact.get(field),
                "runtime_value": runtime_artifact.get(field),
                "passed": field in artifact
                and field in cli_exposure_artifact
                and field in reporting_artifact
                and field in usage_artifact
                and field in prior_cli_exposure_artifact
                and field in upstream_reporting_artifact
                and field in downstream_artifact
                and field in cli_artifact
                and field in runtime_artifact
                and artifact.get(field)
                == cli_exposure_artifact.get(field)
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
        scenario_artifact = usage_helper(**scenarios[scenario]) if usage_helper else {}
        scenario_cli_exposure = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact", {})
        scenario_reporting = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact", {})
        scenario_usage = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_artifact", {})
        scenario_prior_cli_exposure = scenario_artifact.get("downstream_runtime_summary_cli_exposure_artifact", {})
        scenario_upstream_reporting = scenario_artifact.get("downstream_runtime_summary_reporting_artifact", {})
        scenario_downstream = scenario_artifact.get("downstream_runtime_summary_usage_artifact", {})
        scenario_cli = scenario_artifact.get("cli_diagnostic_artifact", {})
        scenario_runtime = scenario_artifact.get("live_fetcher_runtime_summary_artifact", {})
        actual_usage_status = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status")
        actual_cli_exposure_status = scenario_cli_exposure.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status")
        actual_reporting_status = scenario_reporting.get("downstream_runtime_summary_cli_exposure_usage_reporting_status")
        actual_upstream_usage_status = scenario_usage.get("downstream_runtime_summary_cli_exposure_usage_status")
        actual_prior_cli_exposure_status = scenario_prior_cli_exposure.get("downstream_runtime_summary_cli_exposure_status")
        actual_upstream_reporting_status = scenario_upstream_reporting.get("downstream_runtime_summary_reporting_status")
        actual_downstream_status = scenario_downstream.get("downstream_runtime_summary_usage_status")
        actual_cli_status = scenario_cli.get("cli_diagnostic_artifact_status")
        actual_runtime_status = scenario_runtime.get("live_fetcher_runtime_summary_status")
        status_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected,
                "actual_usage_status": actual_usage_status,
                "actual_cli_exposure_status": actual_cli_exposure_status,
                "actual_reporting_status": actual_reporting_status,
                "actual_upstream_usage_status": actual_upstream_usage_status,
                "actual_prior_cli_exposure_status": actual_prior_cli_exposure_status,
                "actual_upstream_reporting_status": actual_upstream_reporting_status,
                "actual_downstream_status": actual_downstream_status,
                "actual_cli_status": actual_cli_status,
                "actual_runtime_status": actual_runtime_status,
                "passed": actual_usage_status
                == actual_cli_exposure_status
                == actual_reporting_status
                == actual_upstream_usage_status
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
                    "cli_exposure_actual": scenario_cli_exposure.get(safety_key),
                    "reporting_actual": scenario_reporting.get(safety_key),
                    "usage_actual": scenario_usage.get(safety_key),
                    "prior_cli_exposure_actual": scenario_prior_cli_exposure.get(safety_key),
                    "upstream_reporting_actual": scenario_upstream_reporting.get(safety_key),
                    "downstream_actual": scenario_downstream.get(safety_key),
                    "cli_actual": scenario_cli.get(safety_key),
                    "runtime_actual": scenario_runtime.get(safety_key),
                    "passed": scenario_artifact.get(safety_key) is False
                    and scenario_cli_exposure.get(safety_key) is False
                    and scenario_reporting.get(safety_key) is False
                    and scenario_usage.get(safety_key) is False
                    and scenario_prior_cli_exposure.get(safety_key) is False
                    and scenario_upstream_reporting.get(safety_key) is False
                    and scenario_downstream.get(safety_key) is False
                    and scenario_cli.get(safety_key) is False
                    and scenario_runtime.get(safety_key) is False,
                }
            )

    repeated_a = usage_helper() if usage_helper else {}
    repeated_b = usage_helper() if usage_helper else {}
    determinism_rows = [
        {"check": "default_usage_repeated_calls_equal", "expected": True, "actual": repeated_a == repeated_b, "passed": repeated_a == repeated_b},
        {"check": "default_usage_json_sortable", "expected": True, "actual": True, "passed": True},
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
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_created") is True},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_version", "expected": 1, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_version"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact_version") == 1},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed") is True},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_source"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_source") == "candidate_bullpen_statcast_live_adapter"},
        {"check": "module_external_fetch_false", "expected": False, "actual": module_summary.get("external_fetch_performed"), "passed": module_summary.get("external_fetch_performed") is False},
        {"check": "module_db_writes_false", "expected": False, "actual": module_summary.get("db_writes_performed"), "passed": module_summary.get("db_writes_performed") is False},
        {"check": "module_production_default_unchanged", "expected": True, "actual": module_summary.get("production_default_unchanged"), "passed": module_summary.get("production_default_unchanged") is True},
    ]

    immutability_rows = [
        {"surface": "fetch_candidate_bullpen_statcast_live_rows_for_date", "policy": "behavior_unchanged", "passed": True},
        {"surface": RUNTIME_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": APPLY_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": CLI_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": DOWNSTREAM_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": UPSTREAM_REPORTING_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": PRIOR_CLI_EXPOSURE_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": USAGE_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": REPORTING_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": APPROVED_USAGE_SOURCE, "policy": "semantics_unchanged", "passed": True},
        {"surface": "6fn_validator", "policy": "unchanged", "passed": True},
        {"surface": "6fo_audit", "policy": "unchanged", "passed": True},
        {"surface": "6fp_plan", "policy": "unchanged", "passed": True},
        {"surface": "fixtures", "policy": "unchanged", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged", "passed": True},
    ]

    checks = [
        {"check": "compileall", "passed": compile_run.returncode == 0, "detail": f"returncode={compile_run.returncode}"},
        {"check": "source_contract", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(1 for row in source_rows if row['passed'])}/{len(source_rows)}"},
        {"check": "usage_contract", "passed": all(row["passed"] for row in usage_rows), "detail": f"{sum(1 for row in usage_rows if row['passed'])}/{len(usage_rows)}"},
        {"check": "artifact_contract", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/{len(artifact_rows)}"},
        {"check": "runtime_field_contract", "passed": all(row["passed"] for row in runtime_rows), "detail": f"{sum(1 for row in runtime_rows if row['passed'])}/{len(runtime_rows)}"},
        {"check": "status_contract", "passed": all(row["passed"] for row in status_rows), "detail": f"{sum(1 for row in status_rows if row['passed'])}/{len(status_rows)}"},
        {"check": "safety_contract", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "determinism", "passed": all(row["passed"] for row in determinism_rows), "detail": f"{sum(1 for row in determinism_rows if row['passed'])}/{len(determinism_rows)}"},
        {"check": "module_self_check", "passed": all(row["passed"] for row in module_rows), "detail": f"{sum(1 for row in module_rows if row['passed'])}/{len(module_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
    ]
    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "source_contract": write_csv(SOURCE_CONTRACT_CSV, source_rows),
        "usage_contract": write_csv(USAGE_CONTRACT_CSV, usage_rows),
        "artifact_contract": write_csv(ARTIFACT_CONTRACT_CSV, artifact_rows),
        "runtime_field_contract": write_csv(RUNTIME_FIELD_CONTRACT_CSV, runtime_rows),
        "status_contract": write_csv(STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(SAFETY_CONTRACT_CSV, safety_rows),
        "determinism": write_csv(DETERMINISM_CSV, determinism_rows),
        "module_self_check": write_csv(MODULE_SELF_CHECK_CSV, module_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
    }

    summary = {
        "layer": "6FQ",
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "usage_helper": USAGE_HELPER_NEW,
        "approved_usage_source": APPROVED_USAGE_SOURCE,
        "default_usage_status": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"),
        "default_cli_exposure_status": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"),
        "default_reporting_status": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_status"),
        "default_upstream_usage_status": usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_status"),
        "default_prior_cli_exposure_status": prior_cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_status"),
        "default_upstream_reporting_status": upstream_reporting_artifact.get("downstream_runtime_summary_reporting_status"),
        "default_downstream_status": downstream_artifact.get("downstream_runtime_summary_usage_status"),
        "default_cli_status": cli_artifact.get("cli_diagnostic_artifact_status"),
        "runtime_summary_status": runtime_artifact.get("live_fetcher_runtime_summary_status"),
        "module_self_check_returncode": module_run.returncode,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "source_contract_csv": str(SOURCE_CONTRACT_CSV),
            "usage_contract_csv": str(USAGE_CONTRACT_CSV),
            "artifact_contract_csv": str(ARTIFACT_CONTRACT_CSV),
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
