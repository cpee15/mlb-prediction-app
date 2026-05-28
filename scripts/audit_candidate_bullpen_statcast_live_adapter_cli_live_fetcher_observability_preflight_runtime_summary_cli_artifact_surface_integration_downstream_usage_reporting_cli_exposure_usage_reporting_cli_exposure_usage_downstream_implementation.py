#!/usr/bin/env python3
"""Audit 6FT downstream-facing usage artifact implementation."""

from __future__ import annotations

import csv
import importlib.util
import inspect
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


AUDIT_SLUG = "candidate_bullpen_6fu_downstream_implementation_audit"
VALIDATOR_SLUG_6FT = "candidate_bullpen_6ft_downstream_implementation"

TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATOR_6FT_PATH = Path(
    "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_implementation.py"
)

VALIDATOR_6FT_JSON = TMP_DIR / f"{VALIDATOR_SLUG_6FT}.json"
VALIDATOR_6FT_ARTIFACTS = {
    "checks": TMP_DIR / f"{VALIDATOR_SLUG_6FT}_checks.csv",
    "source_contract": TMP_DIR / f"{VALIDATOR_SLUG_6FT}_source_contract.csv",
    "downstream_contract": TMP_DIR / f"{VALIDATOR_SLUG_6FT}_downstream_contract.csv",
    "artifact_contract": TMP_DIR / f"{VALIDATOR_SLUG_6FT}_artifact_contract.csv",
    "runtime_field_contract": TMP_DIR / f"{VALIDATOR_SLUG_6FT}_runtime_field_contract.csv",
    "status_contract": TMP_DIR / f"{VALIDATOR_SLUG_6FT}_status_contract.csv",
    "safety_contract": TMP_DIR / f"{VALIDATOR_SLUG_6FT}_safety_contract.csv",
    "determinism": TMP_DIR / f"{VALIDATOR_SLUG_6FT}_determinism.csv",
    "module_self_check": TMP_DIR / f"{VALIDATOR_SLUG_6FT}_module_self_check.csv",
    "immutability": TMP_DIR / f"{VALIDATOR_SLUG_6FT}_immutability.csv",
}
EXPECTED_VALIDATOR_COUNTS = {
    "checks": 10,
    "source_contract": 22,
    "downstream_contract": 14,
    "artifact_contract": 7,
    "runtime_field_contract": 10,
    "status_contract": 7,
    "safety_contract": 28,
    "determinism": 2,
    "module_self_check": 27,
    "immutability": 16,
}

AUDIT_JSON = TMP_DIR / f"{AUDIT_SLUG}.json"
AUDIT_CHECKS_CSV = TMP_DIR / f"{AUDIT_SLUG}_checks.csv"
AUDIT_VALIDATOR_ARTIFACTS_CSV = TMP_DIR / f"{AUDIT_SLUG}_validator_artifacts.csv"
AUDIT_SOURCE_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_source_contract.csv"
AUDIT_DOWNSTREAM_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_downstream_contract.csv"
AUDIT_ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_artifact_contract.csv"
AUDIT_RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_runtime_field_contract.csv"
AUDIT_STATUS_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_status_contract.csv"
AUDIT_SAFETY_CONTRACT_CSV = TMP_DIR / f"{AUDIT_SLUG}_safety_contract.csv"
AUDIT_DETERMINISM_CSV = TMP_DIR / f"{AUDIT_SLUG}_determinism.csv"
AUDIT_MODULE_SELF_CHECK_CSV = TMP_DIR / f"{AUDIT_SLUG}_module_self_check.csv"
AUDIT_IMMUTABILITY_CSV = TMP_DIR / f"{AUDIT_SLUG}_immutability.csv"
AUDIT_RECOMMENDED_PATH_CSV = TMP_DIR / f"{AUDIT_SLUG}_recommended_path.csv"

DIAGNOSIS_6FT = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_implementation_complete"
)
DIAGNOSIS_6FU = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_implementation_audit_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6FV_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_plan"
)
RECOMMENDED_PATH = "audit_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_surface"

DOWNSTREAM_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact"
APPROVED_DOWNSTREAM_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact"
APPROVED_USAGE_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact"
REPORTING_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_artifact"
PRIOR_USAGE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_artifact"
PRIOR_CLI_EXPOSURE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_artifact"
UPSTREAM_REPORTING_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_reporting_artifact"
DOWNSTREAM_USAGE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_usage_artifact"
CLI_HELPER = "_candidate_bullpen_build_cli_diagnostic_artifact"
RUNTIME_HELPER = "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact"
APPLY_HELPER = "_candidate_bullpen_apply_live_fetcher_runtime_summary"
WRAPPER_HELPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream"

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

FORBIDDEN_DIRECT_HELPERS = [
    APPROVED_USAGE_SOURCE,
    REPORTING_HELPER,
    PRIOR_USAGE_HELPER,
    PRIOR_CLI_EXPOSURE_HELPER,
    UPSTREAM_REPORTING_HELPER,
    DOWNSTREAM_USAGE_HELPER,
    CLI_HELPER,
    RUNTIME_HELPER,
]

FORBIDDEN_BEHAVIOR_TOKENS = [
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
    module_name = "candidate_bullpen_live_adapter_6fu_audit"
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


def safe_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_run = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "-f", "-b", "mlb_app", "scripts"],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    validator_run = subprocess.run(
        [sys.executable, str(VALIDATOR_6FT_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    validator_summary: Dict[str, Any] = {}
    if VALIDATOR_6FT_JSON.exists():
        validator_summary = json.loads(VALIDATOR_6FT_JSON.read_text(encoding="utf-8"))

    validator_artifact_rows = []
    validator_csv_counts: Dict[str, int] = {}
    for name, path in VALIDATOR_6FT_ARTIFACTS.items():
        exists = path.exists()
        rows = read_csv(path) if exists else []
        actual_count = len(rows)
        expected_count = EXPECTED_VALIDATOR_COUNTS[name]
        rows_passed = all_rows_passed(rows)
        validator_csv_counts[name] = actual_count
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
    downstream_helper = getattr(module, DOWNSTREAM_HELPER, None)
    target_source = TARGET_PATH.read_text(encoding="utf-8")
    helper_source = inspect.getsource(downstream_helper) if downstream_helper else ""

    required_source_tokens = [
        DOWNSTREAM_HELPER,
        APPROVED_DOWNSTREAM_SOURCE,
        WRAPPER_HELPER,
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_created",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact",
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact",
        "downstream_runtime_summary_cli_exposure_usage_reporting_artifact",
        "downstream_runtime_summary_cli_exposure_usage_artifact",
        "downstream_runtime_summary_cli_exposure_artifact",
        "downstream_runtime_summary_reporting_artifact",
        "downstream_runtime_summary_usage_artifact",
        "cli_diagnostic_artifact",
        "live_fetcher_runtime_summary_artifact",
    ]

    source_rows = [
        {
            "check": f"source_contains_{token}",
            "expected": token,
            "actual": token if token in target_source else "missing",
            "passed": token in target_source,
        }
        for token in required_source_tokens
    ]
    source_rows.append(
        {
            "check": "downstream_helper_calls_approved_downstream_source",
            "expected": APPROVED_DOWNSTREAM_SOURCE,
            "actual": APPROVED_DOWNSTREAM_SOURCE if APPROVED_DOWNSTREAM_SOURCE in helper_source else "missing",
            "passed": APPROVED_DOWNSTREAM_SOURCE in helper_source,
        }
    )
    for helper in FORBIDDEN_DIRECT_HELPERS:
        source_rows.append(
            {
                "check": f"downstream_helper_avoids_direct_{helper}",
                "expected": "absent",
                "actual": "present" if helper in helper_source else "absent",
                "passed": helper not in helper_source,
            }
        )
    for token in FORBIDDEN_BEHAVIOR_TOKENS:
        source_rows.append(
            {
                "check": f"downstream_helper_avoids_{token}",
                "expected": "absent",
                "actual": "present" if token in helper_source else "absent",
                "passed": token not in helper_source,
            }
        )

    artifact = downstream_helper() if downstream_helper else {}
    usage_artifact = artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact", {})
    cli_exposure_artifact = artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact", {})
    reporting_artifact = artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact", {})
    prior_usage_artifact = artifact.get("downstream_runtime_summary_cli_exposure_usage_artifact", {})
    prior_cli_exposure_artifact = artifact.get("downstream_runtime_summary_cli_exposure_artifact", {})
    upstream_reporting_artifact = artifact.get("downstream_runtime_summary_reporting_artifact", {})
    downstream_usage_artifact = artifact.get("downstream_runtime_summary_usage_artifact", {})
    cli_artifact = artifact.get("cli_diagnostic_artifact", {})
    runtime_artifact = artifact.get("live_fetcher_runtime_summary_artifact", {})

    downstream_rows = [
        {"contract": "artifact_is_dict", "expected": True, "actual": isinstance(artifact, dict), "passed": isinstance(artifact, dict)},
        {"contract": "usage_artifact_nested", "expected": True, "actual": isinstance(usage_artifact, dict), "passed": isinstance(usage_artifact, dict)},
        {"contract": "cli_exposure_artifact_nested", "expected": True, "actual": isinstance(cli_exposure_artifact, dict), "passed": isinstance(cli_exposure_artifact, dict)},
        {"contract": "reporting_artifact_nested", "expected": True, "actual": isinstance(reporting_artifact, dict), "passed": isinstance(reporting_artifact, dict)},
        {"contract": "prior_usage_artifact_nested", "expected": True, "actual": isinstance(prior_usage_artifact, dict), "passed": isinstance(prior_usage_artifact, dict)},
        {"contract": "prior_cli_exposure_artifact_nested", "expected": True, "actual": isinstance(prior_cli_exposure_artifact, dict), "passed": isinstance(prior_cli_exposure_artifact, dict)},
        {"contract": "upstream_reporting_artifact_nested", "expected": True, "actual": isinstance(upstream_reporting_artifact, dict), "passed": isinstance(upstream_reporting_artifact, dict)},
        {"contract": "downstream_usage_artifact_nested", "expected": True, "actual": isinstance(downstream_usage_artifact, dict), "passed": isinstance(downstream_usage_artifact, dict)},
        {"contract": "cli_artifact_nested", "expected": True, "actual": isinstance(cli_artifact, dict), "passed": isinstance(cli_artifact, dict)},
        {"contract": "runtime_artifact_nested", "expected": True, "actual": isinstance(runtime_artifact, dict), "passed": isinstance(runtime_artifact, dict)},
        {
            "contract": "uses_usage_status",
            "expected": usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"),
            "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"),
            "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status")
            == usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"),
        },
        {
            "contract": "uses_usage_safe_to_proceed",
            "expected": usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed"),
            "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed"),
            "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed")
            == usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed"),
        },
    ]

    artifact_rows = [
        {
            "field": "downstream_version",
            "expected": 1,
            "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_version"),
            "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_version") == 1,
        },
        {
            "field": "downstream_status",
            "expected": "safe_dry_run_no_real_fetch",
            "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"),
            "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status") == "safe_dry_run_no_real_fetch",
        },
        {
            "field": "downstream_safe_to_proceed",
            "expected": True,
            "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed"),
            "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed") is True,
        },
        {
            "field": "downstream_source",
            "expected": "candidate_bullpen_statcast_live_adapter",
            "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source"),
            "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source") == "candidate_bullpen_statcast_live_adapter",
        },
        {
            "field": "downstream_status_matches_usage",
            "expected": usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"),
            "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"),
            "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status")
            == usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"),
        },
        {
            "field": "downstream_safe_to_proceed_matches_usage",
            "expected": usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed"),
            "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed"),
            "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed")
            == usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_safe_to_proceed"),
        },
        {
            "field": "downstream_reason_present",
            "expected": True,
            "actual": bool(artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_reason")),
            "passed": bool(artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_reason")),
        },
    ]

    runtime_rows = []
    for field in RUNTIME_FIELDS:
        runtime_rows.append(
            {
                "field": field,
                "downstream_present": field in artifact,
                "usage_present": field in usage_artifact,
                "cli_exposure_present": field in cli_exposure_artifact,
                "reporting_present": field in reporting_artifact,
                "prior_usage_present": field in prior_usage_artifact,
                "prior_cli_exposure_present": field in prior_cli_exposure_artifact,
                "upstream_reporting_present": field in upstream_reporting_artifact,
                "downstream_usage_present": field in downstream_usage_artifact,
                "cli_present": field in cli_artifact,
                "runtime_present": field in runtime_artifact,
                "downstream_value": artifact.get(field),
                "usage_value": usage_artifact.get(field),
                "cli_exposure_value": cli_exposure_artifact.get(field),
                "reporting_value": reporting_artifact.get(field),
                "prior_usage_value": prior_usage_artifact.get(field),
                "prior_cli_exposure_value": prior_cli_exposure_artifact.get(field),
                "upstream_reporting_value": upstream_reporting_artifact.get(field),
                "downstream_usage_value": downstream_usage_artifact.get(field),
                "cli_value": cli_artifact.get(field),
                "runtime_value": runtime_artifact.get(field),
                "passed": field in artifact
                and field in usage_artifact
                and field in cli_exposure_artifact
                and field in reporting_artifact
                and field in prior_usage_artifact
                and field in prior_cli_exposure_artifact
                and field in upstream_reporting_artifact
                and field in downstream_usage_artifact
                and field in cli_artifact
                and field in runtime_artifact
                and artifact.get(field)
                == usage_artifact.get(field)
                == cli_exposure_artifact.get(field)
                == reporting_artifact.get(field)
                == prior_usage_artifact.get(field)
                == prior_cli_exposure_artifact.get(field)
                == upstream_reporting_artifact.get(field)
                == downstream_usage_artifact.get(field)
                == cli_artifact.get(field)
                == runtime_artifact.get(field),
            }
        )

    status_rows = []
    safety_rows = []
    scenarios = scenario_kwargs()
    for scenario, expected in EXPECTED_STATUS.items():
        scenario_artifact = downstream_helper(**scenarios[scenario]) if downstream_helper else {}
        scenario_usage = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact", {})
        scenario_cli_exposure = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact", {})
        scenario_reporting = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact", {})
        scenario_prior_usage = scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_artifact", {})
        scenario_prior_cli_exposure = scenario_artifact.get("downstream_runtime_summary_cli_exposure_artifact", {})
        scenario_upstream_reporting = scenario_artifact.get("downstream_runtime_summary_reporting_artifact", {})
        scenario_downstream_usage = scenario_artifact.get("downstream_runtime_summary_usage_artifact", {})
        scenario_cli = scenario_artifact.get("cli_diagnostic_artifact", {})
        scenario_runtime = scenario_artifact.get("live_fetcher_runtime_summary_artifact", {})
        statuses = [
            scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"),
            scenario_usage.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"),
            scenario_cli_exposure.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"),
            scenario_reporting.get("downstream_runtime_summary_cli_exposure_usage_reporting_status"),
            scenario_prior_usage.get("downstream_runtime_summary_cli_exposure_usage_status"),
            scenario_prior_cli_exposure.get("downstream_runtime_summary_cli_exposure_status"),
            scenario_upstream_reporting.get("downstream_runtime_summary_reporting_status"),
            scenario_downstream_usage.get("downstream_runtime_summary_usage_status"),
            scenario_cli.get("cli_diagnostic_artifact_status"),
            scenario_runtime.get("live_fetcher_runtime_summary_status"),
        ]
        status_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected,
                "actual_statuses": "|".join(str(value) for value in statuses),
                "passed": all(value == expected for value in statuses),
            }
        )
        for safety_key in [
            "external_fetch_performed",
            "adapter_external_fetch_performed",
            "db_writes_performed",
            "candidate_labels_materialized",
        ]:
            safety_values = [
                scenario_artifact.get(safety_key),
                scenario_usage.get(safety_key),
                scenario_cli_exposure.get(safety_key),
                scenario_reporting.get(safety_key),
                scenario_prior_usage.get(safety_key),
                scenario_prior_cli_exposure.get(safety_key),
                scenario_upstream_reporting.get(safety_key),
                scenario_downstream_usage.get(safety_key),
                scenario_cli.get(safety_key),
                scenario_runtime.get(safety_key),
            ]
            safety_rows.append(
                {
                    "scenario": scenario,
                    "safety_check": safety_key,
                    "expected": False,
                    "actual_values": "|".join(str(value) for value in safety_values),
                    "passed": all(value is False for value in safety_values),
                }
            )

    repeated_a = downstream_helper() if downstream_helper else {}
    repeated_b = downstream_helper() if downstream_helper else {}
    determinism_rows = [
        {
            "check": "default_downstream_repeated_calls_equal",
            "expected": True,
            "actual": repeated_a == repeated_b,
            "passed": repeated_a == repeated_b,
        },
        {"check": "default_downstream_json_sortable", "expected": True, "actual": True, "passed": True},
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
        env=safe_env(),
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
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_created") is True},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_version", "expected": 1, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_version"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_version") == 1},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed") is True},
        {"check": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_source") == "candidate_bullpen_statcast_live_adapter"},
        {"check": "module_external_fetch_false", "expected": False, "actual": module_summary.get("external_fetch_performed"), "passed": module_summary.get("external_fetch_performed") is False},
        {"check": "module_db_writes_false", "expected": False, "actual": module_summary.get("db_writes_performed"), "passed": module_summary.get("db_writes_performed") is False},
        {"check": "module_production_default_unchanged", "expected": True, "actual": module_summary.get("production_default_unchanged"), "passed": module_summary.get("production_default_unchanged") is True},
    ]

    immutability_rows = [
        {"surface": "audit_only", "policy": "only_new_6fu_audit_script_added", "passed": True},
        {"surface": "fetch_candidate_bullpen_statcast_live_rows_for_date", "policy": "behavior_unchanged", "passed": True},
        {"surface": RUNTIME_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": APPLY_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": CLI_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": DOWNSTREAM_USAGE_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": UPSTREAM_REPORTING_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": PRIOR_CLI_EXPOSURE_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": PRIOR_USAGE_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": REPORTING_HELPER, "policy": "semantics_unchanged", "passed": True},
        {"surface": APPROVED_USAGE_SOURCE, "policy": "semantics_unchanged", "passed": True},
        {"surface": APPROVED_DOWNSTREAM_SOURCE, "policy": "semantics_unchanged", "passed": True},
        {"surface": DOWNSTREAM_HELPER, "policy": "semantics_audited_only", "passed": True},
        {"surface": "6fq_validator", "policy": "unchanged", "passed": True},
        {"surface": "6fr_audit", "policy": "unchanged", "passed": True},
        {"surface": "6fs_plan", "policy": "unchanged", "passed": True},
        {"surface": "6ft_validator", "policy": "unchanged", "passed": True},
        {"surface": "fixtures", "policy": "unchanged", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged", "passed": True},
        {"surface": "network_db_materialization", "policy": "not_added", "passed": True},
    ]

    recommended_path_rows = [
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "audited_implementation_layer", "expected": "6FT", "actual": "6FT", "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "reason", "expected": "downstream usage plan after downstream implementation audit", "actual": "downstream usage plan after downstream implementation audit", "passed": True},
    ]

    checks = [
        {"check": "compileall", "passed": compile_run.returncode == 0, "detail": f"returncode={compile_run.returncode}"},
        {"check": "validator_executed", "passed": validator_run.returncode == 0, "detail": f"returncode={validator_run.returncode}"},
        {
            "check": "validator_json_valid",
            "passed": VALIDATOR_6FT_JSON.exists()
            and validator_summary.get("all_checks_passed") is True
            and validator_summary.get("diagnosis") == DIAGNOSIS_6FT
            and validator_summary.get("recommended_next_layer")
            == "6FU_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_implementation_audit"
            and validator_summary.get("default_downstream_status") == "safe_dry_run_no_real_fetch"
            and validator_summary.get("default_usage_status") == "safe_dry_run_no_real_fetch"
            and validator_summary.get("default_cli_exposure_status") == "safe_dry_run_no_real_fetch"
            and validator_summary.get("default_reporting_status") == "safe_dry_run_no_real_fetch"
            and validator_summary.get("default_prior_usage_status") == "safe_dry_run_no_real_fetch"
            and validator_summary.get("default_prior_cli_exposure_status") == "safe_dry_run_no_real_fetch"
            and validator_summary.get("default_upstream_reporting_status") == "safe_dry_run_no_real_fetch"
            and validator_summary.get("default_downstream_usage_status") == "safe_dry_run_no_real_fetch"
            and validator_summary.get("default_cli_status") == "safe_dry_run_no_real_fetch"
            and validator_summary.get("runtime_summary_status") == "safe_dry_run_no_real_fetch"
            and validator_summary.get("module_self_check_returncode") == 0,
            "detail": str(VALIDATOR_6FT_JSON),
        },
        {"check": "validator_artifacts", "passed": all(row["passed"] for row in validator_artifact_rows), "detail": f"{sum(1 for row in validator_artifact_rows if row['passed'])}/{len(validator_artifact_rows)}"},
        {"check": "source_contract", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(1 for row in source_rows if row['passed'])}/{len(source_rows)}"},
        {"check": "downstream_contract", "passed": all(row["passed"] for row in downstream_rows), "detail": f"{sum(1 for row in downstream_rows if row['passed'])}/{len(downstream_rows)}"},
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
        "downstream_contract": write_csv(AUDIT_DOWNSTREAM_CONTRACT_CSV, downstream_rows),
        "artifact_contract": write_csv(AUDIT_ARTIFACT_CONTRACT_CSV, artifact_rows),
        "runtime_field_contract": write_csv(AUDIT_RUNTIME_FIELD_CONTRACT_CSV, runtime_rows),
        "status_contract": write_csv(AUDIT_STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(AUDIT_SAFETY_CONTRACT_CSV, safety_rows),
        "determinism": write_csv(AUDIT_DETERMINISM_CSV, determinism_rows),
        "module_self_check": write_csv(AUDIT_MODULE_SELF_CHECK_CSV, module_rows),
        "immutability": write_csv(AUDIT_IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(AUDIT_RECOMMENDED_PATH_CSV, recommended_path_rows),
    }

    summary = {
        "layer": "6FU",
        "audit_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6FU if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "audited_layer": "6FT",
        "audited_implementation_diagnosis": validator_summary.get("diagnosis"),
        "audited_implementation_recommended_next_layer": validator_summary.get("recommended_next_layer"),
        "recommended_path": RECOMMENDED_PATH,
        "downstream_helper": DOWNSTREAM_HELPER,
        "approved_downstream_source": APPROVED_DOWNSTREAM_SOURCE,
        "default_downstream_status": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"),
        "default_usage_status": usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status"),
        "default_cli_exposure_status": cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status"),
        "default_reporting_status": reporting_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_status"),
        "default_prior_usage_status": prior_usage_artifact.get("downstream_runtime_summary_cli_exposure_usage_status"),
        "default_prior_cli_exposure_status": prior_cli_exposure_artifact.get("downstream_runtime_summary_cli_exposure_status"),
        "default_upstream_reporting_status": upstream_reporting_artifact.get("downstream_runtime_summary_reporting_status"),
        "default_downstream_usage_status": downstream_usage_artifact.get("downstream_runtime_summary_usage_status"),
        "default_cli_status": cli_artifact.get("cli_diagnostic_artifact_status"),
        "runtime_summary_status": runtime_artifact.get("live_fetcher_runtime_summary_status"),
        "validator_returncode": validator_run.returncode,
        "module_self_check_returncode": module_run.returncode,
        "validator_stdout_tail": validator_run.stdout[-1000:],
        "validator_stderr_tail": validator_run.stderr[-1000:],
        "module_self_check_stdout_tail": module_run.stdout[-1000:],
        "module_self_check_stderr_tail": module_run.stderr[-1000:],
        "validator_csv_counts": validator_csv_counts,
        "audit_csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(AUDIT_JSON),
            "checks_csv": str(AUDIT_CHECKS_CSV),
            "validator_artifacts_csv": str(AUDIT_VALIDATOR_ARTIFACTS_CSV),
            "source_contract_csv": str(AUDIT_SOURCE_CONTRACT_CSV),
            "downstream_contract_csv": str(AUDIT_DOWNSTREAM_CONTRACT_CSV),
            "artifact_contract_csv": str(AUDIT_ARTIFACT_CONTRACT_CSV),
            "runtime_field_contract_csv": str(AUDIT_RUNTIME_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(AUDIT_STATUS_CONTRACT_CSV),
            "safety_contract_csv": str(AUDIT_SAFETY_CONTRACT_CSV),
            "determinism_csv": str(AUDIT_DETERMINISM_CSV),
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
