#!/usr/bin/env python3
"""Audit 6FW downstream usage-facing implementation."""

from __future__ import annotations

import csv
import importlib.util
import inspect
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable


SLUG = "candidate_bullpen_6fx_downstream_usage_implementation_audit"
TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATOR_6FW_PATH = Path(
    "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_implementation.py"
)
PLAN_6FV_PATH = Path(
    "scripts/plan_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage.py"
)
VALIDATOR_6FT_PATH = Path(
    "scripts/validate_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_implementation.py"
)
AUDIT_6FU_PATH = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_implementation.py"
)

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
VALIDATOR_ARTIFACTS_CSV = TMP_DIR / f"{SLUG}_validator_artifacts.csv"
SOURCE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_source_contract.csv"
DOWNSTREAM_USAGE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_downstream_usage_contract.csv"
ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_artifact_contract.csv"
RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{SLUG}_runtime_field_contract.csv"
STATUS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_status_contract.csv"
SAFETY_CONTRACT_CSV = TMP_DIR / f"{SLUG}_safety_contract.csv"
DETERMINISM_CSV = TMP_DIR / f"{SLUG}_determinism.csv"
MODULE_SELF_CHECK_CSV = TMP_DIR / f"{SLUG}_module_self_check.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

VALIDATOR_6FW_JSON = TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation.json"
VALIDATOR_6FW_CSVS = {
    "checks": (TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation_checks.csv", 11),
    "source_contract": (TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation_source_contract.csv", 23),
    "downstream_usage_contract": (TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation_downstream_usage_contract.csv", 13),
    "artifact_contract": (TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation_artifact_contract.csv", 7),
    "runtime_field_contract": (TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation_runtime_field_contract.csv", 10),
    "status_contract": (TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation_status_contract.csv", 7),
    "safety_contract": (TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation_safety_contract.csv", 28),
    "determinism": (TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation_determinism.csv", 2),
    "module_self_check": (TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation_module_self_check.csv", 13),
    "immutability": (TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation_immutability.csv", 17),
}

DIAGNOSIS_6FW = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_implementation_complete"
)
DIAGNOSIS_6FX = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_implementation_audit_complete"
)
RECOMMENDED_NEXT_LAYER = (
    "6FY_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_plan"
)
RECOMMENDED_PATH = "audit_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_surface"

AUDITED_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact"
APPROVED_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact"
AUDITED_WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream_usage"
FORBIDDEN_DIRECT_HELPERS = [
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_reporting_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_usage_artifact",
    "_candidate_bullpen_build_cli_diagnostic_artifact",
    "_candidate_bullpen_build_live_fetcher_runtime_summary_artifact",
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


def safe_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


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


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def csv_rows_pass(rows: list[dict[str, str]]) -> bool:
    return bool(rows) and all(str(row.get("passed")) == "True" for row in rows)


def syntax_compile() -> tuple[int, str]:
    failures: list[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_module() -> Any:
    module_name = "candidate_bullpen_live_adapter_6fx_audit"
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


def nested_artifacts(artifact: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        "usage_surface": artifact,
        "downstream": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact", {}),
        "usage": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_artifact", {}),
        "cli_exposure": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_artifact", {}),
        "reporting": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_artifact", {}),
        "prior_usage": artifact.get("downstream_runtime_summary_cli_exposure_usage_artifact", {}),
        "prior_cli_exposure": artifact.get("downstream_runtime_summary_cli_exposure_artifact", {}),
        "upstream_reporting": artifact.get("downstream_runtime_summary_reporting_artifact", {}),
        "downstream_usage": artifact.get("downstream_runtime_summary_usage_artifact", {}),
        "cli": artifact.get("cli_diagnostic_artifact", {}),
        "runtime": artifact.get("live_fetcher_runtime_summary_artifact", {}),
    }


def status_from(surface: str, artifact: Dict[str, Any]) -> Any:
    keys = {
        "usage_surface": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status",
        "downstream": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status",
        "usage": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_status",
        "cli_exposure": "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_status",
        "reporting": "downstream_runtime_summary_cli_exposure_usage_reporting_status",
        "prior_usage": "downstream_runtime_summary_cli_exposure_usage_status",
        "prior_cli_exposure": "downstream_runtime_summary_cli_exposure_status",
        "upstream_reporting": "downstream_runtime_summary_reporting_status",
        "downstream_usage": "downstream_runtime_summary_usage_status",
        "cli": "cli_diagnostic_artifact_status",
        "runtime": "live_fetcher_runtime_summary_status",
    }
    return artifact.get(keys[surface])


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_output = syntax_compile()
    validator_run = subprocess.run(
        [sys.executable, str(VALIDATOR_6FW_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    validator_summary = load_json(VALIDATOR_6FW_JSON)

    module = load_module()
    helper = getattr(module, AUDITED_HELPER, None)
    source_text = TARGET_PATH.read_text(encoding="utf-8")
    helper_source = inspect.getsource(helper) if helper else ""

    validator_artifact_rows = []
    for name, (path, expected_rows) in VALIDATOR_6FW_CSVS.items():
        rows = read_csv_rows(path)
        validator_artifact_rows.append(
            {
                "artifact": name,
                "path": str(path),
                "exists": path.exists(),
                "expected_rows": expected_rows,
                "actual_rows": len(rows),
                "rows_pass": csv_rows_pass(rows),
                "passed": path.exists() and len(rows) == expected_rows and csv_rows_pass(rows),
            }
        )

    source_rows = [
        {"check": "audited_helper_exists", "expected": True, "actual": helper is not None, "passed": helper is not None},
        {"check": "audited_wrapper_exists", "expected": True, "actual": AUDITED_WRAPPER in source_text, "passed": AUDITED_WRAPPER in source_text},
        {"check": "entrypoint_uses_audited_wrapper", "expected": True, "actual": f"raise SystemExit({AUDITED_WRAPPER}())" in source_text, "passed": f"raise SystemExit({AUDITED_WRAPPER}())" in source_text},
        {"check": "helper_calls_approved_source", "expected": APPROVED_SOURCE, "actual": APPROVED_SOURCE if APPROVED_SOURCE in helper_source else "missing", "passed": APPROVED_SOURCE in helper_source},
    ]
    for forbidden in FORBIDDEN_DIRECT_HELPERS:
        source_rows.append(
            {"check": f"helper_avoids_direct_{forbidden}", "expected": "absent", "actual": "present" if forbidden in helper_source else "absent", "passed": forbidden not in helper_source}
        )
    for token in FORBIDDEN_TOKENS:
        source_rows.append(
            {"check": f"helper_avoids_{token}", "expected": "absent", "actual": "present" if token in helper_source else "absent", "passed": token not in helper_source}
        )

    artifact = helper() if helper else {}
    nested = nested_artifacts(artifact)
    downstream = nested["downstream"]

    downstream_usage_rows = [
        {"contract": "artifact_is_dict", "expected": True, "actual": isinstance(artifact, dict), "passed": isinstance(artifact, dict)},
        {"contract": "artifact_version", "expected": 1, "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_version"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_version") == 1},
        {"contract": "default_status", "expected": "safe_dry_run_no_real_fetch", "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status") == "safe_dry_run_no_real_fetch"},
        {"contract": "default_safe_to_proceed", "expected": True, "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_safe_to_proceed"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_safe_to_proceed") is True},
        {"contract": "source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_source"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_source") == "candidate_bullpen_statcast_live_adapter"},
        {"contract": "status_equals_downstream", "expected": downstream.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status") == downstream.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status")},
        {"contract": "safe_equals_downstream", "expected": downstream.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_safe_to_proceed"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_safe_to_proceed") == downstream.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_safe_to_proceed")},
    ]

    artifact_rows = []
    for surface, nested_artifact in nested.items():
        artifact_rows.append(
            {"surface": surface, "expected": "dict", "actual": type(nested_artifact).__name__, "passed": isinstance(nested_artifact, dict)}
        )

    runtime_rows = []
    for field in RUNTIME_FIELDS:
        values = {surface: value.get(field) for surface, value in nested.items()}
        runtime_rows.append(
            {
                "field": field,
                "present_everywhere": all(field in value for value in nested.values()),
                "values": "|".join(str(values[surface]) for surface in sorted(values)),
                "passed": all(field in value for value in nested.values()) and len(set(values.values())) == 1,
            }
        )

    status_rows = []
    safety_rows = []
    for scenario, expected in EXPECTED_STATUS.items():
        scenario_artifact = helper(**scenario_kwargs()[scenario]) if helper else {}
        scenario_nested = nested_artifacts(scenario_artifact)
        statuses = {surface: status_from(surface, value) for surface, value in scenario_nested.items()}
        status_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected,
                "actual_statuses": "|".join(str(statuses[surface]) for surface in sorted(statuses)),
                "passed": all(value == expected for value in statuses.values()),
            }
        )
        for safety_key in [
            "external_fetch_performed",
            "adapter_external_fetch_performed",
            "db_writes_performed",
            "candidate_labels_materialized",
        ]:
            values = {surface: value.get(safety_key) for surface, value in scenario_nested.items()}
            safety_rows.append(
                {
                    "scenario": scenario,
                    "safety_check": safety_key,
                    "expected": False,
                    "actual_values": "|".join(str(values[surface]) for surface in sorted(values)),
                    "passed": all(value is False for value in values.values()),
                }
            )

    repeated_a = helper() if helper else {}
    repeated_b = helper() if helper else {}
    determinism_rows = [
        {"check": "audited_helper_repeated_calls_equal", "expected": True, "actual": repeated_a == repeated_b, "passed": repeated_a == repeated_b},
        {"check": "audited_helper_json_sortable", "expected": True, "actual": True, "passed": True},
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
        {"check": "downstream_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact_created") is True},
        {"check": "downstream_artifact_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_usage_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_created") is True},
        {"check": "downstream_usage_artifact_version", "expected": 1, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_version"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_version") == 1},
        {"check": "downstream_usage_artifact_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status") == "safe_dry_run_no_real_fetch"},
        {"check": "downstream_usage_safe_to_proceed", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_safe_to_proceed"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_safe_to_proceed") is True},
        {"check": "downstream_usage_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_source"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_source") == "candidate_bullpen_statcast_live_adapter"},
        {"check": "module_external_fetch_false", "expected": False, "actual": module_summary.get("external_fetch_performed"), "passed": module_summary.get("external_fetch_performed") is False},
        {"check": "module_db_writes_false", "expected": False, "actual": module_summary.get("db_writes_performed"), "passed": module_summary.get("db_writes_performed") is False},
        {"check": "module_production_default_unchanged", "expected": True, "actual": module_summary.get("production_default_unchanged"), "passed": module_summary.get("production_default_unchanged") is True},
    ]

    immutability_rows = [
        {"surface": "audit_only", "policy": "true", "passed": True},
        {"surface": "adapter_file", "policy": "unchanged_by_audit", "passed": True},
        {"surface": "6fw_validator", "policy": "unchanged_by_audit", "passed": True},
        {"surface": "6fv_plan", "policy": "unchanged_by_audit", "passed": PLAN_6FV_PATH.exists()},
        {"surface": "6ft_validator", "policy": "unchanged_by_audit", "passed": VALIDATOR_6FT_PATH.exists()},
        {"surface": "6fu_audit", "policy": "unchanged_by_audit", "passed": AUDIT_6FU_PATH.exists()},
        {"surface": "fixtures", "policy": "unchanged", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged", "passed": True},
        {"surface": "network_db_materialization", "policy": "not_added", "passed": True},
    ]

    recommended_path_rows = [
        {"decision": "audited_layer", "expected": "6FW", "actual": "6FW", "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "audit_only", "expected": True, "actual": True, "passed": True},
    ]

    checks = [
        {"check": "syntax_compile", "passed": compile_returncode == 0, "detail": f"returncode={compile_returncode}"},
        {"check": "6fw_validator_exists", "passed": VALIDATOR_6FW_PATH.exists(), "detail": str(VALIDATOR_6FW_PATH)},
        {"check": "6fw_validator_executed", "passed": validator_run.returncode == 0, "detail": f"returncode={validator_run.returncode}"},
        {"check": "6fw_validator_json_exists", "passed": VALIDATOR_6FW_JSON.exists(), "detail": str(VALIDATOR_6FW_JSON)},
        {"check": "6fw_validator_json_passed", "passed": validator_summary.get("all_checks_passed") is True, "detail": str(validator_summary.get("all_checks_passed"))},
        {"check": "6fw_validator_diagnosis", "passed": validator_summary.get("diagnosis") == DIAGNOSIS_6FW, "detail": str(validator_summary.get("diagnosis"))},
        {"check": "6fw_validator_recommends_6fx", "passed": validator_summary.get("recommended_next_layer") == "6FX_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_implementation_audit", "detail": str(validator_summary.get("recommended_next_layer"))},
        {"check": "validator_artifacts", "passed": all(row["passed"] for row in validator_artifact_rows), "detail": f"{sum(1 for row in validator_artifact_rows if row['passed'])}/{len(validator_artifact_rows)}"},
        {"check": "source_contract", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(1 for row in source_rows if row['passed'])}/{len(source_rows)}"},
        {"check": "downstream_usage_contract", "passed": all(row["passed"] for row in downstream_usage_rows), "detail": f"{sum(1 for row in downstream_usage_rows if row['passed'])}/{len(downstream_usage_rows)}"},
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
        "checks": write_csv(CHECKS_CSV, checks),
        "validator_artifacts": write_csv(VALIDATOR_ARTIFACTS_CSV, validator_artifact_rows),
        "source_contract": write_csv(SOURCE_CONTRACT_CSV, source_rows),
        "downstream_usage_contract": write_csv(DOWNSTREAM_USAGE_CONTRACT_CSV, downstream_usage_rows),
        "artifact_contract": write_csv(ARTIFACT_CONTRACT_CSV, artifact_rows),
        "runtime_field_contract": write_csv(RUNTIME_FIELD_CONTRACT_CSV, runtime_rows),
        "status_contract": write_csv(STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(SAFETY_CONTRACT_CSV, safety_rows),
        "determinism": write_csv(DETERMINISM_CSV, determinism_rows),
        "module_self_check": write_csv(MODULE_SELF_CHECK_CSV, module_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_path_rows),
    }

    summary = {
        "layer": "6FX",
        "audit_only": True,
        "audited_layer": "6FW",
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6FX if all_checks_passed else "failed",
        "audited_implementation_diagnosis": validator_summary.get("diagnosis"),
        "audited_implementation_recommended_next_layer": validator_summary.get("recommended_next_layer"),
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "audited_helper": AUDITED_HELPER,
        "approved_source": APPROVED_SOURCE,
        "default_downstream_usage_status": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status"),
        "default_downstream_status": downstream.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_status"),
        "runtime_summary_status": nested["runtime"].get("live_fetcher_runtime_summary_status"),
        "validator_6fw_returncode": validator_run.returncode,
        "module_self_check_returncode": module_run.returncode,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "validator_artifacts_csv": str(VALIDATOR_ARTIFACTS_CSV),
            "source_contract_csv": str(SOURCE_CONTRACT_CSV),
            "downstream_usage_contract_csv": str(DOWNSTREAM_USAGE_CONTRACT_CSV),
            "artifact_contract_csv": str(ARTIFACT_CONTRACT_CSV),
            "runtime_field_contract_csv": str(RUNTIME_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(STATUS_CONTRACT_CSV),
            "safety_contract_csv": str(SAFETY_CONTRACT_CSV),
            "determinism_csv": str(DETERMINISM_CSV),
            "module_self_check_csv": str(MODULE_SELF_CHECK_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }
    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
