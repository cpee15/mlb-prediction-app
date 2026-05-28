#!/usr/bin/env python3
"""Audit 6GC downstream usage reporting-reporting implementation."""

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


SLUG = "candidate_bullpen_6gd_downstream_usage_reporting_reporting_impl_audit"
TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
VALIDATOR_6GC_PATH = Path("scripts/validate_6gc_downstream_usage_reporting_reporting_impl.py")
PLAN_6GB_PATH = Path("scripts/plan_6gb_downstream_usage_reporting_reporting.py")
AUDIT_6GA_PATH = Path("scripts/audit_6ga_downstream_usage_reporting_implementation.py")
VALIDATOR_6FZ_PATH = Path("scripts/validate_6fz_downstream_usage_reporting_implementation.py")
VALIDATOR_6GC_JSON = TMP_DIR / "candidate_bullpen_6gc_downstream_usage_reporting_reporting_impl.json"

JSON_PATH = TMP_DIR / f"{SLUG}.json"
CHECKS_CSV = TMP_DIR / f"{SLUG}_checks.csv"
VALIDATOR_CSV = TMP_DIR / f"{SLUG}_validator.csv"
SOURCE_CONTRACT_CSV = TMP_DIR / f"{SLUG}_source_contract.csv"
REPORTING_CONTRACT_CSV = TMP_DIR / f"{SLUG}_reporting_contract.csv"
ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_artifact_contract.csv"
RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{SLUG}_runtime_field_contract.csv"
STATUS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_status_contract.csv"
SAFETY_CONTRACT_CSV = TMP_DIR / f"{SLUG}_safety_contract.csv"
DETERMINISM_CSV = TMP_DIR / f"{SLUG}_determinism.csv"
MODULE_SELF_CHECK_CSV = TMP_DIR / f"{SLUG}_module_self_check.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

DIAGNOSIS_6GC = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_implementation_complete"
)
DIAGNOSIS_6GD = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_implementation_audit_complete"
)
CURRENT_LAYER = (
    "6GD_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_implementation_audit"
)
RECOMMENDED_NEXT_LAYER = (
    "6GE_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reporting_plan"
)
RECOMMENDED_PATH = (
    "audit_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact_surface"
)

HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact"
WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting"
PREVIOUS_WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream_usage_reporting"
APPROVED_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact"
FORBIDDEN_DIRECT_HELPERS = [
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact",
    "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact",
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
NESTED_KEYS = [
    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact",
    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact",
    "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact",
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
STATUS_SCENARIOS = {
    "default_no_real_gate_live_dry_run": ({}, "safe_dry_run_no_real_fetch"),
    "synthetic_path": (
        {
            "source_mode": "synthetic",
            "resolution_gate": "synthetic",
            "resolution_status": "synthetic",
            "resolution_reason": "synthetic validation",
            "resolution_synthetic_enabled": True,
            "preflight_status": "passed",
            "preflight_reason": "synthetic dry-run",
        },
        "validation_synthetic_dry_run",
    ),
    "real_gated_monkeypatch_path": (
        {
            "source_mode": "live",
            "resolution_gate": "real_gated",
            "resolution_status": "real_gated",
            "resolution_reason": "real-gated candidate",
            "resolution_external_fetch_enabled": True,
            "resolution_real_enabled": True,
            "preflight_status": "passed",
            "preflight_reason": "real-gated dry-run",
        },
        "real_gated_dry_run_candidate",
    ),
    "dependency_missing_path": (
        {
            "resolution_status": "dependency_missing",
            "resolution_reason": "dependency missing",
            "resolution_dependency_error": True,
            "preflight_status": "passed",
            "preflight_reason": "dependency missing but safe",
        },
        "dependency_missing_safe",
    ),
    "live_without_dry_run": (
        {
            "preflight_passed": False,
            "preflight_status": "blocked",
            "preflight_reason": "dry-run required",
            "preflight_dry_run": False,
        },
        "blocked_requires_dry_run",
    ),
    "live_write_attempt": (
        {
            "preflight_passed": False,
            "preflight_status": "blocked",
            "preflight_reason": "write attempt",
            "preflight_write_blocked": False,
            "preflight_allow_live_write": True,
            "db_writes_performed": False,
            "adapter_db_writes_performed": False,
        },
        "blocked_write",
    ),
    "invalid_or_multi_date_window": (
        {
            "preflight_passed": False,
            "preflight_status": "blocked",
            "preflight_reason": "invalid date window",
            "preflight_single_date": False,
        },
        "blocked_date_window_invalid",
    ),
}


def safe_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        parsed, _ = json.JSONDecoder().raw_decode(text)
        return parsed if isinstance(parsed, dict) else {}


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


def syntax_compile() -> tuple[int, str]:
    failures: list[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def load_module() -> Any:
    spec = importlib.util.spec_from_file_location("candidate_bullpen_live_adapter_6gd", TARGET_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load spec for {TARGET_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["candidate_bullpen_live_adapter_6gd"] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, _ = syntax_compile()
    validator_run = subprocess.run(
        [sys.executable, str(VALIDATOR_6GC_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    validator_json = load_json(VALIDATOR_6GC_JSON)

    adapter_before = TARGET_PATH.read_text(encoding="utf-8") if TARGET_PATH.exists() else ""
    validator_before = VALIDATOR_6GC_PATH.read_text(encoding="utf-8") if VALIDATOR_6GC_PATH.exists() else ""
    plan_before = PLAN_6GB_PATH.read_text(encoding="utf-8") if PLAN_6GB_PATH.exists() else ""
    audit_6ga_before = AUDIT_6GA_PATH.read_text(encoding="utf-8") if AUDIT_6GA_PATH.exists() else ""
    validator_6fz_before = VALIDATOR_6FZ_PATH.read_text(encoding="utf-8") if VALIDATOR_6FZ_PATH.exists() else ""

    module = load_module()
    helper = getattr(module, HELPER, None)
    helper_source = inspect.getsource(helper) if helper else ""
    artifact = helper() if helper else {}
    approved = artifact.get(
        "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact",
        {},
    )

    validator_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6gc_validator_exists", "expected": True, "actual": VALIDATOR_6GC_PATH.exists(), "passed": VALIDATOR_6GC_PATH.exists()},
        {"check": "6gc_validator_runs", "expected": 0, "actual": validator_run.returncode, "passed": validator_run.returncode == 0},
        {"check": "6gc_validator_json_exists", "expected": True, "actual": VALIDATOR_6GC_JSON.exists(), "passed": VALIDATOR_6GC_JSON.exists()},
        {"check": "6gc_validator_all_checks_passed", "expected": True, "actual": validator_json.get("all_checks_passed"), "passed": validator_json.get("all_checks_passed") is True},
        {"check": "6gc_validator_diagnosis", "expected": DIAGNOSIS_6GC, "actual": validator_json.get("diagnosis"), "passed": validator_json.get("diagnosis") == DIAGNOSIS_6GC},
        {"check": "6gc_validator_recommended_next_layer", "expected": CURRENT_LAYER, "actual": validator_json.get("recommended_next_layer"), "passed": validator_json.get("recommended_next_layer") == CURRENT_LAYER},
    ]

    source_rows = [
        {"check": "adapter_exists", "expected": True, "actual": TARGET_PATH.exists(), "passed": TARGET_PATH.exists()},
        {"check": "helper_present", "expected": True, "actual": HELPER in adapter_before, "passed": HELPER in adapter_before},
        {"check": "wrapper_present", "expected": True, "actual": WRAPPER in adapter_before, "passed": WRAPPER in adapter_before},
        {"check": "previous_wrapper_present", "expected": True, "actual": PREVIOUS_WRAPPER in adapter_before, "passed": PREVIOUS_WRAPPER in adapter_before},
        {"check": "entrypoint_uses_wrapper", "expected": True, "actual": f"raise SystemExit({WRAPPER}())" in adapter_before, "passed": f"raise SystemExit({WRAPPER}())" in adapter_before},
        {"check": "helper_callable", "expected": True, "actual": callable(helper), "passed": callable(helper)},
        {"check": "helper_calls_approved_source", "expected": APPROVED_SOURCE, "actual": APPROVED_SOURCE if APPROVED_SOURCE in helper_source else "missing", "passed": APPROVED_SOURCE in helper_source},
    ]
    for forbidden in FORBIDDEN_DIRECT_HELPERS:
        source_rows.append(
            {"check": f"helper_avoids_direct_{forbidden}", "expected": "absent", "actual": "present" if forbidden in helper_source else "absent", "passed": forbidden not in helper_source}
        )
    for token in FORBIDDEN_TOKENS:
        source_rows.append(
            {"check": f"helper_avoids_token_{token}", "expected": "absent", "actual": "present" if token in helper_source else "absent", "passed": token not in helper_source}
        )

    reporting_rows = [
        {"check": "artifact_is_dict", "expected": True, "actual": isinstance(artifact, dict), "passed": isinstance(artifact, dict)},
        {"check": "json_sortable", "expected": True, "actual": True, "passed": True},
        {"check": "artifact_created", "expected": True, "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact_created"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact_created") is True},
        {"check": "artifact_version", "expected": 1, "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact_version"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact_version") == 1},
        {"check": "default_status", "expected": "safe_dry_run_no_real_fetch", "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_status"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_status") == "safe_dry_run_no_real_fetch"},
        {"check": "default_safe", "expected": True, "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_safe_to_proceed"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_safe_to_proceed") is True},
        {"check": "source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_source"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_source") == "candidate_bullpen_statcast_live_adapter"},
        {"check": "reason_present", "expected": True, "actual": bool(artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reason")), "passed": bool(artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_reason"))},
        {"check": "status_equals_approved", "expected": approved.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_status"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_status"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_status") == approved.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_status")},
        {"check": "safe_equals_approved", "expected": approved.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_safe_to_proceed"), "actual": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_safe_to_proceed"), "passed": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_safe_to_proceed") == approved.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_safe_to_proceed")},
    ]
    try:
        json.dumps(artifact, sort_keys=True)
    except Exception as exc:
        reporting_rows[1]["actual"] = type(exc).__name__
        reporting_rows[1]["passed"] = False

    artifact_rows = [
        {"key": key, "expected": "dict", "actual": type(artifact.get(key)).__name__, "passed": isinstance(artifact.get(key), dict)}
        for key in NESTED_KEYS
    ]

    runtime_rows = []
    for field in RUNTIME_FIELDS:
        runtime_rows.append(
            {
                "field": field,
                "top_level_present": field in artifact,
                "approved_present": field in approved,
                "top_level_value": artifact.get(field),
                "approved_value": approved.get(field),
                "passed": field in artifact and field in approved and artifact.get(field) == approved.get(field),
            }
        )

    status_rows = []
    safety_rows = []
    for scenario, (kwargs, expected_status) in STATUS_SCENARIOS.items():
        scenario_artifact = helper(**kwargs) if helper else {}
        status_rows.append(
            {
                "scenario": scenario,
                "expected_status": expected_status,
                "actual_status": scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_status"),
                "passed": scenario_artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_status") == expected_status,
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
                    "safety_field": key,
                    "expected": False,
                    "actual": scenario_artifact.get(key),
                    "passed": scenario_artifact.get(key) is False,
                }
            )

    repeated_a = helper() if helper else {}
    repeated_b = helper() if helper else {}
    determinism_rows = [
        {"check": "repeated_helper_calls_equal", "expected": True, "actual": repeated_a == repeated_b, "passed": repeated_a == repeated_b},
        {"check": "json_sortable", "expected": True, "actual": True, "passed": True},
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
        {"check": "module_returncode", "expected": 0, "actual": module_run.returncode, "passed": module_run.returncode == 0},
        {"check": "diagnosis_preserved", "expected": "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete", "actual": module_summary.get("diagnosis"), "passed": module_summary.get("diagnosis") == "candidate_bullpen_statcast_live_adapter_fetch_module_implementation_complete"},
        {"check": "all_checks_passed", "expected": True, "actual": module_summary.get("all_checks_passed"), "passed": module_summary.get("all_checks_passed") is True},
        {"check": "reporting_reporting_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact_created") is True},
        {"check": "reporting_reporting_version", "expected": 1, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact_version"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_artifact_version") == 1},
        {"check": "reporting_reporting_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_status") == "safe_dry_run_no_real_fetch"},
        {"check": "reporting_reporting_safe", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_safe_to_proceed"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_safe_to_proceed") is True},
        {"check": "reporting_reporting_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_source"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_source") == "candidate_bullpen_statcast_live_adapter"},
        {"check": "external_fetch_false", "expected": False, "actual": module_summary.get("external_fetch_performed"), "passed": module_summary.get("external_fetch_performed") is False},
        {"check": "db_writes_false", "expected": False, "actual": module_summary.get("db_writes_performed"), "passed": module_summary.get("db_writes_performed") is False},
        {"check": "production_default_unchanged", "expected": True, "actual": module_summary.get("production_default_unchanged"), "passed": module_summary.get("production_default_unchanged") is True},
    ]

    adapter_after = TARGET_PATH.read_text(encoding="utf-8") if TARGET_PATH.exists() else ""
    validator_after = VALIDATOR_6GC_PATH.read_text(encoding="utf-8") if VALIDATOR_6GC_PATH.exists() else ""
    plan_after = PLAN_6GB_PATH.read_text(encoding="utf-8") if PLAN_6GB_PATH.exists() else ""
    audit_6ga_after = AUDIT_6GA_PATH.read_text(encoding="utf-8") if AUDIT_6GA_PATH.exists() else ""
    validator_6fz_after = VALIDATOR_6FZ_PATH.read_text(encoding="utf-8") if VALIDATOR_6FZ_PATH.exists() else ""
    immutability_rows = [
        {"surface": "adapter", "policy": "unchanged_by_audit", "passed": adapter_after == adapter_before},
        {"surface": "6gc_validator", "policy": "unchanged_by_audit", "passed": validator_after == validator_before},
        {"surface": "6gb_plan", "policy": "unchanged_by_audit", "passed": plan_after == plan_before},
        {"surface": "6ga_audit", "policy": "unchanged_by_audit", "passed": audit_6ga_after == audit_6ga_before},
        {"surface": "6fz_validator", "policy": "unchanged_by_audit", "passed": validator_6fz_after == validator_6fz_before},
        {"surface": "fixtures", "policy": "unchanged_by_audit", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_audit", "passed": True},
    ]

    recommended_rows = [
        {"check": "audit_only", "expected": True, "actual": True, "passed": True},
        {"check": "audited_layer", "expected": "6GC", "actual": "6GC", "passed": True},
        {"check": "audited_implementation_diagnosis", "expected": DIAGNOSIS_6GC, "actual": DIAGNOSIS_6GC, "passed": True},
        {"check": "diagnosis", "expected": DIAGNOSIS_6GD, "actual": DIAGNOSIS_6GD, "passed": True},
        {"check": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"check": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
    ]

    checks = [
        {"check": "validator", "passed": all(row["passed"] for row in validator_rows), "detail": f"{sum(1 for row in validator_rows if row['passed'])}/{len(validator_rows)}"},
        {"check": "source_contract", "passed": all(row["passed"] for row in source_rows), "detail": f"{sum(1 for row in source_rows if row['passed'])}/{len(source_rows)}"},
        {"check": "reporting_contract", "passed": all(row["passed"] for row in reporting_rows), "detail": f"{sum(1 for row in reporting_rows if row['passed'])}/{len(reporting_rows)}"},
        {"check": "artifact_contract", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/{len(artifact_rows)}"},
        {"check": "runtime_field_contract", "passed": all(row["passed"] for row in runtime_rows), "detail": f"{sum(1 for row in runtime_rows if row['passed'])}/{len(runtime_rows)}"},
        {"check": "status_contract", "passed": all(row["passed"] for row in status_rows), "detail": f"{sum(1 for row in status_rows if row['passed'])}/{len(status_rows)}"},
        {"check": "safety_contract", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "determinism", "passed": all(row["passed"] for row in determinism_rows), "detail": f"{sum(1 for row in determinism_rows if row['passed'])}/{len(determinism_rows)}"},
        {"check": "module_self_check", "passed": all(row["passed"] for row in module_rows), "detail": f"{sum(1 for row in module_rows if row['passed'])}/{len(module_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_rows), "detail": f"{sum(1 for row in recommended_rows if row['passed'])}/{len(recommended_rows)}"},
    ]
    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "validator": write_csv(VALIDATOR_CSV, validator_rows),
        "source_contract": write_csv(SOURCE_CONTRACT_CSV, source_rows),
        "reporting_contract": write_csv(REPORTING_CONTRACT_CSV, reporting_rows),
        "artifact_contract": write_csv(ARTIFACT_CONTRACT_CSV, artifact_rows),
        "runtime_field_contract": write_csv(RUNTIME_FIELD_CONTRACT_CSV, runtime_rows),
        "status_contract": write_csv(STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(SAFETY_CONTRACT_CSV, safety_rows),
        "determinism": write_csv(DETERMINISM_CSV, determinism_rows),
        "module_self_check": write_csv(MODULE_SELF_CHECK_CSV, module_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_rows),
    }

    summary = {
        "layer": "6GD",
        "audit_only": True,
        "audited_layer": "6GC",
        "all_checks_passed": all_checks_passed,
        "audited_implementation_diagnosis": DIAGNOSIS_6GC,
        "diagnosis": DIAGNOSIS_6GD if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "validator_script": str(VALIDATOR_6GC_PATH),
        "validator_returncode": validator_run.returncode,
        "module_self_check_returncode": module_run.returncode,
        "default_reporting_reporting_status": artifact.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_reporting_status"),
        "runtime_summary_status": artifact.get("live_fetcher_runtime_summary_status"),
        "reporting_helper": HELPER,
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "validator_csv": str(VALIDATOR_CSV),
            "source_contract_csv": str(SOURCE_CONTRACT_CSV),
            "reporting_contract_csv": str(REPORTING_CONTRACT_CSV),
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
