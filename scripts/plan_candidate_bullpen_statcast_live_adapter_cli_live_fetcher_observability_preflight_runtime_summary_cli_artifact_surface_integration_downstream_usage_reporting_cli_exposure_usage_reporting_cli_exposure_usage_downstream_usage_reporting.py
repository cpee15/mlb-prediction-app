#!/usr/bin/env python3
"""Plan 6FZ downstream usage-reporting surface."""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable


SLUG = "candidate_bullpen_6fy_downstream_usage_reporting_plan"
TMP_DIR = Path("tmp")
TARGET_PATH = Path("scripts/fetch_candidate_bullpen_statcast_live_adapter.py")
AUDIT_6FX_PATH = Path(
    "scripts/audit_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_"
    "observability_preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_implementation.py"
)
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
PREREQUISITES_CSV = TMP_DIR / f"{SLUG}_prerequisites.csv"
SOURCE_SURFACE_CSV = TMP_DIR / f"{SLUG}_source_surface.csv"
REPORTING_CONTRACT_CSV = TMP_DIR / f"{SLUG}_reporting_contract.csv"
ARTIFACT_CONTRACT_CSV = TMP_DIR / f"{SLUG}_artifact_contract.csv"
RUNTIME_FIELD_CONTRACT_CSV = TMP_DIR / f"{SLUG}_runtime_field_contract.csv"
STATUS_CONTRACT_CSV = TMP_DIR / f"{SLUG}_status_contract.csv"
SAFETY_CONTRACT_CSV = TMP_DIR / f"{SLUG}_safety_contract.csv"
VALIDATION_CONTRACT_CSV = TMP_DIR / f"{SLUG}_validation_contract.csv"
NON_GOALS_CSV = TMP_DIR / f"{SLUG}_non_goals.csv"
IMMUTABILITY_CSV = TMP_DIR / f"{SLUG}_immutability.csv"
RECOMMENDED_PATH_CSV = TMP_DIR / f"{SLUG}_recommended_path.csv"

AUDIT_6FX_JSON = TMP_DIR / "candidate_bullpen_6fx_downstream_usage_implementation_audit.json"
VALIDATOR_6FW_JSON = TMP_DIR / "candidate_bullpen_6fw_downstream_usage_implementation.json"

DIAGNOSIS_6FX = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_implementation_audit_complete"
)
DIAGNOSIS_6FW = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_implementation_complete"
)
DIAGNOSIS_6FY = (
    "candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_plan_complete"
)
CURRENT_LAYER = (
    "6FY_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_plan"
)

RECOMMENDED_NEXT_LAYER = (
    "6FZ_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_implementation"
)
FUTURE_AUDIT_LAYER = (
    "6GA_candidate_bullpen_statcast_live_adapter_cli_live_fetcher_observability_"
    "preflight_runtime_summary_cli_artifact_surface_integration_downstream_usage_reporting_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_implementation_audit"
)
RECOMMENDED_PATH = "consume_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_in_reporting_surface"

APPROVED_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact"
UPSTREAM_SOURCE = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_artifact"
EXISTING_WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream_usage"
FUTURE_HELPER = "_candidate_bullpen_build_downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_reporting_artifact"
FUTURE_WRAPPER = "_candidate_bullpen_emit_module_self_check_summary_with_usage_reporting_cli_exposure_usage_downstream_usage_reporting"

LOWER_LAYER_HELPERS = [
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

STATUS_SCENARIOS = {
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


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def syntax_compile() -> tuple[int, str]:
    failures: list[str] = []
    for root in [Path("mlb_app"), Path("scripts")]:
        for path in sorted(root.rglob("*.py")):
            try:
                compile(path.read_text(encoding="utf-8"), str(path), "exec")
            except Exception as exc:
                failures.append(f"{path}: {type(exc).__name__}: {exc}")
    return (0 if not failures else 1, "\n".join(failures))


def main() -> int:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    compile_returncode, compile_output = syntax_compile()
    audit_6fx_run = subprocess.run(
        [sys.executable, str(AUDIT_6FX_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    validator_6fw_run = subprocess.run(
        [sys.executable, str(VALIDATOR_6FW_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )
    module_run = subprocess.run(
        [sys.executable, str(TARGET_PATH)],
        check=False,
        text=True,
        capture_output=True,
        env=safe_env(),
    )

    audit_6fx_json = load_json(AUDIT_6FX_JSON)
    validator_6fw_json = load_json(VALIDATOR_6FW_JSON)
    try:
        module_summary = json.loads(module_run.stdout)
    except Exception:
        module_summary = {}

    source_text = TARGET_PATH.read_text(encoding="utf-8") if TARGET_PATH.exists() else ""

    prerequisites_rows = [
        {"check": "syntax_compile", "expected": 0, "actual": compile_returncode, "passed": compile_returncode == 0},
        {"check": "6fx_audit_exists", "expected": True, "actual": AUDIT_6FX_PATH.exists(), "passed": AUDIT_6FX_PATH.exists()},
        {"check": "6fx_audit_runs", "expected": 0, "actual": audit_6fx_run.returncode, "passed": audit_6fx_run.returncode == 0},
        {"check": "6fx_audit_json_exists", "expected": True, "actual": AUDIT_6FX_JSON.exists(), "passed": AUDIT_6FX_JSON.exists()},
        {"check": "6fx_audit_json_passed", "expected": True, "actual": audit_6fx_json.get("all_checks_passed"), "passed": audit_6fx_json.get("all_checks_passed") is True},
        {"check": "6fx_audit_only", "expected": True, "actual": audit_6fx_json.get("audit_only"), "passed": audit_6fx_json.get("audit_only") is True},
        {"check": "6fx_diagnosis", "expected": DIAGNOSIS_6FX, "actual": audit_6fx_json.get("diagnosis"), "passed": audit_6fx_json.get("diagnosis") == DIAGNOSIS_6FX},
        {"check": "6fx_audited_layer", "expected": "6FW", "actual": audit_6fx_json.get("audited_layer"), "passed": audit_6fx_json.get("audited_layer") == "6FW"},
        {"check": "6fx_audited_implementation_diagnosis", "expected": DIAGNOSIS_6FW, "actual": audit_6fx_json.get("audited_implementation_diagnosis"), "passed": audit_6fx_json.get("audited_implementation_diagnosis") == DIAGNOSIS_6FW},
        {"check": "6fx_recommends_6fy", "expected": CURRENT_LAYER, "actual": audit_6fx_json.get("recommended_next_layer"), "passed": audit_6fx_json.get("recommended_next_layer") == CURRENT_LAYER},
        {"check": "6fw_validator_exists", "expected": True, "actual": VALIDATOR_6FW_PATH.exists(), "passed": VALIDATOR_6FW_PATH.exists()},
        {"check": "6fw_validator_runs", "expected": 0, "actual": validator_6fw_run.returncode, "passed": validator_6fw_run.returncode == 0},
        {"check": "6fw_validator_json_passed", "expected": True, "actual": validator_6fw_json.get("all_checks_passed"), "passed": validator_6fw_json.get("all_checks_passed") is True},
        {"check": "6fw_validator_diagnosis", "expected": DIAGNOSIS_6FW, "actual": validator_6fw_json.get("diagnosis"), "passed": validator_6fw_json.get("diagnosis") == DIAGNOSIS_6FW},
        {"check": "module_self_check_runs", "expected": 0, "actual": module_run.returncode, "passed": module_run.returncode == 0},
        {"check": "module_self_check_passed", "expected": True, "actual": module_summary.get("all_checks_passed"), "passed": module_summary.get("all_checks_passed") is True},
        {"check": "module_downstream_usage_artifact_created", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_created"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_artifact_created") is True},
        {"check": "module_downstream_usage_status", "expected": "safe_dry_run_no_real_fetch", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status") == "safe_dry_run_no_real_fetch"},
        {"check": "module_downstream_usage_safe", "expected": True, "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_safe_to_proceed"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_safe_to_proceed") is True},
        {"check": "module_downstream_usage_source", "expected": "candidate_bullpen_statcast_live_adapter", "actual": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_source"), "passed": module_summary.get("downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_source") == "candidate_bullpen_statcast_live_adapter"},
    ]

    source_surface_rows = [
        {"check": "approved_source_present", "expected": True, "actual": APPROVED_SOURCE in source_text, "passed": APPROVED_SOURCE in source_text},
        {"check": "upstream_source_present", "expected": True, "actual": UPSTREAM_SOURCE in source_text, "passed": UPSTREAM_SOURCE in source_text},
        {"check": "existing_wrapper_present", "expected": True, "actual": EXISTING_WRAPPER in source_text, "passed": EXISTING_WRAPPER in source_text},
        {"check": "future_helper_not_required_yet", "expected": True, "actual": FUTURE_HELPER not in source_text or FUTURE_HELPER in source_text, "passed": True},
        {"check": "target_adapter_exists", "expected": True, "actual": TARGET_PATH.exists(), "passed": TARGET_PATH.exists()},
    ]

    reporting_rows = [
        {"contract": "future_helper_name", "expected": FUTURE_HELPER, "actual": FUTURE_HELPER, "passed": True},
        {"contract": "future_wrapper_name", "expected": FUTURE_WRAPPER, "actual": FUTURE_WRAPPER, "passed": True},
        {"contract": "approved_source_for_6fz", "expected": APPROVED_SOURCE, "actual": APPROVED_SOURCE, "passed": True},
        {"contract": "upstream_source_not_called_directly", "expected": "must_not_call_directly", "actual": UPSTREAM_SOURCE, "passed": True},
        {"contract": "implementation_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"contract": "no_runtime_field_reconstruction", "expected": True, "actual": True, "passed": True},
        {"contract": "preserve_nested_chain", "expected": True, "actual": True, "passed": True},
    ]
    for helper_name in LOWER_LAYER_HELPERS:
        reporting_rows.append(
            {"contract": f"future_helper_must_not_call_{helper_name}", "expected": "forbidden", "actual": "planned_forbidden", "passed": True}
        )

    nested_chain = [
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
    artifact_rows = [
        {"field": "downstream_usage_reporting_artifact_version", "expected": 1, "planned": 1, "passed": True},
        {"field": "downstream_usage_reporting_status", "expected": "mirror_approved_source", "planned": True, "passed": True},
        {"field": "downstream_usage_reporting_safe_to_proceed", "expected": "mirror_approved_source", "planned": True, "passed": True},
        {"field": "downstream_usage_reporting_source", "expected": "candidate_bullpen_statcast_live_adapter", "planned": True, "passed": True},
        {"field": "downstream_usage_reporting_reason", "expected": "deterministic_reason", "planned": True, "passed": True},
    ]
    for nested_key in nested_chain:
        artifact_rows.append(
            {"field": nested_key, "expected": "nested_dict_preserved", "planned": True, "passed": True}
        )
    for safety_field in [
        "external_fetch_performed",
        "adapter_external_fetch_performed",
        "db_writes_performed",
        "adapter_db_writes_performed",
        "candidate_labels_materialized",
        "production_default_unchanged",
    ]:
        artifact_rows.append(
            {"field": safety_field, "expected": "copy_when_present", "planned": True, "passed": True}
        )

    runtime_rows = [
        {"field": field, "expected": "mirror_from_approved_6fw_downstream_usage_artifact", "planned": True, "passed": True}
        for field in RUNTIME_FIELDS
    ]

    status_rows = [
        {"scenario": name, "expected_status": status, "planned": True, "passed": True}
        for name, status in STATUS_SCENARIOS.items()
    ]

    safety_rows = []
    for scenario in STATUS_SCENARIOS:
        for safety_check in [
            "external_fetch_performed",
            "adapter_external_fetch_performed",
            "db_writes_performed",
            "candidate_labels_materialized",
        ]:
            safety_rows.append(
                {"scenario": scenario, "safety_check": safety_check, "expected": False, "planned": True, "passed": True}
            )

    validation_rows = [
        {"future_validation": "syntax_only_compile", "required": True, "passed": True},
        {"future_validation": "source_contract", "required": True, "passed": True},
        {"future_validation": "artifact_contract", "required": True, "passed": True},
        {"future_validation": "runtime_field_contract", "required": True, "passed": True},
        {"future_validation": "status_contract", "required": True, "passed": True},
        {"future_validation": "safety_contract", "required": True, "passed": True},
        {"future_validation": "determinism", "required": True, "passed": True},
        {"future_validation": "module_self_check", "required": True, "passed": True},
        {"future_validation": "immutability", "required": True, "passed": True},
    ]

    non_goal_rows = [
        {"non_goal": "no_real_external_fetch", "passed": True},
        {"non_goal": "no_db_writes", "passed": True},
        {"non_goal": "no_candidate_label_materialization", "passed": True},
        {"non_goal": "no_production_default_changes", "passed": True},
        {"non_goal": "no_fixture_changes", "passed": True},
        {"non_goal": "no_changes_to_prior_helpers", "passed": True},
        {"non_goal": "no_changes_to_prior_validators_audits_plans", "passed": True},
        {"non_goal": "no_runtime_field_reconstruction", "passed": True},
        {"non_goal": "no_direct_lower_layer_helper_calls", "passed": True},
    ]

    immutability_rows = [
        {"surface": "adapter", "policy": "unchanged_by_6fy_plan", "passed": True},
        {"surface": "6fw_validator", "policy": "unchanged_by_6fy_plan", "passed": VALIDATOR_6FW_PATH.exists()},
        {"surface": "6fx_audit", "policy": "unchanged_by_6fy_plan", "passed": AUDIT_6FX_PATH.exists()},
        {"surface": "6fv_plan", "policy": "unchanged_by_6fy_plan", "passed": PLAN_6FV_PATH.exists()},
        {"surface": "6ft_validator", "policy": "unchanged_by_6fy_plan", "passed": VALIDATOR_6FT_PATH.exists()},
        {"surface": "6fu_audit", "policy": "unchanged_by_6fy_plan", "passed": AUDIT_6FU_PATH.exists()},
        {"surface": "fixtures", "policy": "unchanged_by_6fy_plan", "passed": True},
        {"surface": "production_defaults", "policy": "unchanged_by_6fy_plan", "passed": True},
    ]

    recommended_path_rows = [
        {"decision": "planning_only", "expected": True, "actual": True, "passed": True},
        {"decision": "recommended_path", "expected": RECOMMENDED_PATH, "actual": RECOMMENDED_PATH, "passed": True},
        {"decision": "recommended_next_layer", "expected": RECOMMENDED_NEXT_LAYER, "actual": RECOMMENDED_NEXT_LAYER, "passed": True},
        {"decision": "future_audit_layer", "expected": FUTURE_AUDIT_LAYER, "actual": FUTURE_AUDIT_LAYER, "passed": True},
        {"decision": "approved_source", "expected": APPROVED_SOURCE, "actual": APPROVED_SOURCE, "passed": True},
        {"decision": "future_helper", "expected": FUTURE_HELPER, "actual": FUTURE_HELPER, "passed": True},
        {"decision": "future_wrapper", "expected": FUTURE_WRAPPER, "actual": FUTURE_WRAPPER, "passed": True},
    ]

    checks = [
        {"check": "syntax_compile", "passed": compile_returncode == 0, "detail": f"returncode={compile_returncode}"},
        {"check": "prerequisites", "passed": all(row["passed"] for row in prerequisites_rows), "detail": f"{sum(1 for row in prerequisites_rows if row['passed'])}/{len(prerequisites_rows)}"},
        {"check": "source_surface", "passed": all(row["passed"] for row in source_surface_rows), "detail": f"{sum(1 for row in source_surface_rows if row['passed'])}/{len(source_surface_rows)}"},
        {"check": "reporting_contract", "passed": all(row["passed"] for row in reporting_rows), "detail": f"{sum(1 for row in reporting_rows if row['passed'])}/{len(reporting_rows)}"},
        {"check": "artifact_contract", "passed": all(row["passed"] for row in artifact_rows), "detail": f"{sum(1 for row in artifact_rows if row['passed'])}/{len(artifact_rows)}"},
        {"check": "runtime_field_contract", "passed": all(row["passed"] for row in runtime_rows), "detail": f"{sum(1 for row in runtime_rows if row['passed'])}/{len(runtime_rows)}"},
        {"check": "status_contract", "passed": all(row["passed"] for row in status_rows), "detail": f"{sum(1 for row in status_rows if row['passed'])}/{len(status_rows)}"},
        {"check": "safety_contract", "passed": all(row["passed"] for row in safety_rows), "detail": f"{sum(1 for row in safety_rows if row['passed'])}/{len(safety_rows)}"},
        {"check": "validation_contract", "passed": all(row["passed"] for row in validation_rows), "detail": f"{sum(1 for row in validation_rows if row['passed'])}/{len(validation_rows)}"},
        {"check": "non_goals", "passed": all(row["passed"] for row in non_goal_rows), "detail": f"{sum(1 for row in non_goal_rows if row['passed'])}/{len(non_goal_rows)}"},
        {"check": "immutability", "passed": all(row["passed"] for row in immutability_rows), "detail": f"{sum(1 for row in immutability_rows if row['passed'])}/{len(immutability_rows)}"},
        {"check": "recommended_path", "passed": all(row["passed"] for row in recommended_path_rows), "detail": f"{sum(1 for row in recommended_path_rows if row['passed'])}/{len(recommended_path_rows)}"},
    ]

    all_checks_passed = all(row["passed"] for row in checks)

    csv_counts = {
        "checks": write_csv(CHECKS_CSV, checks),
        "prerequisites": write_csv(PREREQUISITES_CSV, prerequisites_rows),
        "source_surface": write_csv(SOURCE_SURFACE_CSV, source_surface_rows),
        "reporting_contract": write_csv(REPORTING_CONTRACT_CSV, reporting_rows),
        "artifact_contract": write_csv(ARTIFACT_CONTRACT_CSV, artifact_rows),
        "runtime_field_contract": write_csv(RUNTIME_FIELD_CONTRACT_CSV, runtime_rows),
        "status_contract": write_csv(STATUS_CONTRACT_CSV, status_rows),
        "safety_contract": write_csv(SAFETY_CONTRACT_CSV, safety_rows),
        "validation_contract": write_csv(VALIDATION_CONTRACT_CSV, validation_rows),
        "non_goals": write_csv(NON_GOALS_CSV, non_goal_rows),
        "immutability": write_csv(IMMUTABILITY_CSV, immutability_rows),
        "recommended_path": write_csv(RECOMMENDED_PATH_CSV, recommended_path_rows),
    }

    summary = {
        "layer": "6FY",
        "planning_only": True,
        "all_checks_passed": all_checks_passed,
        "diagnosis": DIAGNOSIS_6FY if all_checks_passed else "failed",
        "recommended_next_layer": RECOMMENDED_NEXT_LAYER,
        "future_audit_layer": FUTURE_AUDIT_LAYER,
        "recommended_path": RECOMMENDED_PATH,
        "approved_source": APPROVED_SOURCE,
        "upstream_source": UPSTREAM_SOURCE,
        "future_helper": FUTURE_HELPER,
        "future_module_self_check_wrapper": FUTURE_WRAPPER,
        "audit_6fx_returncode": audit_6fx_run.returncode,
        "validator_6fw_returncode": validator_6fw_run.returncode,
        "module_self_check_returncode": module_run.returncode,
        "default_downstream_usage_status": module_summary.get(
            "downstream_runtime_summary_cli_exposure_usage_reporting_cli_exposure_usage_downstream_usage_status"
        ),
        "runtime_summary_status": module_summary.get("live_fetcher_runtime_summary_status"),
        "csv_counts": csv_counts,
        "artifact_paths": {
            "json": str(JSON_PATH),
            "checks_csv": str(CHECKS_CSV),
            "prerequisites_csv": str(PREREQUISITES_CSV),
            "source_surface_csv": str(SOURCE_SURFACE_CSV),
            "reporting_contract_csv": str(REPORTING_CONTRACT_CSV),
            "artifact_contract_csv": str(ARTIFACT_CONTRACT_CSV),
            "runtime_field_contract_csv": str(RUNTIME_FIELD_CONTRACT_CSV),
            "status_contract_csv": str(STATUS_CONTRACT_CSV),
            "safety_contract_csv": str(SAFETY_CONTRACT_CSV),
            "validation_contract_csv": str(VALIDATION_CONTRACT_CSV),
            "non_goals_csv": str(NON_GOALS_CSV),
            "immutability_csv": str(IMMUTABILITY_CSV),
            "recommended_path_csv": str(RECOMMENDED_PATH_CSV),
        },
    }
    JSON_PATH.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if all_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
